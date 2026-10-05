#!/usr/bin/env python3
"""Compare the production port object and public callback header with the Dart C ABI.

Compile constants to LLVM IR; no program, VM, or BLE backend is executed.
Cross-target checks require the matching Rust target and C SDK/sysroot.
"""
import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile


def constants(path):
    text = path.read_text()
    # Rust may encode an integer array as a byte-string aggregate in LLVM IR.
    line = re.search(r"^@OPENBLE_EVENT_LAYOUT =(.+)$", text, re.M)
    encoded = re.search(r'c"([^"\n]+)"', line[1]) if line else None
    if encoded:
        data = bytearray()
        value = encoded[1]
        while value:
            if value[0] == "\\":
                data.append(int(value[1:3], 16))
                value = value[3:]
            else:
                data.append(ord(value[0]))
                value = value[1:]
        if len(data) != 96:
            raise ValueError(f"Incomplete layout bytes in {path}")
        endian = re.search(r'target datalayout = "([eE])', text)[1]
        return [int.from_bytes(data[i:i + 8], "little" if endian == "e" else "big")
                for i in range(0, 96, 8)]
    match = re.search(r"@OPENBLE_EVENT_LAYOUT =[^\n]*\[12 x i64\] \[([^\n]+)\]", text)
    if not match:
        raise ValueError(f"Missing layout constants in {path}")
    values = [int(value) for value in re.findall(r"i64 (\d+)", match[1])]
    if len(values) != 12:
        raise ValueError(f"Incomplete layout constants in {path}")
    return values


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dart-sdk", type=Path)
    parser.add_argument("--rustc", default="rustc")
    parser.add_argument("--clang", default="clang")
    parser.add_argument("--target")
    parser.add_argument("--clang-target", help="C target triple when Clang and Rust spell it differently")
    parser.add_argument("--clang-arg", action="append", default=[])
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    sdk = args.dart_sdk
    if sdk is None:
        dart = shutil.which("dart")
        if not dart:
            parser.error("Provide --dart-sdk or put dart on PATH")
        sdk = Path(dart).resolve().parent.parent
        if not (sdk / "include/dart_native_api.h").is_file():
            sdk = sdk / "bin/cache/dart-sdk"  # Flutter's dart launcher.
    header = sdk / "include/dart_native_api.h"
    if not header.is_file():
        parser.error(f"Missing Dart header: {header}")
    target = args.target
    if target is None:
        version = subprocess.check_output([args.rustc, "-vV"], cwd=root / "rust", text=True)
        target = re.search(r"^host: (.+)$", version, re.M)[1]
    source = (root / "rust/src/event.rs").read_text()
    # Use the actual production types and tag initializers, without copying them.
    types = source[:source.index("pub(crate) type Post")]
    tags = re.findall(r"kind: (\d+),", source[:source.index("#[cfg(test)]")])
    if len(tags) != 2:
        raise ValueError("Expected exactly two production event tag initializers")
    rust = types + f'''
#[unsafe(no_mangle)]
pub static OPENBLE_EVENT_LAYOUT: [u64; 12] = [
    size_of::<CObject>() as u64, align_of::<CObject>() as u64,
    std::mem::offset_of!(CObject, value) as u64,
    size_of::<Value>() as u64, align_of::<Value>() as u64,
    size_of::<TypedData>() as u64, align_of::<TypedData>() as u64,
    std::mem::offset_of!(TypedData, kind) as u64,
    std::mem::offset_of!(TypedData, length) as u64,
    std::mem::offset_of!(TypedData, data) as u64,
    {tags[0]}, {tags[1]},
];
'''
    c = '''#include <stddef.h>
#include "dart_api_dl.h"
#include "bletide.h"
// Compare the public callback directly with the installed VM's actual type.
_Static_assert(__builtin_types_compatible_p(bletide_post_c_object, Dart_PostCObject_Type),
               "Bletide callback signature differs from Dart_PostCObject");
_Static_assert(__builtin_types_compatible_p(__typeof__(&bletide_open),
               int64_t (*)(Dart_Port_DL, Dart_PostCObject_Type)),
               "Bletide open signature differs from its VM callback contract");
int64_t OPENBLE_HEADER_PROBE(Dart_Port_DL port) {
    return bletide_open(port, Dart_PostCObject_DL);
}
#define T __typeof__(((Dart_CObject*)0)->value.as_typed_data)
#define V __typeof__(((Dart_CObject*)0)->value)
const uint64_t OPENBLE_EVENT_LAYOUT[12] = {
    sizeof(Dart_CObject), _Alignof(Dart_CObject), offsetof(Dart_CObject, value),
    sizeof(V), _Alignof(V), sizeof(T), _Alignof(T),
    offsetof(T, type), offsetof(T, length), offsetof(T, values),
    Dart_CObject_kTypedData, Dart_TypedData_kUint8
};
'''
    with tempfile.TemporaryDirectory(prefix="bletide-event-layout-") as directory:
        work = Path(directory)
        (work / "layout.rs").write_text(rust)
        (work / "layout.c").write_text(c)
        subprocess.run([args.rustc, "--edition=2024", "--crate-type=lib", "--emit=llvm-ir",
                        "--target", target, "-A", "warnings", str(work / "layout.rs"),
                        "-o", str(work / "rust.ll")], cwd=root / "rust", check=True)
        subprocess.run([args.clang, "-target", args.clang_target or target, "-std=c17",
                        "-pedantic-errors", "-Wall", "-Wextra", "-Werror", "-S", "-emit-llvm",
                        "-isystem", str(header.parent), "-I", str(root / "src"),
                        *args.clang_arg, str(work / "layout.c"), "-o", str(work / "c.ll")], check=True)
        actual, expected = constants(work / "rust.ll"), constants(work / "c.ll")
        if actual != expected:
            raise ValueError(f"{target}: Rust event layout {actual} != Dart header {expected}")
        print(json.dumps({"target": target, "dart_header": str(header), "layout": actual, "callback_header_checked": True}))


if __name__ == "__main__":
    main()
