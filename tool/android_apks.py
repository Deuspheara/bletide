#!/usr/bin/env python3
"""Verify consumer split APK native assets, architectures, ABI and notices."""
import argparse
import json
from pathlib import Path
import re
import struct
import subprocess
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parents[1]
ARCHITECTURES = {
    'armeabi-v7a': (1, 40, 'android_arm'),
    'arm64-v8a': (2, 183, 'android_arm64'),
    'x86_64': (2, 62, 'android_x64'),
}
ABI = {'bletide_abi_version', 'bletide_open', 'bletide_command',
       'bletide_cancel', 'bletide_close'}
JNI = {'Java_dev_bletide_BletidePlugin_nativeInitialize',
       'Java_dev_bletide_BletidePlugin_nativeAdapterState'}


def run(toolchain, tool, *arguments):
    return subprocess.run([str(toolchain / tool), *map(str, arguments)],
                          check=True, capture_output=True, text=True).stdout


def verify(directory, toolchain):
    for architecture, (elf_class, machine, asset_key) in ARCHITECTURES.items():
        apk = directory / f'app-{architecture}-release.apk'
        with zipfile.ZipFile(apk) as archive:
            libraries = [name for name in archive.namelist()
                         if name.startswith('lib/') and name.endswith('.so')]
            if not libraries or any(not name.startswith(f'lib/{architecture}/') for name in libraries):
                raise ValueError(f'{apk.name}: split contains a different architecture')
            if elf_class == 2:
                for name in libraries:
                    entry = archive.getinfo(name)
                    archive.fp.seek(entry.header_offset)
                    header = archive.fp.read(30)
                    namesize, extrasize = struct.unpack_from('<HH', header, 26)
                    offset = entry.header_offset + 30 + namesize + extrasize
                    if entry.compress_type != zipfile.ZIP_STORED or offset % 16384:
                        raise ValueError(f'{apk.name}: {name} is not uncompressed and 16 KiB ZIP-aligned')
            data = archive.read(f'lib/{architecture}/libbletide.so')
            if (len(data) < 52 or data[:4] != b'\x7fELF' or data[4] != elf_class
                    or data[5] != 1 or struct.unpack_from('<H', data, 18)[0] != machine):
                raise ValueError(f'{apk.name}: wrong native ELF architecture')
            manifest = json.loads(archive.read('assets/flutter_assets/NativeAssetsManifest.json'))
            asset = manifest['native-assets'][asset_key]['package:bletide/src/native/bindings.dart']
            if asset != ['absolute', 'libbletide.so']:
                raise ValueError(f'{apk.name}: native asset manifest does not resolve libbletide.so')
            for filename in ('THIRD_PARTY_LICENSES', 'THIRD_PARTY_NOTICES'):
                if archive.read(f'assets/flutter_assets/packages/bletide/{filename}') != (ROOT / filename).read_bytes():
                    raise ValueError(f'{apk.name}: packaged {filename} differs from source')
            for name in archive.namelist():
                if name.endswith('.dex'):
                    dex = archive.read(name)
                    if b'Lorg/mockito/' in dex or b'Lorg/junit/' in dex:
                        raise ValueError(f'{apk.name}: test dependencies packaged in DEX')
        with tempfile.TemporaryDirectory(prefix='bletide-apk-') as temporary:
            library = Path(temporary) / 'libbletide.so'
            library.write_bytes(data)
            symbols = {line.split()[-1] for line in run(toolchain, 'llvm-nm', '-D', '--defined-only', library).splitlines() if line.split()}
            if {symbol for symbol in symbols if symbol.startswith('bletide_')} != ABI or not JNI <= symbols:
                raise ValueError(f'{apk.name}: consumer ABI/JNI exports differ or internal exports leaked')
            if elf_class == 2:
                headers = [line.split() for line in run(toolchain, 'llvm-readelf', '--program-headers', library).splitlines()
                           if line.strip().startswith('LOAD ')]
                if not headers or any(int(header[-1], 16) < 16384 for header in headers):
                    raise ValueError(f'{apk.name}: native LOAD segments lack 16 KiB alignment')
            dump = run(toolchain, 'llvm-readelf', '--hex-dump=.note.android.ident', library)
            note = b''.join(bytes.fromhex(match.group(1)) for match in re.finditer(
                r'^0x[0-9a-fA-F]+\s+((?:[0-9a-fA-F]{8}\s+){1,4})', dump, re.MULTILINE))
            if len(note) < 12:
                raise ValueError(f'{apk.name}: missing Android NDK identification note')
            namesize, descsize, kind = struct.unpack_from('<III', note)
            start = 12 + ((namesize + 3) & ~3)
            if (kind != 1 or note[12:12 + namesize] != b'Android\0' or descsize < 4
                    or len(note) < start + descsize or struct.unpack_from('<I', note, start)[0] != 24):
                raise ValueError(f'{apk.name}: native asset is not linked against Android API 24')
        print(f'{apk.name}: ELF architecture, API 24, five ABI exports, JNI, assets and notices verified')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, default=ROOT / 'example/build/app/outputs/flutter-apk')
    parser.add_argument('--toolchain', type=Path, required=True, help='NDK LLVM bin directory')
    args = parser.parse_args()
    verify(args.directory, args.toolchain)


if __name__ == '__main__':
    main()
