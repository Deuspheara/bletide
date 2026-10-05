#!/usr/bin/env python3
"""Verify Windows consumer PE files, native assets and redistribution notices."""
import argparse
import json
from pathlib import Path
import struct

ROOT = Path(__file__).resolve().parents[1]
MACHINES = {'x64': 0x8664, 'arm64': 0xAA64}
ABI = {'bletide_abi_version', 'bletide_open', 'bletide_command',
       'bletide_cancel', 'bletide_close'}


def pe_info(binary, *, exports=False):
    data = binary.read_bytes()

    def unpack(format_, offset):
        size = struct.calcsize(format_)
        if offset < 0 or offset + size > len(data):
            raise ValueError(f'Truncated PE structure: {binary}')
        return struct.unpack_from(format_, data, offset)

    if data[:2] != b'MZ':
        raise ValueError(f'Missing DOS signature: {binary}')
    pe_offset, = unpack('<I', 0x3C)
    if data[pe_offset:pe_offset + 4] != b'PE\0\0':
        raise ValueError(f'Missing PE signature: {binary}')
    machine, section_count, _, _, _, optional_size, characteristics = unpack(
        '<HHIIIHH', pe_offset + 4)
    optional = pe_offset + 24
    magic, = unpack('<H', optional)
    if magic != 0x20B or optional_size < 112:
        raise ValueError(f'Expected PE32+ optional header: {binary}')
    unpack(f'{optional_size}s', optional)
    sections = []
    for index in range(section_count):
        section = optional + optional_size + 40 * index
        _, _, address, raw_size, raw_offset = unpack('<8sIIII', section)
        unpack('16s', section + 24)
        sections.append((address, raw_size, raw_offset))

    def offset_for(rva, size):
        for address, raw_size, raw_offset in sections:
            relative = rva - address
            if 0 <= relative and relative + size <= raw_size:
                offset = raw_offset + relative
                if offset + size <= len(data):
                    return offset
        raise ValueError(f'Unmapped PE RVA: {binary}')

    names = set()
    if exports:
        directory_count, = unpack('<I', optional + 108)
        if directory_count == 0 or optional_size < 120:
            raise ValueError(f'Missing export directory: {binary}')
        export_rva, export_size = unpack('<II', optional + 112)
        if export_rva == 0 or export_size < 40:
            raise ValueError(f'Missing export table: {binary}')
        table = offset_for(export_rva, 40)
        function_count, name_count, functions, pointers, ordinals = unpack(
            '<IIIII', table + 20)
        if function_count != len(ABI) or name_count != len(ABI):
            raise ValueError(f'Unexpected number of production exports: {binary}')
        function_table = offset_for(functions, 4 * function_count)
        name_table = offset_for(pointers, 4 * name_count)
        ordinal_table = offset_for(ordinals, 2 * name_count)
        seen_ordinals = set()
        for index in range(name_count):
            name_rva, = unpack('<I', name_table + index * 4)
            name_offset = offset_for(name_rva, 1)
            end = data.find(b'\0', name_offset, name_offset + 256)
            if end < 0:
                raise ValueError(f'Unterminated export name: {binary}')
            offset_for(name_rva, end - name_offset + 1)
            name = data[name_offset:end].decode('ascii')
            ordinal, = unpack('<H', ordinal_table + index * 2)
            if ordinal >= function_count or ordinal in seen_ordinals or name in names:
                raise ValueError(f'Invalid export identity: {binary}')
            function_rva, = unpack('<I', function_table + ordinal * 4)
            if function_rva == 0 or export_rva <= function_rva < export_rva + export_size:
                raise ValueError(f'Missing or forwarded production export: {binary}')
            offset_for(function_rva, 1)
            names.add(name)
            seen_ordinals.add(ordinal)
    return machine, bool(characteristics & 0x2000), names


def verify(bundle, architecture):
    assets = bundle / 'data/flutter_assets'
    native = bundle / 'bletide.dll'
    executable = bundle / 'bletide_example.exe'
    for required in (executable, native, bundle / 'flutter_windows.dll'):
        if not required.is_file():
            raise ValueError(f'Missing bundle component: {required}')
    for binary in [executable, *sorted(bundle.rglob('*.dll'))]:
        machine, is_dll, _ = pe_info(binary)
        if machine != MACHINES[architecture] or is_dll != (binary != executable):
            raise ValueError(f'Wrong PE architecture or image type: {binary}')
    if pe_info(native, exports=True)[2] != ABI:
        raise ValueError('Production ABI exports differ')
    manifest = json.loads((assets / 'NativeAssetsManifest.json').read_text())
    mapping = manifest['native-assets'][f'windows_{architecture}'][
        'package:bletide/src/native/bindings.dart']
    if mapping != ['absolute', 'bletide.dll']:
        raise ValueError('Native asset manifest does not resolve bletide.dll')
    for filename in ('THIRD_PARTY_LICENSES', 'THIRD_PARTY_NOTICES'):
        if (assets / 'packages/bletide' / filename).read_bytes() != (ROOT / filename).read_bytes():
            raise ValueError(f'Packaged {filename} differs from source')
    print(f'{architecture}: bundle PE, five production ABI exports, native assets and notices verified')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle', type=Path, required=True)
    parser.add_argument('--architecture', choices=MACHINES, default='x64')
    args = parser.parse_args()
    verify(args.bundle, args.architecture)


if __name__ == '__main__':
    main()
