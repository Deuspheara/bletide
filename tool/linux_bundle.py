#!/usr/bin/env python3
"""Verify a Linux consumer bundle's native assets, architecture and licenses."""
import argparse
import json
from pathlib import Path
import struct
import subprocess

ROOT = Path(__file__).resolve().parents[1]
MACHINES = {'x64': 62, 'arm64': 183}
ABI = {'bletide_abi_version', 'bletide_open', 'bletide_command',
       'bletide_cancel', 'bletide_close'}


def verify(bundle, architecture, nm):
    assets = bundle / 'data/flutter_assets'
    native = bundle / 'lib/libbletide.so'
    for required in (bundle / 'bletide_example', native,
                     bundle / 'lib/libflutter_linux_gtk.so'):
        if not required.is_file():
            raise ValueError(f'Missing bundle component: {required}')
    binaries = [bundle / 'bletide_example', *sorted((bundle / 'lib').rglob('*.so'))]
    for binary in binaries:
        with binary.open('rb') as stream:
            header = stream.read(64)
        if (len(header) < 64 or header[:4] != b'\x7fELF'
                or header[4:6] != bytes((2, 1))
                or struct.unpack_from('<H', header, 18)[0] != MACHINES[architecture]):
            raise ValueError(f'Wrong ELF architecture: {binary}')
    manifest = json.loads((assets / 'NativeAssetsManifest.json').read_text())
    mapping = manifest['native-assets'][f'linux_{architecture}'][
        'package:bletide/src/native/bindings.dart']
    if mapping != ['absolute', 'libbletide.so']:
        raise ValueError('Native asset manifest does not resolve libbletide.so')
    for filename in ('THIRD_PARTY_LICENSES', 'THIRD_PARTY_NOTICES'):
        if (assets / 'packages/bletide' / filename).read_bytes() != (ROOT / filename).read_bytes():
            raise ValueError(f'Packaged {filename} differs from source')
    output = subprocess.run([str(nm), '-D', '--defined-only', str(native)],
                            check=True, capture_output=True, text=True).stdout
    symbols = {line.split()[-1] for line in output.splitlines() if line.strip()}
    if symbols != ABI:
        raise ValueError(f'Production ABI exports differ: {sorted(symbols)}')
    print(f'{architecture}: bundle ELF, five production ABI exports, native assets and licenses verified')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle', type=Path, required=True)
    parser.add_argument('--architecture', choices=MACHINES, default='x64')
    parser.add_argument('--nm', default='nm', help='GNU or LLVM nm executable')
    args = parser.parse_args()
    verify(args.bundle, args.architecture, args.nm)


if __name__ == '__main__':
    main()
