#!/usr/bin/env python3
"""Inspect Apple consumer architectures, targets, ABI, assets and notices."""
import argparse
import json
from pathlib import Path
import plistlib
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
ARCHITECTURES = {'macos': {'arm64', 'x86_64'}, 'ios': {'arm64'},
                 'simulator': {'arm64', 'x86_64'}}
TARGETS = {'macos': 'MACOS', 'ios': 'IOS', 'simulator': 'IOSSIMULATOR'}
ABI = {'_bletide_abi_version', '_bletide_open', '_bletide_command',
       '_bletide_cancel', '_bletide_close'}


def output(*args):
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout


def version(value):
    parts = value.split('.')
    if not 1 <= len(parts) <= 3 or not all(part.isdigit() for part in parts):
        raise ValueError(f'Invalid deployment version: {value}')
    return tuple(map(int, parts)) + (0,) * (3 - len(parts))


def check_target(binary, architecture, platform, deployment):
    commands = output('xcrun', 'vtool', '-show-build', '-arch', architecture, str(binary))
    target = re.search(r'^\s*platform (\S+)$', commands, re.M)
    if target:
        correct = target.group(1) == TARGETS[platform]
        minimum = re.search(r'^\s*minos (\S+)$', commands, re.M)
    else:
        # Older deployment targets use legacy load commands. ARM64 iOS
        # Simulator starts at iOS14 and must have an explicit build target.
        legacy = 'LC_VERSION_MIN_MACOSX' if platform == 'macos' else 'LC_VERSION_MIN_IPHONEOS'
        correct = f'cmd {legacy}' in commands and (
            platform == 'macos' or (platform == 'ios' and architecture == 'arm64')
            or (platform == 'simulator' and architecture == 'x86_64'))
        minimum = re.search(r'^\s*version (\S+)$', commands, re.M)
    if not correct or minimum is None:
        raise ValueError(f'Wrong or missing Mach-O platform: {binary} ({architecture})')
    if version(minimum.group(1)) > version(deployment):
        raise ValueError(f'Binary requires a newer OS than the app declares: {binary}')


def verify(app, platform):
    contents = app / 'Contents' if platform == 'macos' else app
    native = contents / 'Frameworks/bletide.framework/bletide'
    assets = contents / ('Frameworks/App.framework/Versions/A/Resources/flutter_assets'
                         if platform == 'macos' else 'Frameworks/App.framework/flutter_assets')
    info = plistlib.loads((contents / 'Info.plist').read_bytes())
    deployment = info['LSMinimumSystemVersion' if platform == 'macos' else 'MinimumOSVersion']
    if platform != 'macos' and deployment != '15.0':
        raise ValueError('Example iOS deployment must remain 15.0')
    for key in ('NSBluetoothAlwaysUsageDescription', 'NSBluetoothPeripheralUsageDescription'):
        if not isinstance(info.get(key), str) or not info[key].strip():
            raise ValueError(f'Missing Bluetooth usage description: {key}')
    executable = contents / ('MacOS' if platform == 'macos' else '') / info['CFBundleExecutable']
    upstream = json.loads((ROOT / 'rust/vendor/btleplug-source.json').read_text())['version']
    exports = ABI | {
        f'___{prefix}_btleplug::corebluetooth::central_delegate::CentralDelegate{upstream}'
        for prefix in ('CLASS', 'DROP_FLAG_OFFSET', 'IVAR_OFFSET', 'REGISTER_CLASS')
    }
    architectures = set(output('lipo', '-archs', str(native)).split())
    if architectures != ARCHITECTURES[platform]:
        raise ValueError(f'Unexpected framework architectures: {sorted(architectures)}')
    if set(output('lipo', '-archs', str(executable)).split()) != architectures:
        raise ValueError('App and native framework architectures differ')
    manifest = json.loads((assets / 'NativeAssetsManifest.json').read_text())['native-assets']
    for architecture in sorted(architectures):
        symbols = set(output('nm', '-arch', architecture, '-gjU', str(native)).splitlines())
        if symbols != exports:
            raise ValueError(f'Production exports differ for {architecture}: {sorted(symbols)}')
        for binary in (native, executable):
            check_target(binary, architecture, platform, deployment)
        key = ('macos_' if platform == 'macos' else 'ios_') + (
            'arm64' if architecture == 'arm64' else 'x64')
        if manifest[key]['package:bletide/src/native/bindings.dart'] != ['absolute', 'bletide.framework/bletide']:
            raise ValueError(f'Native asset mapping differs for {key}')
    for filename in ('THIRD_PARTY_LICENSES', 'THIRD_PARTY_NOTICES'):
        if (assets / 'packages/bletide' / filename).read_bytes() != (ROOT / filename).read_bytes():
            raise ValueError(f'Packaged {filename} differs from source')
    print(f'{platform}: architectures, Mach-O targets, exports, native assets, deployment, permissions and notices verified')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--app', type=Path, required=True)
    parser.add_argument('--platform', choices=ARCHITECTURES, required=True)
    args = parser.parse_args()
    verify(args.app, args.platform)


if __name__ == '__main__':
    main()
