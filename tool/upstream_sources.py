#!/usr/bin/env python3
"""Verify pinned upstream source inventories and reviewed local patch hashes."""
from pathlib import Path
import hashlib
import json

ROOT = Path(__file__).resolve().parents[1]
VENDOR = ROOT / 'rust/vendor'


def verify(crate, version, metadata_copies):
    manifest = json.loads((VENDOR / f'{crate}-source.json').read_text())
    original = manifest['original_files']
    patched = manifest['patched_files']
    if manifest['crate'] != crate or manifest['version'] != version:
        raise ValueError('Unexpected upstream identity')
    for field in ('registry_checksum', 'patch_sha256'):
        if len(manifest[field]) != 64:
            raise ValueError('Invalid pinned checksum')
    if hashlib.sha256((VENDOR / f'{crate}.patch').read_bytes()).hexdigest() != manifest['patch_sha256']:
        raise ValueError('Reviewed upstream patch drift: ' + crate)
    # Pub omits dotfiles. Preserve published metadata in non-hidden siblings.
    for name, copy in metadata_copies.items():
        if hashlib.sha256((VENDOR / copy).read_bytes()).hexdigest() != original[name]:
            raise ValueError('Published upstream metadata drift: ' + name)
    expected = original | patched
    actual = {f.relative_to(VENDOR / crate).as_posix()
              for f in (VENDOR / crate).rglob('*') if f.is_file()}
    for name in metadata_copies:
        if name not in actual:
            expected.pop(name)
    if actual != set(expected):
        raise ValueError('Vendored source inventory drift: ' + str(actual ^ set(expected)))
    for name, digest in expected.items():
        if hashlib.sha256((VENDOR / crate / name).read_bytes()).hexdigest() != digest:
            raise ValueError('Vendored source drift: ' + name)
    print(f'{crate} {version}: {len(original)} original files, '
          f'{len(patched)} reviewed overrides/additions verified')


verify('btleplug', '0.13.3', {
    '.cargo_vcs_info.json': 'btleplug-vcs-info.json',
    'src/droidplug/java/.gitignore': 'btleplug-java-gitignore.txt',
})
verify('bluez-async', '0.8.2', {
    '.cargo_vcs_info.json': 'bluez-async-vcs-info.json',
})
verify('dbus', '0.9.12', {
    '.cargo_vcs_info.json': 'dbus-vcs-info.json',
})
verify('dbus-tokio', '0.7.6', {
    '.cargo_vcs_info.json': 'dbus-tokio-vcs-info.json',
})
