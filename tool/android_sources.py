#!/usr/bin/env python3
"""Verify bundled upstream Java bytes and explicitly reviewed local patches."""
from pathlib import Path
import hashlib
from patch_replay import sequence

root = Path(__file__).resolve().parents[1] / 'android'

def manifest(name):
    entries = {}
    for line in (root / name).read_text().splitlines():
        digest, path = line.split('  ', 1)
        if path in entries or len(digest) != 64:
            raise ValueError(f'Invalid manifest entry: {line}')
        entries[path] = digest
    return entries

base = manifest('UPSTREAM-SOURCES.sha256')
patches = manifest('LOCAL-PATCHES.sha256')
actual = {p.relative_to(root).as_posix() for namespace in ('com', 'io')
          for p in (root / 'src/main/java' / namespace).rglob('*.java')}
if actual != set(base):
    raise ValueError('Bundled upstream Java inventory differs from its manifest')
for path in patches:
    if path not in base and not path.startswith('patches/'):
        raise ValueError(f'Patch overrides unknown upstream source: {path}')
for path, digest in (base | patches).items():
    if hashlib.sha256((root / path).read_bytes()).hexdigest() != digest:
        raise ValueError(f'Bundled Android source drift: {path}')
order = ['stale-gatt', 'notification-lifetime', 'cancellation-lifetime',
         'attribute-identity', 'service-identity', 'callback-failure',
         'api33-values', 'notification-overflow-cause',
         'notification-compatibility', 'scan-failure', 'connection-visibility']
if {f'patches/{name}.patch' for name in order} != {
        name for name in patches if name.startswith('patches/')}:
    raise ValueError('Android patch sequence differs from its reviewed inventory')
sequence(root.parent / 'rust/vendor/btleplug/src/droidplug/java',
         [root / f'patches/{name}.patch' for name in order],
         root / 'src/main/java',
         # Restrict to Java: upstream also includes Gradle and wrapper files.
         {name: digest for name, digest in base.items()})
print(f'Android source hashes verified: {len(base)} upstream files, '
      f'{len(patches)} patch entries')
