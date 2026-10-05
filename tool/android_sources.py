#!/usr/bin/env python3
"""Verify bundled upstream Java bytes and explicitly reviewed local patches."""
from pathlib import Path
import hashlib

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
actual = {str(p.relative_to(root)) for namespace in ('com', 'io')
          for p in (root / 'src/main/java' / namespace).rglob('*.java')}
if actual != set(base):
    raise ValueError('Bundled upstream Java inventory differs from its manifest')
for path in patches:
    if path not in base and not path.startswith('patches/'):
        raise ValueError(f'Patch overrides unknown upstream source: {path}')
for path, digest in (base | patches).items():
    if hashlib.sha256((root / path).read_bytes()).hexdigest() != digest:
        raise ValueError(f'Bundled Android source drift: {path}')
print(f'Android source hashes verified: {len(base)} upstream files, '
      f'{len(patches)} patch entries')
