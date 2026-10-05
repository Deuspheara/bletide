#!/usr/bin/env python3
"""Check the public file set for common leaks and nonportable documentation."""
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]

# Report only paths/rule names, never matched credential values.
RULES = {
    'private key': re.compile(r'-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----'),
    'GitHub token': re.compile(r'\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{50,})\b'),
    'AWS access key': re.compile(r'\b(?:AKIA|ASIA)[A-Z0-9]{16}\b'),
    'Slack token': re.compile(r'\bxox[baprs]-[A-Za-z0-9-]{20,}\b'),
    'Google API key': re.compile(r'\bAIza[0-9A-Za-z_-]{35}\b'),
    'credential-bearing URL': re.compile(r'https?://[^\s/:@]+:[^\s/@]+@'),
    'personal Apple signing team': re.compile(r'DEVELOPMENT_TEAM\s*=\s*[A-Z0-9]{10}\s*;'),
    'private workstation path': re.compile(r'(?:/Users/|/home/)[A-Za-z0-9._-]+/|[A-Z]:\\Users\\[A-Za-z0-9._-]+\\'),
}
PRIVATE_PARTS = {'.local', '.dart_tool', '.gradle', '.kotlin', '.idea', '.aws', '.ssh',
                 'build', 'coverage', '__pycache__'}
PRIVATE_NAMES = {'.env', 'local.properties', 'key.properties', '.netrc',
                 'credentials.json', 'hardware.local.json'}
PRIVATE_SUFFIXES = {'.pem', '.key', '.jks', '.keystore', '.p12', '.pfx',
                    '.mobileprovision', '.pcap', '.pcapng', '.pyc'}
REQUIRED = {
    'README.md', 'LICENSE', 'CONTRIBUTING.md', 'SECURITY.md',
    'pubspec.yaml', 'lib/bletide.dart', 'src/bletide.h',
    'rust/Cargo.toml', 'rust/Cargo.lock', 'rust/rust-toolchain.toml',
    'THIRD_PARTY_LICENSES', 'THIRD_PARTY_NOTICES', '.github/workflows/ci.yml',
    'example/android/gradlew', 'example/android/gradlew.bat',
    'example/android/gradle/wrapper/gradle-wrapper.jar',
    'integration_test/hardware.example.json',
}


def public_files():
    result = subprocess.run(
        ['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z'],
        cwd=ROOT, check=True, capture_output=True,
    )
    return {entry.decode('utf-8') for entry in result.stdout.split(b'\0') if entry}


def check():
    paths = public_files()
    errors = []
    for required in sorted(REQUIRED - paths):
        errors.append(f'{required}: required public file is missing or ignored')
    for name in sorted(paths):
        path = ROOT / name
        if not path.is_file():
            errors.append(f'{name}: missing file')
            continue
        relative = Path(name)
        # Upstream source inventories may contain intentionally named fixtures.
        vendored = name.startswith('rust/vendor/')
        if not vendored and (set(relative.parts) & PRIVATE_PARTS
                             or relative.name in PRIVATE_NAMES
                             or relative.suffix in PRIVATE_SUFFIXES
                             or relative.name.startswith('.env.')
                             and not relative.name.endswith('.example')):
            errors.append(f'{name}: private/generated file is eligible for publication')
        try:
            content = path.read_text(encoding='utf-8')
        except UnicodeError:
            continue
        for rule, pattern in RULES.items():
            if pattern.search(content):
                errors.append(f'{name}: {rule} found; inspect locally')
        # Check first-party Markdown links; preserve original upstream docs.
        if relative.suffix == '.md' and not vendored and not name.startswith('android/patches/'):
            for target in re.findall(r'\]\(([^)\s]+)(?:\s+"[^"]*")?\)', content):
                target = unquote(target.strip('<>')).split('#', 1)[0]
                if not target or re.match(r'[A-Za-z][A-Za-z0-9+.-]*:', target):
                    continue
                resolved = (path.parent / target).resolve()
                try:
                    public_target = str(resolved.relative_to(ROOT))
                except ValueError:
                    errors.append(f'{name}: link leaves the repository')
                    continue
                if public_target not in paths and not resolved.is_dir():
                    errors.append(f'{name}: broken/nonpublic link to {public_target}')
    if errors:
        print('\n'.join(errors), file=sys.stderr)
        return 1
    print(f'Repository hygiene passed: {len(paths)} public files checked')
    return 0


if __name__ == '__main__':
    sys.exit(check())
