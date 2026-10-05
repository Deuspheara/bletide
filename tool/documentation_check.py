#!/usr/bin/env python3
"""Type-check every Dart README example against the resolved package API."""
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    snippets = re.findall(r'```dart\n(.*?)```', (ROOT / 'README.md').read_text(), re.S)
    if not snippets:
        raise ValueError('README has no Dart examples to verify')
    if not (ROOT / '.dart_tool/package_config.json').is_file():
        raise ValueError('Resolve package dependencies before checking documentation')
    with tempfile.TemporaryDirectory(prefix='documentation-', dir=ROOT / '.dart_tool') as temporary:
        directory = Path(temporary)
        for index, snippet in enumerate(snippets):
            (directory / f'example_{index}.dart').write_text(snippet)
        subprocess.run(['dart', 'analyze', '--fatal-infos', str(directory)], cwd=ROOT, check=True)
    print(f'{len(snippets)} Dart README examples type-checked; no hardware calls executed')


if __name__ == '__main__':
    main()
