"""Offline patch provenance: restore original bytes and reproduce reviewed bytes."""
import hashlib
from pathlib import Path
import shutil
import subprocess
import tempfile


def inventory(directory):
    return {path.relative_to(directory).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in directory.rglob('*') if path.is_file()}


def apply(directory, patch, *, reverse=False):
    subprocess.run(['git', '-c', 'core.autocrlf=false', 'apply', '--whitespace=nowarn',
                    *(['--reverse'] if reverse else []), str(patch.resolve())],
                   cwd=directory, check=True, capture_output=True)


def round_trip(source, patch, original):
    reviewed = inventory(source)
    with tempfile.TemporaryDirectory(prefix='bletide-patch-') as temporary:
        directory = Path(temporary) / 'source'
        shutil.copytree(source, directory)
        apply(directory, patch, reverse=True)
        # Historical patches describe additions as empty-file hunks rather than
        # /dev/null additions. Reverse application leaves empty files behind.
        for name in set(reviewed) - set(original):
            path = directory / name
            if path.exists():
                if path.read_bytes():
                    raise ValueError(f'{patch.name}: addition was not fully reversed: {name}')
                path.unlink()
        if inventory(directory) != original:
            raise ValueError(f'{patch.name}: reverse patch does not restore the original inventory')
        apply(directory, patch)
        if inventory(directory) != reviewed:
            raise ValueError(f'{patch.name}: forward patch does not reproduce the reviewed inventory')


def sequence(original_source, patches, reviewed_source, expected_original):
    originals = inventory(original_source / 'src/main/java')
    if {f'src/main/java/{name}': digest for name, digest in originals.items()} != expected_original:
        raise ValueError('Android original source differs from pinned upstream inventory')
    with tempfile.TemporaryDirectory(prefix='bletide-java-patches-') as temporary:
        directory = Path(temporary) / 'source'
        shutil.copytree(original_source / 'src/main/java', directory / 'src/main/java')
        for patch in patches:
            # Java patches include the src/main/java prefix.
            apply(directory, patch)
        actual = inventory(directory / 'src/main/java')
        expected = inventory(reviewed_source)
        # The bootstrap namespace is first-party, outside the upstream patch set.
        expected = {name: digest for name, digest in expected.items()
                    if name.startswith(('com/', 'io/'))}
        if actual != expected:
            raise ValueError('Android patches do not reproduce the bundled Java inventory')
