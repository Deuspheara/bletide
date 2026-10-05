#!/usr/bin/env python3
"""Run upstream JNI tests against this package's actual bundled Java classes."""
import argparse
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--target-dir', type=Path,
                    default=root / 'rust/target/jni-host-tests')
args = parser.parse_args()
target = args.target_dir.resolve()
jar = target / 'debug/java/libs/btleplug-jni.jar'
jar.parent.mkdir(parents=True, exist_ok=True)
sources = sorted((root / 'android/src/main/java/io').rglob('*.java'))
sources += sorted((root / 'tool/fixtures/jni').rglob('*.java'))
with tempfile.TemporaryDirectory(prefix='bletide-jni-classes-') as directory:
    subprocess.run(['javac', '--release', '17', '-d', directory,
                    *map(str, sources)], check=True)
    subprocess.run(['jar', 'cf', str(jar), '-C', directory, '.'], check=True)
subprocess.run(['cargo', 'test', '--locked', '--manifest-path',
                str(root / 'rust/vendor/btleplug/Cargo.toml'), '--target-dir',
                str(target), '--features', 'jni-host-tests', '--lib'],
               cwd=root / 'rust', check=True)
