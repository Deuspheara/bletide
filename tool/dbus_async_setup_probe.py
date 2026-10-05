#!/usr/bin/env python3
"""Qualify D-Bus async setup using private Unix sockets, without a real bus.

Default: qualify all stages with the bundled driver and production bootstrap.
Use --upstream-driver to inspect the unmodified driver's readiness failures.
This probe does not establish Bletide initialization cancellation guarantees.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import tarfile
from pathlib import PurePosixPath


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cargo', default='cargo')
    parser.add_argument('--pkg-config-path')
    parser.add_argument('--library-path')
    parser.add_argument('--stage', choices=['authentication', 'hello', 'registration', 'inline', 'session', 'waker', 'all'], default='all')
    parser.add_argument('--report', type=Path)
    parser.add_argument('--auth-watch-candidate', action='store_true', help='Reconstruct the reactor fix in a private checksum-verified copy for comparison.')
    parser.add_argument('--upstream-driver', action='store_true', help='Use the checksum-verified unmodified upstream driver as a negative control.')
    args = parser.parse_args()
    if args.auth_watch_candidate and args.upstream_driver:
        parser.error('Choose only one alternate driver.')
    if sys.platform not in ('darwin', 'linux'):
        parser.error('This qualification probe requires Unix sockets on macOS or Linux.')
    root = Path(__file__).resolve().parent.parent
    fixture = root / 'tool/fixtures/dbus_async_setup.rs'
    lock = (root / 'rust/Cargo.lock').read_text()

    def version(name):
        match = re.search(r'^name = "' + re.escape(name) + r'"\nversion = "([^"]+)"$', lock, re.M)
        if not match:
            raise RuntimeError('Missing pinned dependency: ' + name)
        return match.group(1)

    dbus = json.dumps(str(root / 'rust/vendor/dbus'))
    manifest = '\n'.join([
        '[package]', 'name = "bletide-dbus-async-setup-probe"',
        'version = "0.0.0"', 'edition = "2024"', '[dependencies]',
        'dbus = {path = ' + dbus + ', features = ["futures"]}',
        'dbus-tokio = "=' + version('dbus-tokio') + '"',
        'bluez-async = {path = ' + json.dumps(str(root / 'rust/vendor/bluez-async')) + '}',
        'tokio = {version = "=' + version('tokio') + '", features = ["macros", "rt", "net", "io-util", "time"]}',
        '[patch.crates-io]', 'dbus = {path = ' + dbus + '}', '',
    ])
    if not args.auth_watch_candidate and not args.upstream_driver:
        manifest += 'dbus-tokio = {path = ' + json.dumps(str(root / 'rust/vendor/dbus-tokio')) + '}\n'
    stages = {
        'authentication': 'cancellation_during_authentication_closes_owned_transport',
        'hello': 'cancellation_during_hello_closes_owned_transport',
        'registration': 'completed_hello_installs_owned_unique_name_and_restarts_io',
        'inline': 'production_inline_setup',
        'session': 'production_session_constructor',
        'waker': 'openble_waker_ownership_tests',
    }
    environment = os.environ.copy()
    if args.pkg_config_path:
        environment['PKG_CONFIG_PATH'] = args.pkg_config_path
    if args.library_path:
        key = 'DYLD_LIBRARY_PATH' if sys.platform == 'darwin' else 'LD_LIBRARY_PATH'
        environment[key] = args.library_path
    report = {
        'scope': 'Pinned Rust D-Bus client with isolated Unix peer; no daemon or BLE device',
        'platform': sys.platform,
        'stage': args.stage,
        'driver': 'temporary authentication-watch candidate' if args.auth_watch_candidate else ('pinned unmodified driver' if args.upstream_driver else 'bundled production driver'),
        'versions': {name: version(name) for name in ('tokio', 'dbus-tokio')},
        'source_sha256': {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
                          for path in (fixture, Path(__file__).resolve(), root / 'rust/Cargo.toml', root / 'rust/Cargo.lock', root / 'rust/vendor/dbus/src/channel/ffichannel.rs', root / 'rust/vendor/dbus/src/nonblock.rs', root / 'rust/vendor/dbus-tokio/src/connection.rs', root / 'rust/vendor/dbus-tokio/src/wake.rs', root / 'rust/vendor/bluez-async/src/async_setup.rs', root / 'rust/vendor/bluez-async/src/lib.rs', root / 'rust/vendor/btleplug/src/bluez/manager.rs', root / 'rust/vendor/btleplug/src/bluez/native_error.rs')},
        'limitations': ['Socket open is still synchronous', 'This does not run the full Bletide engine',
                        'Passing host probes does not prove Linux runtime behavior'],
    }
    with tempfile.TemporaryDirectory(prefix='bletide-dbus-setup-') as temporary:
        workspace = Path(temporary)
        (workspace / 'src').mkdir()
        (workspace / 'Cargo.toml').write_text(manifest)
        (workspace / 'src/lib.rs').write_bytes(fixture.read_bytes())
        (workspace / 'src/async_setup.rs').write_bytes((root / 'rust/vendor/bluez-async/src/async_setup.rs').read_bytes())
        (workspace / 'src/dbus_wake.rs').write_bytes((root / 'rust/vendor/dbus-tokio/src/wake.rs').read_bytes())
        if args.auth_watch_candidate or args.upstream_driver:
            package_name = 'dbus-tokio-' + version('dbus-tokio')
            checksum = json.loads((root / 'rust/vendor/dbus-tokio-source.json').read_text())['registry_checksum']
            cargo_home = Path(environment.get('CARGO_HOME', str(Path.home() / '.cargo')))
            archives = list(cargo_home.glob('registry/cache/*/' + package_name + '.crate'))
            archive = next((path for path in archives if hashlib.sha256(path.read_bytes()).hexdigest() == checksum), None)
            if archive is None:
                raise RuntimeError('Candidate qualification requires the locked driver archive in the local Cargo cache.')
            with tarfile.open(archive, 'r:gz') as package:
                for member in package.getmembers():
                    parts = PurePosixPath(member.name).parts
                    if not parts or parts[0] != package_name or '..' in parts or member.name.startswith('/'):
                        raise RuntimeError('Unsafe driver archive member')
                    destination = workspace.joinpath(*parts)
                    if member.isdir():
                        destination.mkdir(parents=True, exist_ok=True)
                    elif member.isfile():
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        destination.write_bytes(package.extractfile(member).read())
                    else:
                        raise RuntimeError('Unsupported driver archive member type')
            driver = workspace / package_name / 'src/connection.rs'
            before = driver.read_text()
            changes = [
                ('                self.write_pending = false;\n                c.read_write',
                 '                self.write_pending = false;\n                let before = c.watch();\n                c.read_write'),
                ('                if c.has_messages_to_send() {',
                 '                let after = c.watch();\n'
                 '                if !before.write && after.write && write_guard.is_ready() {\n'
                 '                    continue;\n'
                 '                }\n'
                 '                if after.write && c.has_messages_to_send() {'),
                ('let mut interest = tokio::io::Interest::READABLE;',
                 'let mut interest = tokio::io::Interest::READABLE | tokio::io::Interest::WRITABLE;'),
            ]
            after = before
            for old, new in changes:
                if not args.auth_watch_candidate:
                    break
                if after.count(old) != 1:
                    raise RuntimeError('Pinned driver source no longer matches candidate change')
                after = after.replace(old, new, 1)
            driver.write_text(after)
            with (workspace / 'Cargo.toml').open('a') as output:
                output.write('dbus-tokio = {path = ' + json.dumps(str(workspace / package_name)) + '}\n')
            report['candidate'] = {
                'locked_archive_sha256': checksum,
                'original_driver_sha256': hashlib.sha256(before.encode()).hexdigest(),
                'candidate_driver_sha256': hashlib.sha256(after.encode()).hexdigest(),
                'scope': 'Temporary workspace only; reconstructed readiness fix' if args.auth_watch_candidate else 'Temporary unmodified upstream negative control',
            }

        environment['CARGO_TARGET_DIR'] = str(workspace / 'target')
        environment['TMPDIR'] = str(workspace)
        # Pass a private address at process launch; Rust tests never mutate the
        # process-wide environment or contact the user's system bus.
        environment['OPENBLE_DBUS_TEST_ENDPOINT'] = str(workspace / 's')
        environment['DBUS_SYSTEM_BUS_ADDRESS'] = 'unix:path=' + str(workspace / 's')
        command = [args.cargo, 'test', '--offline', '--manifest-path', str(workspace / 'Cargo.toml'), '--lib']
        if args.stage != 'all':
            command.append(stages[args.stage])
        # Process-wide fd baselines require independent tests to run serially.
        # Each fixture still drives peer/client and cancellation concurrently.
        command.extend(['--', '--test-threads=1', '--nocapture'])
        result = subprocess.run(command, env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        report.update(exit_code=result.returncode, output=result.stdout,
                      generated_lock_sha256=hashlib.sha256((workspace / 'Cargo.lock').read_bytes()).hexdigest()
                      if (workspace / 'Cargo.lock').exists() else None)
    report['temporary_workspace_removed'] = not workspace.exists()
    encoded = json.dumps(report, indent=2) + '\n'
    if args.report:
        args.report.write_text(encoded)
    print(result.stdout, end='')
    print('Temporary probe workspace removed:', report['temporary_workspace_removed'])
    return result.returncode


if __name__ == '__main__':
    raise SystemExit(main())
