"""User-service generation and installation are tested with simulated managers only."""
import fcntl
import json
import os
from pathlib import Path
import plistlib
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'program'))
sys.path.insert(0, str(ROOT / 'installer'))
import supervisor
import install
import package


class SupervisorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='tango-supervisor-SIMULATION-')
        self.base = Path(self.temporary.name).resolve()
        self.home = self.base / 'home with spaces'
        (self.home / 'program').mkdir(parents=True)
        (self.home / 'program' / 'autonomous.py').write_text('# Synthetic installed runner\n')
        self.codex = self.base / 'SIMULATED codex'
        self.codex.write_text('#!/bin/sh\nexit 0\n')
        self.codex.chmod(0o755)

    def tearDown(self):
        self.temporary.cleanup()

    def spec(self, system='Darwin'):
        return supervisor.service_spec(self.home, sys.executable, str(self.codex), system)

    def manager_result(self, returncode=0, stdout=''):
        return subprocess.CompletedProcess(['SIMULATED-manager'], returncode, stdout, '')

    def test_render_only_writes_requested_file_and_never_activates(self):
        output = self.base / 'rendered'
        with mock.patch.object(supervisor, 'invoke') as invoke:
            result = supervisor.render(self.home, output, sys.executable, str(self.codex), 'Darwin')
        invoke.assert_not_called()
        self.assertFalse(result['activated'])
        self.assertEqual(list(output.iterdir()), [Path(result['rendered'])])
        self.assertEqual(Path(result['rendered']).stat().st_mode & 0o777, 0o600)
        self.assertFalse((self.home / 'service-manager.json').exists())

    def test_launchd_uses_argument_vector_and_restart_throttle(self):
        spec = self.spec()
        value = plistlib.loads(spec['content'])
        self.assertEqual(value['ProgramArguments'], spec['argv'])
        self.assertEqual(value['ProgramArguments'][-1], 'run')
        self.assertIn(str(self.home), value['ProgramArguments'])
        self.assertIn(str(self.codex), value['ProgramArguments'])
        self.assertEqual(value['KeepAlive'], {'SuccessfulExit': False})
        self.assertTrue(value['RunAtLoad'])
        self.assertEqual(value['ThrottleInterval'], 15)
        self.assertEqual(value['Umask'], 0o077)
        self.assertNotIn('CODEX_API_KEY', value['EnvironmentVariables'])

    def test_systemd_quotes_spaced_arguments_without_shell(self):
        spec = self.spec('Linux')
        text = spec['content'].decode()
        self.assertIn('"' + str(self.home) + '"', text)
        self.assertIn('"' + str(self.codex) + '"', text)
        self.assertIn('Restart=on-failure\nRestartSec=15s', text)
        self.assertIn('KillMode=control-group', text)
        self.assertIn('UMask=0077', text)
        self.assertNotIn('/bin/sh -c', text)

    def test_systemd_rejects_expansion_and_control_characters(self):
        for value in ['name%h', 'name$PATH', 'line\nbreak', 'quote"', 'back\\slash', 'null\0']:
            with self.subTest(value=value), self.assertRaises(supervisor.SupervisorError):
                supervisor.systemd_path(value)

    def test_distinct_homes_have_distinct_service_names(self):
        other = self.base / 'another-home'
        self.assertNotEqual(supervisor.identity(self.home), supervisor.identity(other))
        self.assertEqual(supervisor.identity(self.home), supervisor.identity(self.home / '.'))

    def test_install_requires_actual_installed_runner_and_supported_python(self):
        (self.home / 'program' / 'autonomous.py').unlink()
        with self.assertRaisesRegex(supervisor.SupervisorError, 'no regular'):
            self.spec()
        with mock.patch.object(subprocess, 'run', return_value=self.manager_result(stdout='3.9\n')):
            with self.assertRaisesRegex(supervisor.SupervisorError, '3.10'):
                supervisor.validate_python(sys.executable)

    def test_linux_registration_and_removal_use_only_managed_paths(self):
        target = self.base / 'SIMULATED-systemd' / (supervisor.identity(self.home) + '.service')
        with mock.patch.object(supervisor.platform, 'system', return_value='Linux'), \
             mock.patch.object(supervisor, 'service_target', return_value=target), \
             mock.patch.object(supervisor, 'manager_command', return_value='/SIMULATED/systemctl'), \
             mock.patch.object(supervisor, 'invoke', return_value=self.manager_result()) as invoke:
            result = supervisor.install_service(self.home, sys.executable, str(self.codex))
            self.assertTrue(result['activation_requested'])
            self.assertEqual(invoke.call_args_list, [
                mock.call(['/SIMULATED/systemctl', '--user', 'daemon-reload']),
                mock.call(['/SIMULATED/systemctl', '--user', 'enable', '--now', target.name]),
            ])
            self.assertEqual(target.stat().st_mode & 0o777, 0o600)
            self.assertEqual((self.home / 'service-manager.json').stat().st_mode & 0o777, 0o600)
            (self.home / 'checkpoint.txt').write_text('Synthetic retained work')
            removed = supervisor.uninstall_service(self.home)
            self.assertTrue(removed['task_data_retained'])
            self.assertFalse(target.exists())
            self.assertTrue((self.home / 'checkpoint.txt').exists())

    def test_launchd_second_install_does_not_kill_active_runner(self):
        target = self.base / 'SIMULATED-launchd' / (supervisor.identity(self.home) + '.plist')
        with mock.patch.object(supervisor.platform, 'system', return_value='Darwin'), \
             mock.patch.object(supervisor, 'service_target', return_value=target), \
             mock.patch.object(supervisor, 'manager_command', return_value='/SIMULATED/launchctl'), \
             mock.patch.object(supervisor, 'invoke', return_value=self.manager_result(stdout='state = running')) as invoke:
            supervisor.install_service(self.home, sys.executable, str(self.codex))
            supervisor.install_service(self.home, sys.executable, str(self.codex))
            self.assertFalse(any('-k' in call.args[0] for call in invoke.call_args_list))
            self.assertFalse(any('bootout' in call.args[0] for call in invoke.call_args_list))

    def test_unmanaged_or_modified_service_is_never_overwritten(self):
        target = self.base / 'unmanaged.service'
        target.write_text('Unrelated configuration')
        with mock.patch.object(supervisor.platform, 'system', return_value='Linux'), \
             mock.patch.object(supervisor, 'service_target', return_value=target), \
             mock.patch.object(supervisor, 'manager_command', return_value='/SIMULATED/systemctl'), \
             mock.patch.object(supervisor, 'invoke') as invoke:
            with self.assertRaisesRegex(supervisor.SupervisorError, 'No matching'):
                supervisor.install_service(self.home, sys.executable, str(self.codex))
            invoke.assert_not_called()
            self.assertEqual(target.read_text(), 'Unrelated configuration')

    def test_installer_refuses_active_autonomous_lock(self):
        with (self.home / 'autonomous.lock').open('w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(install.InstallError, 'Active autonomous runner lock'):
                with install.reserved_home(self.home, apply=False):
                    self.fail('Active runner was not excluded')

    def test_installer_emits_absolute_python_launchers_without_starting_services(self):
        package_root = self.base / 'package'
        package_root.mkdir()
        for directory in ['program', 'docs', 'installer']:
            (package_root / directory).mkdir()
        for path in ['install.sh', 'README.md', 'program/tango.py', 'program/service.py',
                     'program/autonomous.py', 'program/supervisor.py',
                     'installer/install.py', 'installer/migration_check.py']:
            (package_root / path).write_text('# Synthetic package file\n')
        destination = self.base / 'payload'
        with mock.patch.object(subprocess, 'run') as run:
            install.build_payload(self.home, package_root, destination)
            run.assert_not_called()
        for name in ['tango', 'tango-auto', 'tango-supervisor']:
            launcher = destination / 'bin' / name
            self.assertTrue(launcher.is_file())
            self.assertIn(sys.executable, launcher.read_text())
            self.assertEqual(launcher.stat().st_mode & 0o777, 0o755)


if __name__ == '__main__':
    unittest.main(verbosity=2)
