"""Subprocess-fixture tests; no model calls or native-agent acceptance are claimed."""
import contextlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'program'))
from codex_backend import BackendError, CodexBackend, process_identity


FAKE = r'''
import json, os, pathlib, sys, time, uuid
args=sys.argv[1:]
data=json.load(sys.stdin)
mode=data.get('mode','success')
work=pathlib.Path.cwd()
count=work/'invocations.txt'
count.write_text(str(int(count.read_text())+1 if count.exists() else 1))
(work/'arguments.json').write_text(json.dumps(args))
time.sleep(.05)
def emit(x):print(json.dumps(x),flush=True)
if mode in ('pre_permission','pre_quota','pre_os_permission'):
 message={'pre_permission':'Authentication failed: 401','pre_quota':'Usage limit exceeded: 429',
          'pre_os_permission':'failed to initialize client: Operation not permitted (os error 1)'}[mode]
 print(message,file=sys.stderr)
 sys.exit(1)
if mode=='stderr_network_path_warning':
 print('WARNING: proceeding, even though we could not create PATH aliases: Operation not permitted (os error 1)',file=sys.stderr)
 print('HTTP 503: network connection failed',file=sys.stderr)
 sys.exit(1)
sid=args[args.index('resume')+1] if 'resume' in args else 'fixture-'+uuid.uuid4().hex
if mode=='wrong_resume':sid='fixture-wrong-session'
if mode=='malformed':
 print('THIS IS NOT JSON',flush=True);time.sleep(.2);sys.exit(1)
if mode!='missing_session':emit({'type':'thread.started','thread_id':sid})
emit({'type':'turn.started'})
if mode in ('sleep','ignore_term'):
 if mode=='ignore_term':
  import signal
  signal.signal(signal.SIGTERM,signal.SIG_IGN)
 time.sleep(30)
if mode in ('quota','permission','network','network_path_warning','failure'):
 message={'quota':'Usage limit exceeded: 429', 'permission':'Authentication failed: 401',
          'network':'Network connection failed', 'network_path_warning':'HTTP 503: network connection failed',
          'failure':'Execution failed'}[mode]
 emit({'type':'turn.failed','error':{'message':message}})
 if mode=='network_path_warning':
  print('WARNING: proceeding, even though we could not create PATH aliases: Operation not permitted (os error 1)',file=sys.stderr)
 print('Bearer private-test-token',file=sys.stderr)
 sys.exit(1)
dest=pathlib.Path(args[args.index('--output-last-message')+1])
if mode=='bad_response':dest.write_text('not json')
elif mode=='no_response':pass
else:dest.write_text(json.dumps({'status':'candidate','value':42}))
if mode!='missing_completion':emit({'type':'turn.completed','usage':{'input_tokens':1,'output_tokens':1}})
'''


class CodexBackendTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='tango-cli-fixture-')
        self.base = Path(self.tmp.name)
        self.work = self.base / 'work'
        self.work.mkdir()
        self.fake = self.base / 'fixture-cli'
        self.fake.write_text('#!' + sys.executable + '\n' + FAKE)
        self.fake.chmod(0o755)
        self.backend = CodexBackend(self.fake, poll_interval=0.02)
        self.schema = {'type': 'object', 'properties': {'status': {'type': 'string'}}}

    def tearDown(self):
        self.tmp.cleanup()

    def run_fixture(self, mode='success', name='attempt', **kwargs):
        return self.backend.run(json.dumps({'mode': mode}), self.work, self.base / name,
                                self.schema, timeout=kwargs.pop('timeout', 5), **kwargs)

    def test_real_subprocess_records_session_completion_and_private_files(self):
        started = []
        result = self.run_fixture(on_started=lambda sid, receipt: started.append((sid, receipt)))
        self.assertEqual(result['response']['value'], 42)
        self.assertEqual(result['exit_code'], 0)
        self.assertTrue(result['turn_completed'])
        self.assertEqual(len(started), 1)
        self.assertEqual(started[0][0], result['session'])
        self.assertEqual(started[0][1]['result']['event'], 'thread.started')
        self.assertNotIn('exit_code', started[0][1]['result'])
        self.assertEqual(result['receipt']['result']['thread_id'], result['session'])
        self.assertTrue(result['receipt']['result']['process_exited'])
        self.assertEqual(result['terminal_receipt']['status'], 'completed')
        self.assertEqual(result['terminal_receipt']['tool'], 'codex.process')
        self.assertGreater(result['process_identity']['pid'], 0)
        for name in ('attempt.json', 'events.jsonl', 'stderr.log', 'prompt.txt', 'schema.json', 'response.json'):
            self.assertEqual((self.base / 'attempt' / name).stat().st_mode & 0o777, 0o600)

    def test_default_flags_preserve_sandbox_and_never_enable_dangerous_bypass(self):
        self.run_fixture()
        args = json.loads((self.work / 'arguments.json').read_text())
        self.assertIn('approval_policy="never"', args)
        self.assertIn('sandbox_mode="workspace-write"', args)
        self.assertIn('--sandbox', args)
        self.assertNotIn('--dangerously-bypass-approvals-and-sandbox', args)
        self.assertNotIn('--ignore-rules', args)
        self.assertNotIn('--ignore-user-config', args)
        self.assertNotIn('--trust', args)

    def test_resume_uses_exact_real_session_and_read_only_override(self):
        first = self.run_fixture()
        second = self.run_fixture(name='resume', session=first['session'], read_only=True)
        self.assertEqual(second['session'], first['session'])
        self.assertEqual(second['receipt']['tool'], 'codex.exec.resume')
        args = json.loads((self.work / 'arguments.json').read_text())
        self.assertIn('sandbox_mode="read-only"', args)
        self.assertEqual(args[args.index('resume') + 1], first['session'])

    def test_two_fresh_calls_use_distinct_sessions(self):
        first = self.run_fixture()
        verifier = self.run_fixture(name='verifier', read_only=True)
        self.assertNotEqual(first['session'], verifier['session'])

    def test_completed_attempt_is_idempotently_reused_but_changed_inputs_refused(self):
        first = self.run_fixture()
        replay = self.run_fixture()
        self.assertTrue(replay['cached'])
        self.assertEqual(replay['session'], first['session'])
        self.assertEqual((self.work / 'invocations.txt').read_text(), '1')
        with self.assertRaisesRegex(BackendError, 'different inputs'):
            self.run_fixture('failure')

    def test_nonzero_cli_errors_are_classified_and_secrets_not_in_exception(self):
        for mode in ('quota', 'permission', 'network', 'failure'):
            with self.subTest(mode=mode):
                with self.assertRaises(BackendError) as context:
                    self.run_fixture(mode, name=mode)
                error = context.exception
                self.assertEqual(error.kind, mode if mode != 'failure' else 'program')
                self.assertNotIn('private-test-token', str(error))
                self.assertTrue(error.record['process_exited'])
                self.assertEqual(error.terminal_receipt['status'], 'failed')
                self.assertEqual(error.record['exit_code'], 1)
                self.assertFalse(error.record['turn_completed'])

    def test_malformed_and_missing_protocol_evidence_never_pass(self):
        for mode in ('malformed', 'missing_session', 'missing_completion', 'bad_response', 'no_response'):
            with self.subTest(mode=mode):
                with self.assertRaises(BackendError) as context:
                    self.run_fixture(mode, name=mode)
                self.assertFalse(context.exception.record.get('state') == 'completed')
                self.assertTrue(context.exception.record['process_exited'])

    def test_pre_session_auth_and_quota_failures_preserve_the_actual_blocker(self):
        for mode, kind in [('pre_permission', 'permission'), ('pre_quota', 'quota'), ('pre_os_permission', 'permission')]:
            with self.subTest(mode=mode):
                with self.assertRaises(BackendError) as context:
                    self.run_fixture(mode, name=mode)
                error = context.exception
                self.assertEqual(error.kind, kind)
                self.assertIsNone(error.session)
                self.assertIsNone(error.receipt)
                self.assertTrue(error.record['process_started'])
                self.assertTrue(error.record['process_exited'])

    def test_incidental_path_alias_warning_does_not_turn_network_failure_into_permission(self):
        for mode in ('network_path_warning', 'stderr_network_path_warning'):
            with self.subTest(mode=mode):
                with self.assertRaises(BackendError) as context:
                    self.run_fixture(mode, name=mode)
                self.assertEqual(context.exception.kind, 'network')
                self.assertIn('503', str(context.exception))
                self.assertNotIn('PATH aliases', str(context.exception))

    def test_wrong_resume_session_is_rejected(self):
        with self.assertRaises(BackendError) as context:
            self.run_fixture('wrong_resume', session='expected-session')
        self.assertEqual(context.exception.kind, 'format')
        self.assertEqual(context.exception.session, 'expected-session')
        self.assertIsNone(context.exception.receipt)
        self.assertFalse(context.exception.terminal_receipt['result']['thread_observed'])

    def test_owner_cancellation_terminates_owned_process_and_persists_receipt(self):
        seen = []
        with self.assertRaises(BackendError) as context:
            self.run_fixture('sleep', on_started=lambda sid, receipt: seen.append(sid),
                             on_poll=lambda: bool(seen))
        error = context.exception
        self.assertEqual(error.kind, 'cancelled')
        self.assertEqual(error.record['state'], 'interrupted')
        self.assertTrue(error.record['process_exited'])
        self.assertIsNone(process_identity(error.record['process_identity']['pid']))
        self.assertEqual(error.terminal_receipt['status'], 'interrupted')

    def test_turn_timeout_is_not_an_unbounded_hanging_worker(self):
        with self.assertRaises(BackendError) as context:
            self.run_fixture('sleep', timeout=0.2)
        self.assertEqual(context.exception.kind, 'timeout')
        self.assertTrue(context.exception.record['process_exited'])
        self.assertIsNone(process_identity(context.exception.record['process_identity']['pid']))

    def test_spawn_failure_proves_no_child_but_does_not_invent_session(self):
        backend = CodexBackend(self.base / 'absent-cli')
        with self.assertRaises(BackendError) as context:
            backend.run('fixture', self.work, self.base / 'absent', self.schema)
        error = context.exception
        self.assertEqual(error.kind, 'dependency')
        self.assertEqual(error.record['state'], 'spawn_failed')
        self.assertFalse(error.record['process_started'])
        self.assertTrue(error.record['process_exited'])
        self.assertIsNone(error.session)
        self.assertIsNone(error.terminal_receipt)

    def test_ambiguous_spawn_gap_is_not_retried_or_declared_finished(self):
        directory = self.base / 'gap'
        directory.mkdir()
        (directory / 'attempt.json').write_text(json.dumps({'state': 'spawning', 'process_started': False}))
        with self.assertRaisesRegex(BackendError, 'Ambiguous spawn gap'):
            self.backend.recover(directory, cancel=True)
        self.assertFalse((self.work / 'invocations.txt').exists())

    def test_recovery_does_not_signal_recycled_or_forged_identity(self):
        sleeper = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(20)'], start_new_session=True)
        try:
            identity = process_identity(sleeper.pid)
            self.assertIsNotNone(identity)
            identity['start'] = 'deliberately-wrong-birth-marker'
            directory = self.base / 'stale'
            directory.mkdir()
            (directory / 'attempt.json').write_text(json.dumps({
                'state': 'running', 'process_started': True, 'process_identity': identity,
                'session': 'fixture-session'}))
            with self.assertRaisesRegex(BackendError, 'no PID was signalled'):
                self.backend.recover(directory, cancel=True)
            self.assertIsNone(sleeper.poll())
        finally:
            sleeper.terminate()
            sleeper.wait(timeout=5)

    def test_completed_recovery_returns_saved_evidence_without_new_process(self):
        result = self.run_fixture()
        recovered = self.backend.recover(self.base / 'attempt')
        self.assertEqual(recovered['terminal_receipt'], result['terminal_receipt'])
        self.assertEqual((self.work / 'invocations.txt').read_text(), '1')

    def test_owner_crash_reconciles_actual_orphan_before_allowing_new_attempt(self):
        directory = self.base / 'orphan'
        code = (
            'import sys; sys.path.insert(0,sys.argv[1]); '
            'from codex_backend import CodexBackend; '
            'CodexBackend(sys.argv[2],poll_interval=.02).run('
            '\'{"mode":"sleep"}\',sys.argv[3],sys.argv[4],{"type":"object"},timeout=30)'
        )
        owner = subprocess.Popen([sys.executable, '-B', '-c', code, str(ROOT / 'program'),
                                  str(self.fake), str(self.work), str(directory)],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        record = None
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                try:
                    record = json.loads((directory / 'attempt.json').read_text())
                    if record.get('thread_observed'):
                        break
                except (OSError, ValueError):
                    pass
                time.sleep(0.02)
            self.assertTrue(record and record.get('thread_observed'))
            owner.kill()
            owner.wait(timeout=5)
            with self.assertRaisesRegex(BackendError, 'still active'):
                self.backend.recover(directory)
            recovered = self.backend.recover(directory, cancel=True)
            self.assertEqual(recovered['state'], 'interrupted')
            self.assertTrue(recovered['process_exited'])
            self.assertIsNone(recovered['exit_code'])
            self.assertFalse(recovered['turn_completed'])
            self.assertEqual(recovered['session'], record['session'])
            self.assertEqual(recovered['terminal_receipt']['status'], 'interrupted')
            self.assertIsNone(process_identity(record['process_identity']['pid']))
        finally:
            if owner.poll() is None:
                owner.kill()
            owner.wait(timeout=5)
            if record and record.get('process_identity'):
                with contextlib.suppress(BackendError):
                    self.backend.recover(directory, cancel=True)

    def test_failure_is_not_automatically_replayed_under_same_attempt_id(self):
        with self.assertRaises(BackendError):
            self.run_fixture('failure')
        with self.assertRaisesRegex(BackendError, 'Attempt already exists'):
            self.run_fixture('failure')
        self.assertEqual((self.work / 'invocations.txt').read_text(), '1')


if __name__ == '__main__':
    unittest.main(verbosity=2)
