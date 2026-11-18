"""Submission crash/race fixtures; no model or background service is started."""
import concurrent.futures
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'program'))
import autonomous
from tango import Broker, TangoError


class SubmissionRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='tango-submission-SIMULATION-')
        self.root = Path(self.temporary.name).resolve()
        self.home = self.root / 'home'

    def tearDown(self):
        self.temporary.cleanup()

    def boot_without_executing(self):
        runner = autonomous.Runner(self.home, backend=mock.Mock())
        try:
            with mock.patch.object(runner, 'cycle', return_value=False):
                runner.run(once=True)
            return runner.last_error
        finally:
            runner.close()

    def current_goal(self):
        broker = Broker(self.home)
        try:
            run = broker.status()[0]['run_id']
            return run, broker.goal(broker.run(run))
        finally:
            broker.db.close()

    def test_concurrent_submissions_never_cross_goals_and_criteria(self):
        barrier = threading.Barrier(2)

        def submit(number):
            barrier.wait(timeout=5)
            try:
                return autonomous.create_job(self.home, f'Synthetic goal {number}',
                                             [f'Synthetic criterion {number}'], [], 1)
            except TangoError as error:
                return {'error': str(error)}

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(submit, number) for number in (1, 2)]
            results = [future.result(timeout=10) for future in futures]
        self.assertEqual(sum('run_id' in result for result in results), 1, results)
        self.assertEqual(sum('error' in result for result in results), 1, results)
        run, goal = self.current_goal()
        winner = int(goal['body'].rsplit(' ', 1)[1])
        self.assertEqual(goal['criteria'], [{'id': 'criterion_1',
                                            'description': f'Synthetic criterion {winner}'}])
        saved = json.loads((self.home / 'autonomous' / 'jobs' / (run + '.json')).read_text())
        self.assertEqual(saved['run_id'], run)
        self.assertEqual(saved['backend'], 'codex_cli')

    def test_crash_writing_job_is_replayed_on_runner_startup(self):
        write = autonomous.atomic_json

        def crash(path, value):
            if Path(path).parent.name == 'jobs':
                raise OSError('SIMULATION: crash before enrollment file commit')
            return write(path, value)

        with mock.patch.object(autonomous, 'atomic_json', side_effect=crash):
            with self.assertRaisesRegex(OSError, 'SIMULATION'):
                autonomous.create_job(self.home, 'Synthetic retained goal', ['Retained criterion'], [], 1)
        run, before = self.current_goal()
        job = self.home / 'autonomous' / 'jobs' / (run + '.json')
        self.assertFalse(job.exists())
        self.assertIsNone(self.boot_without_executing())
        self.assertTrue(job.is_file())
        self.assertEqual(self.current_goal()[1], before)
        broker = Broker(self.home)
        try:
            self.assertEqual(broker.db.execute('SELECT COUNT(*) FROM runs').fetchone()[0], 1)
            self.assertEqual(len(broker.pending(run_id=run)), 1)
        finally:
            broker.db.close()
        intents = list((self.home / 'autonomous' / 'submissions').glob('*.json'))
        self.assertEqual(len(intents), 1)
        self.assertEqual(json.loads(intents[0].read_text())['state'], 'enrolled')

    def test_crash_after_job_write_preserves_a_later_goal_revision(self):
        write = autonomous.atomic_json

        def crash(path, value):
            if Path(path).parent.name == 'submissions' and value.get('state') == 'enrolled':
                raise OSError('SIMULATION: crash before submission completion marker')
            return write(path, value)

        with mock.patch.object(autonomous, 'atomic_json', side_effect=crash):
            with self.assertRaisesRegex(OSError, 'SIMULATION'):
                autonomous.create_job(self.home, 'Original synthetic goal', ['Original criterion'], [], 1)
        run, _ = self.current_goal()
        self.assertTrue((self.home / 'autonomous' / 'jobs' / (run + '.json')).is_file())
        broker = Broker(self.home)
        try:
            broker.route('SIMULATION-revision', '\\TANGO_TASK_1 update goal: Revised synthetic goal', 'test_injection')
            broker.set_contract(run, 2, [{'id': 'new', 'description': 'Revised criterion'}])
            revised = broker.goal(broker.run(run))
        finally:
            broker.db.close()
        self.assertIsNone(self.boot_without_executing())
        self.assertEqual(self.current_goal()[1], revised)
        intents = list((self.home / 'autonomous' / 'submissions').glob('*.json'))
        self.assertEqual(json.loads(intents[0].read_text())['state'], 'enrolled')

    def test_running_service_replays_interrupted_submit_without_restart(self):
        runner = autonomous.Runner(self.home, backend=mock.Mock())
        try:
            # The service has already completed startup with an empty queue.
            runner.recover_submissions()
            write = autonomous.atomic_json

            def crash(path, value):
                if Path(path).parent.name == 'jobs':
                    raise OSError('SIMULATION: submitting process interrupted while service stays alive')
                return write(path, value)

            with mock.patch.object(autonomous, 'atomic_json', side_effect=crash):
                with self.assertRaisesRegex(OSError, 'SIMULATION'):
                    autonomous.create_job(self.home, 'Live-service synthetic goal',
                                          ['Live-service criterion'], [], 1)
            run, before = self.current_goal()
            self.assertFalse(runner.owns(run))
            with mock.patch.object(runner, 'process') as dispatch:
                self.assertTrue(runner.cycle())
                dispatch.assert_called_once()
                self.assertEqual(dispatch.call_args.args[0]['run_id'], run)
            self.assertTrue(runner.owns(run))
            self.assertEqual(self.current_goal()[1], before)
            self.assertIsNone(runner.last_error)
        finally:
            runner.close()

    def test_foreign_native_task_is_not_enrolled_by_pending_intent(self):
        broker = Broker(self.home)
        try:
            run = broker.route('SIMULATION-native-start', '\\TANGO_TASK_1 start: Existing native goal', 'test_injection')['run_id']
            broker.set_contract(run, 1, [{'id': 'native', 'description': 'Original native criterion'}])
            before = broker.goal(broker.run(run))
        finally:
            broker.db.close()
        message_id = uuid.uuid4().hex
        intent = {'message_id': message_id, 'backend': 'codex_cli', 'state': 'pending',
                  'goal': 'Different synthetic CLI goal', 'criteria': ['Different criterion'],
                  'sources': [], 'slot': 1, 'created': time.time()}
        autonomous.atomic_json(self.home / 'autonomous' / 'submissions' / (message_id + '.json'), intent)
        error = self.boot_without_executing()
        self.assertIn('slot was taken', error)
        self.assertEqual(self.current_goal()[1], before)
        runner = autonomous.Runner(self.home, backend=mock.Mock())
        try:
            self.assertFalse(runner.owns(run))
        finally:
            runner.close()

    def test_normal_goal_file_trailing_newline_is_accepted(self):
        goal_file = self.root / 'task.txt'
        goal_file.write_text('Synthetic goal from a normal text file\n', encoding='utf-8')
        result = subprocess.run([sys.executable, '-B', str(ROOT / 'program' / 'autonomous.py'),
                                 '--home', str(self.home), 'submit', '--goal-file', str(goal_file),
                                 '--enqueue-only'], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        run, goal = self.current_goal()
        self.assertEqual(goal['body'], 'Synthetic goal from a normal text file')
        self.assertTrue((self.home / 'autonomous' / 'jobs' / (run + '.json')).is_file())


if __name__ == '__main__':
    unittest.main(verbosity=2)
