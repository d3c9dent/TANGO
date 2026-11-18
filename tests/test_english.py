"""English UI regressions; every mutable broker lives in a temporary home."""
import ast
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/"program"))
from tango import Broker, VERSION

EPIGRAPH = '“The great Tang scholar Wu Wenzhong once said: ‘Mathematical proof is essentially coding.’”'


class EnglishTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='tango-english-test-')
        self.b = Broker(self.tmp.name, simulation=True)
        self.serial = 0

    def tearDown(self):
        self.b.db.close()
        self.tmp.cleanup()

    def route(self, body, slot=1):
        self.serial += 1
        return self.b.route(f'english-{self.serial}', f'\\TANGO_TASK_{slot} {body}', 'test_injection')

    def start(self, goal='Produce an exact result'):
        return self.route('start: ' + goal)['run_id']

    def dispatch(self, run):
        self.b.tick()
        q = next(q for q in self.b.pending() if q['run_id'] == run)
        c = self.b.claim(q['id'], 'english-test-owner')
        session = 'simulation-' + q['id']
        self.b.bind_native(q['id'], c['owner'], c['fence'], session,
                           {'kind': 'simulation', 'agent_id': session})
        return q['id'], c['owner'], c['fence']

    def candidate(self, run, claim):
        p = self.b.path(run) / 'work' / 'answer.txt'
        p.write_text('42')
        return self.b.candidate(*claim, [str(p)])

    def assert_english(self, text):
        self.assertTrue(text.isascii(), repr(text))

    def assert_outbox_english(self):
        for item in self.b.outbox():
            self.assert_english(item['text'])
            self.assertTrue(item['text'].startswith(f"\\TANGO_TASK_{item['slot']} [run="))

    def test_english_control_lifecycle_and_exact_notices(self):
        self.assertIn('Provide a goal or a control command', self.route('')['messages'][0])
        for command in ('status', 'pause', 'resume', 'stop'):
            self.assert_english(self.route(command)['messages'][0])
        run = self.start()
        self.assertIn('Goal: Produce an exact result; saved and waiting for native adapter dispatch', self.b.outbox()[-1]['text'])
        self.assertIn('Slot occupied;', self.route('new: replacement')['messages'][0])
        self.assertIn('Only paused tasks can resume;', self.route('resume')['messages'][0])
        self.assertIn('Checkpoint and history retained;', self.route('pause')['messages'][0])
        self.assertEqual(self.b.run(run)['state'], 'PAUSED')
        self.assertIn('Resuming from the latest valid checkpoint', self.route('resume')['messages'][0])
        self.assertIn('Directed input received;', self.route('Keep exact output')['messages'][0])
        self.route('stop')
        self.assertIn('Task has already ended', self.route('pause')['messages'][0])
        self.assertIn('use new: <goal>', self.route('More input')['messages'][0])
        self.assertEqual(self.b.run(run)['state'], 'STOPPED')
        self.assertNotEqual(self.route('new: replacement')['run_id'], run)
        self.assert_outbox_english()

    def test_chinese_control_aliases_emit_english(self):
        for command in ('状态', '暂停', '继续', '停止'):
            self.assert_english(self.route(command)['messages'][0])
        run = self.route('启动：Produce a result')['run_id']
        self.assertEqual(self.b.goal(self.b.run(run))['body'], 'Produce a result')
        self.assertEqual(self.route('新任务：replacement')['run_id'], run)
        self.route('暂停')
        self.assertEqual(self.b.run(run)['state'], 'PAUSED')
        self.route('继续')
        self.assertEqual(self.b.run(run)['state'], 'RUNNING')
        self.route('停止')
        self.assertEqual(self.b.run(run)['state'], 'STOPPED')
        self.assertNotEqual(self.route('新任务：replacement')['run_id'], run)
        self.assert_outbox_english()

    def test_case_insensitive_english_keeps_goal_bytes(self):
        goal = 'MiXeD Case goal\nWith EXACT second line'
        run = self.route('StArT: ' + goal)['run_id']
        self.assertEqual(self.b.goal(self.b.run(run))['body'], goal)
        self.route('PAUSE')
        self.assertEqual(self.b.run(run)['state'], 'PAUSED')
        self.route('ReSuMe')
        self.assertEqual(self.b.run(run)['state'], 'RUNNING')
        self.route('STOP')
        replacement = self.route('NEW：Next MiXeD Goal')['run_id']
        self.assertEqual(self.b.goal(self.b.run(replacement))['body'], 'Next MiXeD Goal')
        self.assert_outbox_english()

    def test_english_and_chinese_goal_revision_aliases(self):
        aliases = ('revise goal', 'update goal', 'set goal', 'set acceptance',
                   '修改目标', '更新目标', '目标改为', '验收改为', 'UPDATE GOAL')
        run = self.start()
        for version, alias in enumerate(aliases, 2):
            with self.subTest(alias=alias):
                self.b.set_contract(run, version - 1, [{'id': 'exact', 'description': 'Exact result'}], ['Native only'])
                body = f'Revised Goal {version}\nPreserve second line'
                result = self.route(alias + ': ' + body)
                self.assert_english(result['messages'][0])
                current = self.b.run(run)
                self.assertEqual(current['goal_version'], version)
                self.assertEqual(self.b.goal(current)['body'], body)
                self.assertEqual(self.b.goal(current)['criteria'], [])
                self.assertEqual(self.b.goal(current)['constraints'], ['Native only'])
        self.assertEqual(self.b.db.execute('SELECT COUNT(*) FROM goals').fetchone()[0], 1 + len(aliases))
        self.assert_outbox_english()

    def test_goal_revision_keeps_paused_run_paused(self):
        run = self.start()
        self.route('pause')
        self.route('update goal: Revised goal')
        self.assertEqual(self.b.run(run)['state'], 'PAUSED')
        self.assertEqual(self.b.run(run)['goal_version'], 2)
        self.b.tick()
        self.assertEqual(self.b.pending(), [])
        self.assert_outbox_english()

    def test_chinese_user_goal_is_preserved_inside_english_notice(self):
        goal = '保留用户原始目标，逐字不变'
        result = self.route('start: ' + goal)
        run = result['run_id']
        self.assertEqual(self.b.goal(self.b.run(run))['body'], goal)
        message = result['messages'][0]
        self.assertIn('Goal: ' + goal + '; saved', message)
        self.assert_english(message.replace(goal, '<original user goal>'))
        self.route('还要保留原文')
        self.assert_outbox_english_except_goal(goal)

    def assert_outbox_english_except_goal(self, goal):
        for item in self.b.outbox():
            self.assert_english(item['text'].replace(goal, '<original user goal>'))

    def test_implicit_goal_notice_is_english(self):
        result = self.route('Produce a complete result')
        self.assertIn('Goal: Produce a complete result; saved', result['messages'][0])
        self.assert_outbox_english()

    def test_exact_three_prefixes_retained(self):
        for slot in (1, 2, 3):
            result = self.route('start: result', slot)
            self.assertTrue(result['messages'][0].startswith(f'\\TANGO_TASK_{slot} [run='))
        for text in ('\\tango_task_1 status', '\\TANGO_TASK_4 status', '\\TANGO_TASK_10 status',
                     ' \\TANGO_TASK_1 status', 'quote \\TANGO_TASK_1 status',
                     '```\n\\TANGO_TASK_1 status\n```'):
            self.assertFalse(self.b.route('invalid-prefix', text, 'test_injection')['routed'])
        self.assert_outbox_english()

    def test_recovery_notice_is_english(self):
        run = self.start()
        self.dispatch(run)
        self.b.db.execute('UPDATE runs SET lease_until=0 WHERE run_id=?', (run,))
        self.b.recover()
        self.assertIn('Execution lease expired;', self.b.outbox()[-1]['text'])
        self.assert_outbox_english()

    def test_candidate_default_notice_is_english(self):
        run = self.start()
        result = self.candidate(run, self.dispatch(run))
        self.assertIn('Candidate saved; awaiting independent verification', result['message'])
        self.assertFalse(result['completed'])
        self.assert_outbox_english()

    def test_candidate_blocker_notice_is_english(self):
        run = self.start()
        self.b.fail(*self.dispatch(run), 'materials', 'Required file missing')
        self.route('New material must be checked')
        result = self.candidate(run, self.dispatch(run))
        self.assertIn('prerequisites remain unresolved; candidate retained', result['message'])
        self.assertEqual(self.b.run(run)['state'], 'WAITING')
        self.assert_outbox_english()

    def test_verification_success_and_failure_notices_are_english(self):
        run = self.start()
        self.b.set_contract(run, 1, [{'id': 'exact', 'description': 'Exact result'}])
        for passed in (False, True):
            self.candidate(run, self.dispatch(run))
            claim = self.dispatch(run)
            r = self.b.run(run)
            report = {key: r[key] for key in ('goal_version', 'input_version', 'artifact_version')}
            report.update(checks=[{'id': 'exact', 'passed': passed, 'evidence': 'simulation: deterministic equality'}], missing=[])
            verdict = self.b.verify(*claim, report)
            self.assertEqual(verdict['passed'], passed)
            expected = 'All acceptance criteria are supported by evidence' if passed else 'Verification failed; goal retained'
            self.assertIn(expected, self.b.outbox()[-1]['text'])
        self.assert_outbox_english()

    def test_retry_and_wait_notices_are_english(self):
        for slot, kind in ((1, 'program'), (2, 'permission'), (3, 'quota')):
            run = self.route('start: result', slot)['run_id']
            self.b.fail(*self.dispatch(run), kind, 'Actual failure details', checkpoint={'step': 1})
            expected = 'Checkpoint saved; diagnosis and reverification will follow' if kind == 'program' else 'Waiting for prerequisites'
            self.assertIn(expected, self.b.outbox()[-1]['text'])
        self.assert_outbox_english()

    def test_historical_run_marker_is_english_without_rewriting_stored_text(self):
        run = self.start()
        stored = self.b.db.execute('SELECT text FROM outbox WHERE run_id=?', (run,)).fetchone()[0]
        self.route('stop')
        self.route('new: next goal')
        historical = next(item for item in self.b.outbox() if item['run_id'] == run)
        self.assertTrue(historical['text'].endswith('[historical run message]'))
        self.assertEqual(self.b.db.execute('SELECT text FROM outbox WHERE run_id=? ORDER BY created LIMIT 1', (run,)).fetchone()[0], stored)
        self.assert_outbox_english()

    def test_owned_source_strings_contain_no_chinese_except_aliases(self):
        allowed = {'状态', '暂停', '停止', '继续',
                   r'^(启动|新任务|start|new)\s*[:：]\s*([\s\S]+)$',
                   r'^(修改目标|更新目标|目标改为|验收改为|revise goal|update goal|set goal|set acceptance)\s*[:：]\s*([\s\S]+)$'}
        for name in ('tango.py', 'service.py'):
            for node in ast.walk(ast.parse((ROOT / "program" / name).read_text())):
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    if any('\u3400' <= c <= '\u9fff' for c in node.value):
                        self.assertIn(node.value, allowed, (name, node.lineno, node.value))

    def test_cli_help_status_version_and_error_are_english(self):
        cmd = [sys.executable, str(ROOT / 'program' / 'tango.py'), '--home', self.tmp.name]
        for args in (['--help'], ['--version'], ['status'], ['doctor'], ['route', '--help']):
            result = subprocess.run(cmd + args, capture_output=True, text=True, check=True)
            self.assert_english(result.stdout)
            self.assert_english(result.stderr)
        self.assertEqual(VERSION, '0.4.0-autonomous')
        result = subprocess.run(cmd + ['call', 'export'], input='{"run_id":"absent"}', capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(json.loads(result.stderr)['error'], 'Unknown run')
        self.assert_english(result.stderr)
        service_help = subprocess.run([sys.executable, str(ROOT / 'program' / 'service.py'), '--help'], capture_output=True, text=True, check=True)
        self.assert_english(service_help.stdout)

    def test_readme_is_english_and_retains_epigraph_and_acceptance_limits(self):
        readme = (ROOT / 'README.md').read_text()
        self.assertEqual(readme.splitlines().count(EPIGRAPH), 1)
        self.assertIn('Mathematical proof is essentially coding.', readme)
        self.assertIn('## Project epigraph', readme)
        self.assertIn('TANGO 0.4.0 Autonomous Source', readme)
        self.assertIn('docs/USAGE.md', readme)
        self.assertIn('docs/AUTONOMOUS.md', readme)
        self.assertIn('docs/NATIVE_PARENT_LOOP.md', readme)
        self.assertIn('A real five-agent, ten-round discussion has not yet passed end-to-end acceptance.', readme)
        self.assertIn('The original collaboration adapter does not wake the native parent on its own.', readme)
        # English prose may use typographic quotation marks and dashes.
        self.assertFalse(any('\u3400' <= c <= '\u9fff' for c in readme))

    def test_public_distribution_specification_remains_byte_exact(self):
        data = (ROOT / 'spec' / 'user_spec.txt').read_bytes()
        self.assertEqual(len(data), 1825)
        self.assertEqual(hashlib.sha256(data).hexdigest(), '82a5694a7c60ee7c0528f06527c236419efb451978f3df615c1ba961c8f8ec68')


if __name__ == '__main__':
    unittest.main(verbosity=2)
