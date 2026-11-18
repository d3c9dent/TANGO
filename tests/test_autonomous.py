"""End-to-end local orchestration fixtures, not real model acceptance evidence.

These tests launch a synthetic CLI subprocess through the real backend and use
the production broker receipt path (simulation=False). The synthetic executable
only produces deterministic test data; it does not impersonate a live model in
the release's acceptance claims.
"""
import contextlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'program'))
from autonomous import Runner, create_job
from codex_backend import CodexBackend
from tango import Broker


FIXTURE = r'''
import fcntl, json, pathlib, sys, time, uuid
CONTROL = pathlib.Path(__CONTROL__)
args = sys.argv[1:]
prompt = sys.stdin.read()
data = json.loads(prompt.splitlines()[-1])
schema = json.loads(pathlib.Path(args[args.index('--output-schema')+1]).read_text())
props = schema['properties']
config = json.loads((CONTROL/'config.json').read_text())
scenario = config['scenario']
if 'artifacts' in props: role='execute'
elif 'contrary' in props: role='verify'
elif 'coverage' in props: role='prepare'
elif 'next_step' in props: role='post'
elif 'direction' in props: role='decision'
elif 'checks' in props: role='prerequisite'
else: role='opening'
session = args[args.index('resume')+1] if 'resume' in args else 'fixture-'+uuid.uuid4().hex
with (CONTROL/'state.lock').open('a+') as lock:
 fcntl.flock(lock,fcntl.LOCK_EX)
 state_file=CONTROL/'state.json'
 state=json.loads(state_file.read_text()) if state_file.exists() else {'calls':[], 'counts':{}}
 count=state['counts'][role]=state['counts'].get(role,0)+1
 state['calls'].append({'role':role,'session':session,'resume':'resume' in args,
                        'round':data.get('round'), 'context_hash':data.get('context_hash'),
                        'prior_posts':len(data.get('previous_rounds',[]))})
 state_file.write_text(json.dumps(state))
time.sleep(.04)
if scenario=='pre_quota' and role=='execute' and count==1:
 print('Usage limit exceeded: 429',file=sys.stderr);sys.exit(1)
print(json.dumps({'type':'thread.started','thread_id':session}),flush=True)
print(json.dumps({'type':'turn.started'}),flush=True)
none={'kind':'none','evidence':''}
if role=='execute':
 if scenario in ('pause','crash') and count==1:time.sleep(30)
 if scenario=='permission' and count==1:
  answer={'status':'blocked','summary':'Fixture prerequisite missing','artifacts':[],
          'checkpoint':'Await explicit fixture permission','blocker':{'kind':'permission','evidence':'Fixture permission unavailable'}}
 elif (scenario=='no_progress' and count<=2) or (scenario=='meeting' and count<=10):
  answer={'status':'no_progress','summary':'Fixture route failed; try another route','artifacts':[],
          'checkpoint':'Attempt '+str(count)+' retained','blocker':none}
 else:
  result=pathlib.Path.cwd()/'result.txt'
  result.write_text('wrong' if scenario=='repair' and count==1 else '42')
  answer={'status':'candidate','summary':'Fixture candidate written','artifacts':['result.txt'],
          'checkpoint':'Candidate is in result.txt','blocker':none}
elif role=='verify':
 passed=(pathlib.Path.cwd()/'result.txt').read_text()=='42'
 answer={'original_goal_status':'satisfied' if passed else 'not_satisfied',
         'disposition':'VERIFIED_GOAL_SATISFIED' if passed else 'REPAIR',
         'checks':[{'id':c['id'],'passed':passed,'evidence':'Fixture read result.txt and compared exact bytes with 42'} for c in data['goal']['criteria']],
         'missing':[], 'contrary':{'summary':'','evidence':'','refutes_criterion_ids':[]}}
elif role=='prerequisite':
 answer={'checks':[{'kind':b['kind'],'met':bool(config.get('unblock')),
                   'evidence':{'method':'fixture prerequisite recheck','result':'available' if config.get('unblock') else 'unavailable',
                               'reference':'authenticated fixture follow-up input'}} for b in data['blockers']]}
elif role=='prepare':
 catalogue=data['catalogue']
 summary={k:[] for k in ['accepted_results','failed_routes','conditional_assumptions','open_obligations','unverified_findings']}
 summary['failed_routes']=[{'id':'fixture-routes','statement':'Ten synthetic no-progress routes are retained',
                            'scope':'Test fixture only','verification_status':'rejected','duplicate_of':'',
                            'sources':[{'path':s['path'],'sha256':s['sha256']} for s in catalogue['sources']]}]
 answer={'status':'prepared','blocker':none,'summary':summary,
         'coverage':{'claim':'scoped_complete','search_method':'Read every synthetic source in the declared catalogue',
                     'unavailable':[],'excluded':[],'caveats':[],'source_dispositions':[]}}
elif role=='post':
 if len(data['previous_rounds'])!=5*(data['round']-1):
  print(json.dumps({'type':'turn.failed','error':{'message':'Barrier violation in fixture'}}));sys.exit(1)
 answer={k:'Fixture round '+str(data['round'])+' substantive '+k for k in ['analysis','evidence','objections','open_obligations','next_step']}
elif role=='decision':
 answer={k:'Fixture discussion next direction: write exact 42' for k in ['direction','rationale','next_steps','expected_artifacts','open_obligations','rejected_routes','dissent','stop_conditions']}
 answer['original_goal_preserved']=True
else:answer={'analysis':'Fixture coordinator continuity confirmed'}
pathlib.Path(args[args.index('--output-last-message')+1]).write_text(json.dumps(answer))
print(json.dumps({'type':'turn.completed','usage':{'input_tokens':1,'output_tokens':1}}),flush=True)
'''


class AutonomousLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='tango-autonomous-fixture-')
        self.root = Path(self.temp.name)
        self.home = self.root / 'home'
        self.control = self.root / 'fixture-control'
        self.control.mkdir()
        self.cli = self.root / 'fixture-cli'
        self.cli.write_text('#!' + sys.executable + '\n' + FIXTURE.replace('__CONTROL__', repr(str(self.control))))
        self.cli.chmod(0o755)
        self.runner = None

    def tearDown(self):
        if self.runner:
            self.runner.close()
        self.temp.cleanup()

    def configure(self, scenario, **kwargs):
        (self.control / 'config.json').write_text(json.dumps({'scenario': scenario, **kwargs}))

    def start(self, scenario):
        self.configure(scenario)
        self.run_id = create_job(self.home, 'Write exact 42 into result.txt', ['result.txt contains exactly 42'], [], 1)['run_id']
        self.runner = Runner(self.home, backend=CodexBackend(self.cli, poll_interval=0.02), poll_interval=0.02, timeout=10)
        return self.run_id

    def state(self):
        return json.loads((self.control / 'state.json').read_text())

    def advance(self, terminal='COMPLETED', seconds=20):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            self.runner.cycle()
            run = self.runner.broker.run(self.run_id)
            if run['state'] == terminal:
                return run
            if run['state'] in {'STOPPED', 'PAUSED'} or (run['state'] == 'WAITING' and run['wait_reason'] != 'retry_backoff'):
                self.fail(f'Unexpected terminal state {run["state"]}: {self.runner.last_error}')
            time.sleep(0.02)
        self.fail(f'No {terminal} outcome: {self.runner.broker.run(self.run_id)}; {self.runner.last_error}')

    def test_failed_independent_verification_repairs_and_finishes_without_new_user_input(self):
        self.start('repair')
        self.advance()
        calls = self.state()['calls']
        self.assertEqual([c['role'] for c in calls], ['execute', 'verify', 'execute', 'verify'])
        self.assertEqual(len({c['session'] for c in calls}), 4)
        reports = self.runner.broker.db.execute('SELECT passed FROM verifications ORDER BY created').fetchall()
        self.assertEqual([r[0] for r in reports], [0, 1])
        self.assertFalse(self.runner.broker.simulation)
        before = len(calls)
        self.assertFalse(self.runner.cycle())
        self.assertEqual(len(self.state()['calls']), before)

    def test_no_progress_replans_and_retains_checkpoint_until_candidate_passes(self):
        self.start('no_progress')
        self.advance()
        self.assertEqual(self.state()['counts'], {'execute': 3, 'verify': 1})
        self.assertEqual(self.runner.broker._calculation_count(self.run_id), 3)
        failures = self.runner.broker.db.execute("SELECT result FROM requests WHERE state='failed'").fetchall()
        self.assertEqual(len(failures), 2)
        self.assertTrue(all(json.loads(row[0])['kind'] == 'no_progress' for row in failures))

    def test_permission_waits_without_retries_then_authenticated_input_rechecks(self):
        self.start('permission')
        self.runner.cycle()
        self.assertEqual(self.runner.broker.run(self.run_id)['state'], 'WAITING')
        self.assertFalse(self.runner.cycle())
        self.assertEqual(self.state()['counts'], {'execute': 1})
        self.configure('permission', unblock=True)
        self.runner.broker.route(uuid.uuid4().hex, '\\TANGO_TASK_1 Fixture permission is now explicitly granted', 'user')
        self.advance()
        self.assertEqual(self.state()['counts'], {'execute': 2, 'prerequisite': 1, 'verify': 1})
        self.assertEqual(self.runner.broker.prerequisites(self.run_id), [])

    def test_pre_session_quota_error_is_reconciled_then_waits_for_new_input(self):
        self.start('pre_quota')
        self.runner.cycle()
        self.assertEqual(self.runner.broker.run(self.run_id)['state'], 'WAITING')
        self.assertEqual(self.runner.broker.run(self.run_id)['wait_reason'], 'quota')
        self.assertEqual(self.runner.broker.cancellations(run_id=self.run_id), [])
        self.assertFalse(self.runner.cycle())
        self.configure('pre_quota', unblock=True)
        self.runner.broker.route(uuid.uuid4().hex, '\\TANGO_TASK_1 Fixture quota has been restored; recheck it', 'user')
        self.advance()
        self.assertEqual(self.state()['counts']['prerequisite'], 1)

    def test_mandatory_inventory_and_five_by_ten_meeting_continue_into_real_next_request(self):
        self.start('meeting')
        self.advance(seconds=60)
        state = self.state()
        self.assertEqual(state['counts']['execute'], 11)
        self.assertEqual(state['counts']['post'], 50)
        self.assertEqual(state['counts']['prepare'], 1)
        self.assertEqual(state['counts']['decision'], 1)
        posts = [call for call in state['calls'] if call['role'] == 'post']
        self.assertEqual(len({call['session'] for call in posts}), 5)
        for round_number in range(1, 11):
            current = [call for call in posts if call['round'] == round_number]
            self.assertEqual(len(current), 5)
            self.assertEqual(len({call['context_hash'] for call in current}), 1)
            self.assertTrue(all(call['prior_posts'] == 5 * (round_number - 1) for call in current))
            self.assertTrue(all(call['resume'] == (round_number > 1) for call in current))
        meeting = self.runner.broker.db.execute('SELECT * FROM meetings').fetchone()
        self.assertEqual(meeting['state'], 'decided')
        self.assertEqual(meeting['round'], 11)
        self.assertEqual(self.runner.broker.db.execute('SELECT COUNT(*) FROM meeting_posts').fetchone()[0], 50)

    def test_pause_during_running_worker_cancels_then_resume_continues(self):
        self.start('pause')
        self.runner.close()
        self.runner = None
        errors = []

        def run_owner():
            owner = Runner(self.home, backend=CodexBackend(self.cli, poll_interval=0.02), poll_interval=0.02, timeout=10)
            try:
                owner.run(until_run=self.run_id)
            except Exception as exc:
                errors.append(exc)
            finally:
                owner.close()

        worker = threading.Thread(target=run_owner)
        worker.start()
        broker = Broker(self.home)
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                row = broker.db.execute("SELECT native_session FROM requests WHERE state='claimed'").fetchone()
                if row and row[0]:
                    break
                time.sleep(0.02)
            self.assertTrue(row and row[0])
            broker.route(uuid.uuid4().hex, '\\TANGO_TASK_1 pause', 'user')
            worker.join(timeout=8)
            self.assertFalse(worker.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(broker.run(self.run_id)['state'], 'PAUSED')
            self.assertEqual(broker.cancellations(run_id=self.run_id), [])
            broker.route(uuid.uuid4().hex, '\\TANGO_TASK_1 resume', 'user')
        finally:
            if worker.is_alive():
                broker.route(uuid.uuid4().hex, '\\TANGO_TASK_1 stop', 'user')
                worker.join(timeout=12)
            broker.db.close()
        self.runner = Runner(self.home, backend=CodexBackend(self.cli, poll_interval=0.02), timeout=10)
        self.advance()
        self.assertEqual(self.state()['counts']['execute'], 2)

    def test_supervisor_crash_recovers_owned_orphan_before_continuing(self):
        self.start('crash')
        self.runner.close()
        self.runner = None
        owner = subprocess.Popen([
            sys.executable, '-B', str(ROOT / 'program' / 'autonomous.py'),
            '--home', str(self.home), '--codex', str(self.cli), '--timeout', '30', 'run',
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        journal = None
        broker = Broker(self.home)
        try:
            deadline = time.monotonic() + 6
            while time.monotonic() < deadline:
                row = broker.db.execute("SELECT native_session,native_receipt FROM requests WHERE state='claimed'").fetchone()
                if row and row[0]:
                    journal = Path(json.loads(row[1])['result']['journal_path'])
                    break
                time.sleep(0.02)
            self.assertIsNotNone(journal, 'Fixture CLI was not actually bound before owner crash')
            owner.kill()
            owner.wait(timeout=5)
            self.runner = Runner(self.home, backend=CodexBackend(self.cli, poll_interval=0.02), timeout=10)
            self.runner.recover_owned_claims()
            self.assertEqual(self.runner.broker.cancellations(run_id=self.run_id), [])
            recovered = json.loads(journal.read_text())
            self.assertEqual(recovered['state'], 'interrupted')
            self.assertTrue(recovered['process_exited'])
            self.assertIsNone(recovered['exit_code'])
            self.advance()
            self.assertEqual(self.state()['counts'], {'execute': 2, 'verify': 1})
        finally:
            if owner.poll() is None:
                owner.kill()
            owner.wait(timeout=5)
            broker.db.close()
            if journal:
                with contextlib.suppress(Exception):
                    CodexBackend(self.cli).recover(journal.parent, cancel=True)


if __name__ == '__main__':
    unittest.main(verbosity=2)
