"""Independent continuation review: isolated simulation homes, no live state or APIs.

All routed messages are test_injection and every synthetic native receipt is
kind=simulation. SQL time shifts below affect only this test's TemporaryDirectory.
"""
import concurrent.futures
import copy
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'program'))
from tango import Broker, TangoError


class ContinuationReviewTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='tango-continuation-review-')
        self.b = Broker(self.temp.name, simulation=True)
        self.serial = 0

    def tearDown(self):
        self.b.db.close()
        self.temp.cleanup()

    def route(self, slot, body):
        self.serial += 1
        return self.b.route(f'test-injection-{self.serial}',
                            f'\\TANGO_TASK_{slot} {body}', 'test_injection')

    def start(self, slot=1, contract=True):
        run = self.route(slot, f'start: Prove the full original result for slot {slot}')['run_id']
        if contract:
            self.b.set_contract(run, 1, [
                {'id': 'full', 'description': 'Complete proof of the original result'},
                {'id': 'valid', 'description': 'Every proof step is independently checked'},
            ], ['Keep the original goal and all constraints'])
        return run

    def claim(self, run, kind='execute', tick=True):
        if tick:
            self.b.tick(run_id=run)
        pending = self.b.pending(run_id=run)
        self.assertEqual(len(pending), 1)
        q = pending[0]
        self.assertEqual(q['kind'], kind)
        owner = 'simulation-owner-' + q['id']
        c = self.b.claim(q['id'], owner)
        session = 'simulation-session-' + q['id']
        self.b.bind_native(q['id'], owner, c['fence'], session,
                           {'kind': 'simulation', 'agent_id': session})
        return q['id'], owner, c['fence']

    def write_candidate(self, run):
        path = self.b.path(run) / 'work' / 'candidate.txt'
        path.write_text('Simulation-only candidate; a partial lemma is not the full goal.')
        return path

    def candidate(self, run, claim):
        return self.b.candidate(*claim, [str(self.write_candidate(run))],
                                checkpoint={'verified_lemmas': ['L1'], 'open_goal': 'full'})

    def report(self, run, passed=True):
        r = self.b.run(run)
        return {'goal_version': r['goal_version'], 'input_version': r['input_version'],
                'artifact_version': r['artifact_version'], 'missing': [] if passed else ['full proof'],
                'checks': [
                    {'id': 'full', 'passed': passed, 'evidence': 'simulation criterion evidence'},
                    {'id': 'valid', 'passed': True, 'evidence': 'simulation independent check'},
                ]}

    def contrary_report(self, run):
        report = self.report(run, False)
        report.update(disposition='VERIFIED_CONTRARY_REQUIRES_GOAL_DECISION',
                      original_goal_status='not_satisfied', missing=[],
                      contrary={'summary': 'A checked counterexample refutes the original claim',
                                'evidence': 'Simulation-only independent refutation evidence',
                                'refutes_criterion_ids': ['full']})
        return report

    def prepare_verifier(self):
        run = self.start()
        self.candidate(run, self.claim(run))
        return run, self.claim(run, 'verify', tick=False)

    def rows(self, table, run):
        return [dict(x) for x in self.b.db.execute(f'SELECT * FROM {table} WHERE run_id=?', (run,))]

    def snapshot(self, run):
        return {table: self.rows(table, run) for table in (
            'runs', 'goals', 'inputs', 'events', 'requests', 'artifacts',
            'verifications', 'outbox', 'cancellations', 'parent_actions', 'prerequisites')}

    def race(self, operations):
        """Separate SQLite connections synchronized only after isolated initialization."""
        barrier = threading.Barrier(len(operations))
        def worker(operation):
            b = Broker(self.temp.name, simulation=True)
            try:
                barrier.wait(timeout=10)
                try:
                    return ('ok', operation(b))
                except TangoError as exc:
                    return ('fenced', str(exc))
            finally:
                b.db.close()
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(operations)) as pool:
            return list(pool.map(worker, operations))

    def test_candidate_queues_one_independent_verifier_without_tick(self):
        run = self.start()
        claim = self.claim(run)
        result = self.candidate(run, claim)
        self.assertFalse(result['completed'])
        self.assertEqual(self.b.run(run)['state'], 'VERIFYING')
        pending = self.b.pending(run_id=run)
        self.assertEqual([q['id'] for q in pending], result['next_requests'])
        self.assertEqual(len(pending), 1)
        payload = pending[0]['payload']
        self.assertEqual(payload['kind'], 'verify')
        self.assertTrue(payload['independent_context_required'])
        self.assertNotIn('checkpoint', payload)
        self.assertNotIn('latest_failure', payload)
        self.assertFalse(payload['continuation']['partial_result_is_completion'])
        self.assertEqual(self.b.parent_actions(run_id=run)[0]['action'], 'dispatch_verify')
        self.assertEqual(self.b.tick(run_id=run)['queued'], [])

    def test_incomplete_result_immediately_queues_repair_preserving_original(self):
        run, verifier = self.prepare_verifier()
        original = copy.deepcopy(self.b.goal(self.b.run(run)))
        result = self.b.verify(*verifier, self.report(run, False))
        self.assertFalse(result['passed'])
        self.assertEqual(result['state'], 'REPAIRING')
        pending = self.b.pending(run_id=run)
        self.assertEqual(len(pending), 1)
        self.assertEqual(result['next_requests'], [pending[0]['id']])
        self.assertEqual(pending[0]['kind'], 'execute')
        self.assertEqual(pending[0]['payload']['goal'], original)
        self.assertEqual(pending[0]['payload']['checkpoint']['verified_lemmas'], ['L1'])
        failure = json.loads(pending[0]['payload']['latest_failure']['payload'])
        self.assertIn('full proof', failure['report']['missing'])
        self.assertEqual(self.b.goal(self.b.run(run)), original)

    def test_repeated_no_progress_is_immediate_replanning_never_completion(self):
        run = self.start()
        original = copy.deepcopy(self.b.goal(self.b.run(run)))
        for attempt in range(1, 8):
            claim = self.claim(run, tick=attempt == 1)
            result = self.b.fail(*claim, 'no_progress', f'Simulation attempt {attempt}: lemma insufficient',
                                 checkpoint={'attempt': attempt, 'rejected_routes': list(range(attempt))})
            self.assertEqual(result['state'], 'REPLANNING')
            self.assertEqual(result['retry_after'], 0)
            self.assertEqual(len(result['next_requests']), 1)
            pending = self.b.pending(run_id=run)
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0]['payload']['continuation']['action'], 'replan')
            self.assertEqual(pending[0]['payload']['continuation']['failures'], attempt)
            self.assertFalse(pending[0]['payload']['continuation']['no_progress_is_terminal'])
            self.assertEqual(pending[0]['payload']['checkpoint']['attempt'], attempt)
            self.assertEqual(self.b.prerequisites(run), [])
            self.assertEqual(self.b.goal(self.b.run(run)), original)

    def test_mathematical_difficulty_cannot_be_declared_prerequisite(self):
        run = self.start()
        claim = self.claim(run)
        before = self.snapshot(run)
        for kind in ('no_progress', 'verification', 'format', 'program'):
            with self.subTest(kind=kind), self.assertRaises(TangoError):
                self.b.fail(*claim, kind, 'Proof remains difficult', blocked=True,
                            blocker={'category': 'resource', 'method': 'simulation',
                                     'result': 'no proof', 'needed': 'a better idea'})
        self.assertEqual(self.snapshot(run), before)

    def test_resource_block_requires_structured_evidence(self):
        run = self.start()
        claim = self.claim(run)
        for blocker in (None, {}, {'category': 'math', 'method': 'checked', 'result': 'hard', 'needed': 'proof'},
                        {'category': 'resource', 'method': 'simulation', 'result': 'not available'}):
            with self.subTest(blocker=blocker), self.assertRaises(TangoError):
                self.b.fail(*claim, 'environment', 'Simulation resource unavailable', blocked=True, blocker=blocker)
        blocker = {'category': 'resource', 'method': 'simulation resource inspection',
                   'result': 'required executable absent', 'needed': 'install required executable'}
        result = self.b.fail(*claim, 'environment', 'Simulation required executable absent',
                             blocked=True, blocker=blocker)
        self.assertEqual(result['state'], 'WAITING')
        self.assertEqual(result['next_requests'], [])
        self.assertEqual(self.b.pending(run_id=run), [])
        self.assertEqual(self.b.parent_actions(run_id=run)[0]['action'], 'await_prerequisite')
        self.assertEqual(self.b.prerequisites(run)[0]['original_evidence']['blocker'], blocker)
        self.assertEqual(self.b.tick(run_id=run)['queued'], [])

    def test_permission_materials_quota_remain_real_blockers(self):
        for slot, kind in enumerate(('permission', 'materials', 'quota'), 1):
            run = self.start(slot)
            result = self.b.fail(*self.claim(run), kind, 'Simulation missing prerequisite')
            self.assertEqual(result['state'], 'WAITING')
            self.assertEqual(result['next_requests'], [])
            self.assertEqual(self.b.prerequisites(run)[0]['kind'], kind)
        self.assertEqual(self.b.tick()['queued'], [])
        self.assertEqual(self.b.pending(), [])

    def test_candidate_atomic_successor_failure_rolls_back_database(self):
        run = self.start()
        claim = self.claim(run)
        path = self.write_candidate(run)
        before = self.snapshot(run)
        with mock.patch.object(self.b, '_queue_ready', side_effect=RuntimeError('injected queue failure')):
            with self.assertRaisesRegex(RuntimeError, 'injected'):
                self.b.candidate(*claim, [str(path)])
        self.assertEqual(self.snapshot(run), before)
        retry = self.b.candidate(*claim, [str(path)])
        self.assertEqual(retry['artifact_version'], 1)
        self.assertEqual(len(self.b.pending(run_id=run)), 1)

    def test_verify_atomic_successor_failure_rolls_back_verdict(self):
        run, verifier = self.prepare_verifier()
        before = self.snapshot(run)
        with mock.patch.object(self.b, '_queue_ready', side_effect=RuntimeError('injected queue failure')):
            with self.assertRaises(RuntimeError):
                self.b.verify(*verifier, self.report(run, False))
        self.assertEqual(self.snapshot(run), before)
        self.assertEqual(len(self.b.verify(*verifier, self.report(run, False))['next_requests']), 1)

    def test_no_progress_atomic_successor_failure_rolls_back_failure(self):
        run = self.start()
        claim = self.claim(run)
        before = self.snapshot(run)
        with mock.patch.object(self.b, '_queue_ready', side_effect=RuntimeError('injected queue failure')):
            with self.assertRaises(RuntimeError):
                self.b.fail(*claim, 'no_progress', 'Simulation no proof')
        self.assertEqual(self.snapshot(run), before)
        self.assertEqual(len(self.b.fail(*claim, 'no_progress', 'Simulation no proof')['next_requests']), 1)

    def test_restart_preserves_successor_ids_without_duplicate_dispatch(self):
        run = self.start()
        candidate = self.candidate(run, self.claim(run))
        expected = candidate['next_requests']
        self.b.db.close()
        self.b = Broker(self.temp.name, simulation=True)
        for _ in range(5):
            self.assertEqual(self.b.tick(run_id=run)['queued'], [])
            self.assertEqual([q['id'] for q in self.b.pending(run_id=run)], expected)
        self.assertEqual(self.b.parent_actions(run_id=run)[0]['request_id'], expected[0])
        self.assertEqual(len(self.rows('artifacts', run)), 1)

    def test_concurrent_ticks_and_claims_never_duplicate(self):
        run = self.start()
        ticks = self.race([lambda b: b.tick(run_id=run) for _ in range(6)])
        self.assertTrue(all(status == 'ok' for status, _ in ticks))
        self.assertEqual(sum(len(value['queued']) for _, value in ticks), 1)
        pending = self.b.pending(run_id=run)
        self.assertEqual(len(pending), 1)
        qid = pending[0]['id']
        claims = self.race([lambda b, i=i: b.claim(qid, f'simulation-race-{i}') for i in range(6)])
        self.assertEqual(sum(status == 'ok' for status, _ in claims), 1)
        self.assertEqual(self.b.db.execute("SELECT count(*) FROM requests WHERE state='claimed'").fetchone()[0], 1)

    def test_duplicate_candidate_race_publishes_one_version_and_successor(self):
        run = self.start()
        claim = self.claim(run)
        path = str(self.write_candidate(run))
        outcomes = self.race([lambda b: b.candidate(*claim, [path]) for _ in range(4)])
        self.assertEqual(sum(status == 'ok' for status, _ in outcomes), 1)
        self.assertEqual(self.b.run(run)['artifact_version'], 1)
        self.assertEqual(len(self.rows('artifacts', run)), 1)
        self.assertEqual(len(self.b.pending(run_id=run)), 1)

    def control_candidate_race(self, command):
        run = self.start()
        claim = self.claim(run)
        path = str(self.write_candidate(run))
        outcomes = self.race([
            lambda b: b.candidate(*claim, [path]),
            lambda b: b.route('simulation-race-control', f'\\TANGO_TASK_1 {command}', 'test_injection'),
        ])
        self.assertEqual(outcomes[1][0], 'ok')
        self.assertEqual(self.b.run(run)['state'], 'PAUSED' if command == 'pause' else 'STOPPED')
        self.assertEqual(self.b.pending(run_id=run), [])
        self.assertEqual(self.b.tick(run_id=run)['queued'], [])
        self.assertEqual(self.b.db.execute("SELECT count(*) FROM requests WHERE state IN ('queued','claimed')").fetchone()[0], 0)
        with self.assertRaises(TangoError):
            self.b.candidate(*claim, [path])

    def test_pause_race_fences_candidate_and_cancels_any_successor(self):
        self.control_candidate_race('pause')

    def test_stop_race_fences_candidate_and_cancels_any_successor(self):
        self.control_candidate_race('stop')

    def test_pause_after_successor_commit_cancels_it(self):
        run = self.start()
        result = self.candidate(run, self.claim(run))
        successor = result['next_requests'][0]
        self.route(1, 'pause')
        self.assertEqual(self.b.db.execute('SELECT state FROM requests WHERE id=?', (successor,)).fetchone()[0], 'cancelled')
        with self.assertRaises(TangoError):
            self.b.claim(successor, 'simulation-late-verifier')
        self.assertEqual(self.b.pending(run_id=run), [])

    def test_two_tasks_remain_independent_and_older_work_is_not_starved(self):
        one, two = self.start(1), self.start(2)
        self.b.tick()
        second_pending = self.b.pending(run_id=two)[0]
        second_snapshot = self.snapshot(two)
        for attempt in range(5):
            self.b.fail(*self.claim(one, tick=False), 'no_progress', f'Simulation incomplete {attempt}')
            self.assertEqual(self.b.pending()[0]['id'], second_pending['id'])
            self.assertEqual(self.snapshot(two), second_snapshot)
        first = self.claim(one, tick=False)
        second = self.claim(two, tick=False)
        self.assertNotEqual(first[0], second[0])
        self.assertEqual(self.b.db.execute("SELECT count(*) FROM requests WHERE state='claimed'").fetchone()[0], 2)
        self.candidate(two, second)
        self.assertEqual(self.b.run(one)['state'], 'RUNNING')
        self.assertEqual(self.b.run(two)['state'], 'VERIFYING')

    def test_scoped_tick_does_not_queue_or_recover_other_slot(self):
        one, two = self.start(1), self.start(2)
        second = self.claim(two)
        self.b.db.execute('UPDATE runs SET lease_until=0 WHERE run_id=?', (two,))
        before = self.snapshot(two)
        self.b.tick(slot=1, run_id=one)
        self.assertEqual(self.snapshot(two), before)
        self.assertEqual(len(self.b.pending(slot=1)), 1)
        self.assertEqual(self.b.cancellations(slot=1), [])
        self.assertEqual(self.b.recover(run_id=two), [two])
        self.assertEqual(self.b.cancellations(run_id=two)[0]['request_id'], second[0])

    def test_scope_rejects_invalid_slot_and_mismatched_run_without_changes(self):
        one, two = self.start(1), self.start(2)
        before = self.snapshot(one), self.snapshot(two)
        for method in ('tick', 'recover', 'pending', 'outbox', 'cancellations', 'delivery_issues', 'parent_actions'):
            for scope in ({'slot': 1, 'run_id': two}, {'slot': 4}, {'slot': True}, {'slot': '1'}):
                with self.subTest(method=method, scope=scope), self.assertRaises(TangoError):
                    getattr(self.b, method)(**scope)
        self.assertEqual((self.snapshot(one), self.snapshot(two)), before)

    def test_scoped_outbox_preserves_historical_other_slot_and_nonselected_ids(self):
        historical = self.start(1)
        historical_ids = [o['id'] for o in self.b.outbox(run_id=historical)]
        self.route(1, 'stop')
        current = self.start(1)
        other = self.start(2)
        old = self.b.outbox('simulation-old-delivery', run_id=historical)[0]
        other_message = self.b.outbox('simulation-other-delivery', run_id=other)[0]
        self.b.db.execute('UPDATE outbox SET claim_until=0 WHERE id IN (?,?)', (old['id'], other_message['id']))
        historical_before, other_before = self.rows('outbox', historical), self.rows('outbox', other)
        messages = self.b.outbox(slot=1, run_id=current, current_only=True)
        self.assertTrue(messages)
        selected = messages[0]['id']
        claims = self.b.outbox('simulation-scoped', slot=1, current_only=True, ids=[selected, other_message['id']] + historical_ids)
        self.assertEqual([o['id'] for o in claims], [selected])
        self.assertFalse(claims[0]['stale_run'])
        self.assertEqual(self.rows('outbox', historical), historical_before)
        self.assertEqual(self.rows('outbox', other), other_before)
        self.assertEqual(self.b.outbox('simulation-empty', ids=[]), [])

    def test_scoped_delivery_issues_and_cancellations_stay_separate(self):
        runs = [self.start(slot) for slot in (1, 2)]
        for slot, run in enumerate(runs, 1):
            self.claim(run)
            self.route(slot, 'pause')
            out = self.b.outbox(f'simulation-delivery-{slot}', run_id=run)[0]
            self.b.ack(out['id'], f'simulation-delivery-{slot}', error='Simulation ambiguous delivery')
        for slot, run in enumerate(runs, 1):
            self.assertEqual({x['run_id'] for x in self.b.cancellations(slot=slot)}, {run})
            self.assertEqual({x['run_id'] for x in self.b.delivery_issues(run_id=run)}, {run})
        self.assertEqual(len(self.b.cancellations()), 2)
        self.assertEqual(len(self.b.delivery_issues()), 2)

    def test_parent_presence_expires_truthfully_and_epoch_fences_release(self):
        run = self.start()
        action = self.b.parent_actions(run_id=run)[0]
        self.assertTrue(action['action_needed'])
        self.assertFalse(action['native_owner_present'])
        self.assertFalse(action['local_can_wake_native'])
        first = self.b.parent_heartbeat(run, 'simulation-parent-one', 60)
        again = self.b.parent_heartbeat(run, 'simulation-parent-one', 60)
        self.assertEqual(first['epoch'], again['epoch'])
        with self.assertRaises(TangoError):
            self.b.parent_heartbeat(run, 'simulation-parent-two', 60)
        action = self.b.parent_actions(run_id=run)[0]
        self.assertTrue(action['native_owner_present'])
        self.assertFalse(action['action_needed'])
        self.b.db.execute('UPDATE parent_presence SET lease_until=0 WHERE run_id=?', (run,))
        self.assertTrue(self.b.parent_actions(run_id=run)[0]['action_needed'])
        replacement = self.b.parent_heartbeat(run, 'simulation-parent-two', 60)
        self.assertGreater(replacement['epoch'], first['epoch'])
        with self.assertRaises(TangoError):
            self.b.parent_release(run, 'simulation-parent-one', first['epoch'])
        self.b.parent_release(run, 'simulation-parent-two', replacement['epoch'])
        self.assertFalse(self.b.parent_actions(run_id=run)[0]['native_owner_present'])

    def test_execution_heartbeat_refreshes_parent_action_due_without_checkpoint(self):
        run = self.start()
        claim = self.claim(run)
        before = self.b.parent_actions(run_id=run)[0]['due']
        self.b.heartbeat(*claim, lease_seconds=900)
        action = self.b.parent_actions(run_id=run)[0]
        self.assertEqual(action['action'], 'monitor_native')
        self.assertEqual(action['due'], self.b.run(run)['lease_until'])
        self.assertGreater(action['due'], before)

    def test_parent_actions_recover_reconcile_then_dispatch_without_cross_slot(self):
        one, two = self.start(1), self.start(2)
        claim = self.claim(one)
        second_before = self.snapshot(two)
        self.b.db.execute('UPDATE runs SET lease_until=0 WHERE run_id=?', (one,))
        self.b.tick(run_id=one)
        self.assertEqual(self.b.parent_actions(run_id=one)[0]['action'], 'reconcile_native')
        self.assertEqual(self.snapshot(two), second_before)
        pending = self.b.pending(run_id=one)[0]
        with self.assertRaises(TangoError):
            self.b.claim(pending['id'], 'simulation-replacement')
        self.b.ack_cancel(claim[0], {'kind': 'simulation', 'agent_id': 'simulation-session-' + claim[0]})
        self.assertEqual(self.b.parent_actions(run_id=one)[0]['action'], 'dispatch_execute')
        self.assertEqual(self.snapshot(two), second_before)
        self.assertEqual(self.b.claim(pending['id'], 'simulation-replacement')['request_id'], pending['id'])

    def test_contrary_requires_goal_decision_never_completes_or_queues(self):
        run, verifier = self.prepare_verifier()
        original = copy.deepcopy(self.b.goal(self.b.run(run)))
        result = self.b.verify(*verifier, self.contrary_report(run))
        self.assertFalse(result['passed'])
        self.assertTrue(result['report']['contrary_verified'])
        self.assertFalse(result['report']['accepted'])
        self.assertEqual(result['report']['original_goal_status'], 'not_satisfied')
        self.assertEqual(result['state'], 'WAITING')
        self.assertEqual(result['next_requests'], [])
        self.assertEqual(self.b.run(run)['wait_reason'], 'goal_decision')
        self.assertEqual(self.b.goal(self.b.run(run)), original)
        prerequisite = self.b.prerequisites(run)[0]
        self.assertEqual(prerequisite['kind'], 'goal_decision')
        self.assertEqual(prerequisite['original_evidence']['verification_id'], result['verification_id'])
        action = self.b.parent_actions(run_id=run)[0]
        self.assertEqual(action['action'], 'await_goal_decision')
        self.assertEqual(action['supervision_status'], 'blocked')
        self.assertEqual(self.b.tick(run_id=run)['queued'], [])
        self.assertEqual(self.b.pending(run_id=run), [])
        self.b.db.close()
        self.b = Broker(self.temp.name, simulation=True)
        self.assertEqual(self.b.tick(run_id=run)['queued'], [])
        self.assertEqual(self.b.parent_actions(run_id=run)[0]['action'], 'await_goal_decision')

    def test_invalid_contrary_is_rejected_without_consuming_verifier(self):
        run, verifier = self.prepare_verifier()
        before = self.snapshot(run)
        invalid = []
        report = self.contrary_report(run); report['original_goal_status'] = 'satisfied'; invalid.append(report)
        report = self.contrary_report(run); report['checks'].pop(); invalid.append(report)
        report = self.contrary_report(run); report['checks'].append(copy.deepcopy(report['checks'][0])); invalid.append(report)
        report = self.contrary_report(run); report['checks'][0]['evidence'] = ''; invalid.append(report)
        report = self.contrary_report(run); report['contrary']['refutes_criterion_ids'] = ['valid']; invalid.append(report)
        report = self.contrary_report(run); report['contrary']['refutes_criterion_ids'] = []; invalid.append(report)
        report = self.contrary_report(run); report['contrary']['evidence'] = ''; invalid.append(report)
        report = self.contrary_report(run); report['artifact_version'] += 1; invalid.append(report)
        report = self.contrary_report(run); report['missing'] = ['Unproved lemma required for the counterexample']; invalid.append(report)
        for index, report in enumerate(invalid):
            with self.subTest(index=index), self.assertRaises(TangoError):
                self.b.verify(*verifier, report)
            self.assertEqual(self.snapshot(run), before)
        self.assertEqual(self.b.verify(*verifier, self.contrary_report(run))['state'], 'WAITING')

    def test_tampered_artifact_cannot_support_contrary_outcome(self):
        run, verifier = self.prepare_verifier()
        manifest = json.loads(self.rows('artifacts', run)[0]['manifest'])
        Path(manifest[0]['path']).write_text('Simulation tampering')
        with self.assertRaises(TangoError):
            self.b.verify(*verifier, self.contrary_report(run))
        self.assertEqual(self.b.run(run)['state'], 'VERIFYING')
        self.assertEqual(self.b.prerequisites(run), [])

    def test_explicitly_unsatisfied_original_goal_cannot_be_completed(self):
        run, verifier = self.prepare_verifier()
        report = self.report(run, True)
        report['original_goal_status'] = 'not_satisfied'
        try:
            result = self.b.verify(*verifier, report)
        except TangoError:
            self.assertNotEqual(self.b.run(run)['state'], 'COMPLETED')
            return
        self.assertFalse(result['passed'])
        self.assertNotEqual(result['state'], 'COMPLETED')
        self.assertNotEqual(self.b.run(run)['state'], 'COMPLETED')

    def test_all_original_criteria_pass_ends_without_successor(self):
        run, verifier = self.prepare_verifier()
        result = self.b.verify(*verifier, self.report(run, True))
        self.assertTrue(result['passed'])
        self.assertEqual(result['state'], 'COMPLETED')
        self.assertEqual(result['next_requests'], [])
        self.assertEqual(self.b.pending(run_id=run), [])
        self.assertEqual(self.b.parent_actions(run_id=run)[0]['action'], 'none')
        self.assertEqual(self.b.tick(run_id=run)['queued'], [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
