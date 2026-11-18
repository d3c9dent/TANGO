"""Synthetic local journal/receipt fixtures; these are not real model acceptance tests."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
import uuid

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'program'))
from tango import Broker, TangoError


class CliReceiptTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='tango-cli-receipt-test-')
        self.b=Broker(self.tmp.name)
        self.r=self.b.route('start','\\TANGO_TASK_1 start: Preserve the original result','test_injection')['run_id']
        self.b.set_contract(self.r,1,[{'id':'correct','description':'Exact original result'}])

    def tearDown(self):
        self.b.db.close();self.tmp.cleanup()

    def claim(self,kind='execute'):
        self.b.tick(run_id=self.r)
        q=self.b.pending(run_id=self.r)[0]
        self.assertEqual(q['kind'],kind)
        c=self.b.claim(q['id'],'parent')
        return q['id'],'parent',c['fence']

    def attempt(self,c,session='SIMULATION-thread',completed=False,member=None,tool='codex.exec'):
        attempt_id='SIMULATION-'+uuid.uuid4().hex
        p=Path(self.tmp.name)/'attempts'/c[0]/attempt_id/'attempt.json'
        self.b.register_cli_attempt(*c,str(p),member=member)
        p.parent.mkdir(parents=True)
        identity={'pid':12345,'start':'SIMULATION-start','platform':'linux','process_group':12345}
        data={'schema_version':1,'attempt_id':attempt_id,'state':'running','process_started':True,
              'process_identity':identity,'session':session,'thread_observed':True,'process_exited':False,
              'exit_code':None,'turn_completed':False}
        p.write_text(json.dumps(data))
        receipt={'kind':'codex_cli','tool':tool,'agent_id':session,
                 'result':{'thread_id':session,'attempt_id':attempt_id,'event':'thread.started',
                           'process_identity':identity,'journal_path':str(p)}}
        if completed:receipt=self.finish(p,receipt)
        return p,receipt

    def finish(self,p,receipt,state='completed',exit_code=0):
        data=json.loads(p.read_text())
        data.update(state=state,process_exited=True,exit_code=exit_code,turn_completed=state=='completed')
        p.write_text(json.dumps(data))
        result=copy.deepcopy(receipt)
        result['result'].update(process_exited=True,exit_code=exit_code,turn_completed=state=='completed')
        return result

    def terminal(self,receipt,status='completed'):
        result=copy.deepcopy(receipt)
        result.update(tool='codex.process',status=status)
        return result

    def bind(self,c,receipt):
        return self.b.bind_native(*c,receipt['agent_id'],receipt)

    def artifact(self):
        p=self.b.path(self.r)/'work'/'answer.txt';p.write_text('SIMULATION exact result')
        return p

    def unbound_receipt(self,c,p,status):
        data=json.loads(p.read_text())
        return {'kind':'codex_cli','tool':'codex.process','agent_id':'unbound:'+c[0],
                'request_id':c[0],'status':status,'result':{
                    'journal_path':str(p),'journal_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),
                    'attempt_id':data['attempt_id'],'state':data['state'],
                    'process_started':data['process_started'],'process_exited':data['process_exited']}}

    def test_candidate_requires_completed_cli_turn_then_independent_verifier(self):
        c=self.claim();p,started=self.attempt(c);self.bind(c,started)
        with self.assertRaises(TangoError):self.b.candidate(*c,[str(self.artifact())])
        completed=self.finish(p,started);self.bind(c,completed)
        self.b.candidate(*c,[str(self.artifact())])
        v=self.claim('verify');vp,reused=self.attempt(v,session=started['agent_id'],completed=True)
        with self.assertRaisesRegex(TangoError,'separate'):self.bind(v,reused)
        # A separate journal can be registered only after the first one is terminal.
        vp,verified=self.attempt(v,session='SIMULATION-independent-verifier',completed=True)
        self.bind(v,verified)
        r=self.b.run(self.r)
        report={key:r[key] for key in ('goal_version','input_version','artifact_version')}
        report.update(checks=[{'id':'correct','passed':True,'evidence':'SIMULATION fixture equality'}],missing=[])
        self.assertTrue(self.b.verify(*v,report)['passed'])
        self.assertEqual(self.b.run(self.r)['state'],'COMPLETED')

    def test_registration_cannot_replace_a_running_attempt(self):
        c=self.claim();p,started=self.attempt(c);self.bind(c,started)
        new=p.parent.parent/'replacement'/'attempt.json'
        with self.assertRaisesRegex(TangoError,'termination'):self.b.register_cli_attempt(*c,str(new))
        failed=self.finish(p,started,state='failed',exit_code=1)
        self.b.register_cli_attempt(*c,str(new))
        self.assertEqual(self.b.db.execute('SELECT native_session FROM requests WHERE id=?',(c[0],)).fetchone()[0],started['agent_id'])
        with self.assertRaises(TangoError):self.b.candidate(*c,[str(self.artifact())])

    def test_wrong_journal_identity_and_unobserved_resume_are_rejected(self):
        c=self.claim();p,receipt=self.attempt(c)
        wrong=copy.deepcopy(receipt);wrong['result']['thread_id']='other'
        with self.assertRaises(TangoError):self.bind(c,wrong)
        wrong=copy.deepcopy(receipt);wrong['result']['attempt_id']='other'
        with self.assertRaises(TangoError):self.bind(c,wrong)
        data=json.loads(p.read_text());data['thread_observed']=False;p.write_text(json.dumps(data))
        with self.assertRaisesRegex(TangoError,'observed'):self.bind(c,receipt)

    def test_fail_does_not_release_a_running_process(self):
        c=self.claim();p,receipt=self.attempt(c);self.bind(c,receipt)
        with self.assertRaises(TangoError):self.b.fail(*c,'program','SIMULATION worker failure')
        self.finish(p,receipt,state='failed',exit_code=1)
        self.assertEqual(self.b.fail(*c,'program','SIMULATION worker exited')['state'],'REPAIRING')

    def test_cancel_requires_actual_terminal_registered_process(self):
        c=self.claim();p,receipt=self.attempt(c);self.bind(c,receipt)
        self.b.route('pause','\\TANGO_TASK_1 pause','test_injection')
        with self.assertRaises(TangoError):self.b.ack_cancel(c[0],self.terminal(receipt))
        terminal=self.terminal(self.finish(p,receipt,state='interrupted',exit_code=-15),'interrupted')
        self.b.ack_cancel(c[0],terminal)
        self.assertEqual(self.b.cancellations(run_id=self.r),[])
        self.assertEqual(self.b.run(self.r)['state'],'PAUSED')

    def test_definite_spawn_failure_can_close_unbound_cancellation(self):
        c=self.claim();p,receipt=self.attempt(c)
        data=json.loads(p.read_text());data.update(state='spawn_failed',process_started=False,
            process_identity=None,session=None,thread_observed=False,process_exited=True,
            exit_code=None,turn_completed=False,error='SIMULATION Popen FileNotFoundError')
        p.write_text(json.dumps(data))
        self.b.interrupt_cli_request(*c,'tool','SIMULATION executable missing')
        evidence=self.unbound_receipt(c,p,'not_started')
        changed=copy.deepcopy(evidence);changed['result']['journal_sha256']='0'*64
        with self.assertRaises(TangoError):self.b.ack_cli_not_started(c[0],changed)
        self.b.ack_cli_not_started(c[0],evidence)
        self.assertEqual(self.b.cancellations(run_id=self.r),[])

    def test_unbound_started_process_is_not_mislabeled_not_started(self):
        c=self.claim();p,receipt=self.attempt(c)
        self.finish(p,receipt,state='failed',exit_code=2)
        data=json.loads(p.read_text());data.update(session=None,thread_observed=False);p.write_text(json.dumps(data))
        self.b.interrupt_cli_request(*c,'format','SIMULATION process exited before a thread event')
        with self.assertRaises(TangoError):self.b.ack_cli_not_started(c[0],self.unbound_receipt(c,p,'not_started'))
        self.b.ack_cli_unbound(c[0],self.unbound_receipt(c,p,'unbound_terminal'))
        self.assertEqual(self.b.cancellations(run_id=self.r),[])

    def test_failed_resume_spawn_retains_known_session_without_claiming_new_execution(self):
        c=self.claim();p,receipt=self.attempt(c,completed=True);self.bind(c,receipt)
        p,attempt=self.attempt(c,session=receipt['agent_id'],tool='codex.exec.resume')
        data=json.loads(p.read_text());data.update(state='spawn_failed',process_started=False,
            process_identity=None,requested_session=receipt['agent_id'],thread_observed=False,
            process_exited=True,exit_code=None,turn_completed=False,error='SIMULATION resume Popen failure')
        p.write_text(json.dumps(data))
        self.b.interrupt_cli_request(*c,'dependency','SIMULATION executable unavailable for resume')
        evidence=self.unbound_receipt(c,p,'not_started');evidence['agent_id']=receipt['agent_id']
        self.b.ack_cli_not_started(c[0],evidence)
        self.assertEqual(self.b.cancellations(run_id=self.r),[])

    def test_ambiguous_spawn_gap_stays_blocked(self):
        c=self.claim();p,receipt=self.attempt(c)
        data=json.loads(p.read_text());data.update(state='spawning',session=None,process_identity=None,
            process_started=False,thread_observed=False,process_exited=False);p.write_text(json.dumps(data))
        self.b.interrupt_cli_request(*c,'tool','SIMULATION interrupted spawn gap')
        with self.assertRaises(TangoError):self.b.ack_cli_not_started(c[0],self.unbound_receipt(c,p,'not_started'))
        self.assertEqual(len(self.b.cancellations(run_id=self.r)),1)

    def test_orphan_thread_can_be_identified_after_process_termination(self):
        c=self.claim();p,receipt=self.attempt(c,completed=True)
        self.b.interrupt_cli_request(*c,'service','SIMULATION crash before binding')
        terminal=self.terminal(receipt)
        self.b.reconcile_spawn(c[0],receipt['agent_id'],terminal)
        self.b.ack_cancel(c[0],terminal)
        self.assertEqual(self.b.cancellations(run_id=self.r),[])

    def test_cli_interruption_preserves_blockers_and_original_goal(self):
        c=self.claim();p,receipt=self.attempt(c);self.bind(c,receipt)
        before=self.b.goal(self.b.run(self.r))['body']
        result=self.b.interrupt_cli_request(*c,'quota','SIMULATION quota exhausted')
        self.assertEqual(result['state'],'WAITING')
        self.assertIsNone(result['retry_after'])
        self.assertEqual(self.b.goal(self.b.run(self.r))['body'],before)
        self.assertEqual(self.b.prerequisites(self.r)[0]['kind'],'quota')
        self.assertEqual(len(result['cancellations']),1)
        with self.assertRaises(TangoError):self.b.heartbeat(*c)

    def test_contrary_then_unrelated_input_and_failed_recheck_returns_to_waiting(self):
        c=self.claim();p,producer=self.attempt(c,completed=True);self.bind(c,producer)
        self.b.candidate(*c,[str(self.artifact())])
        v=self.claim('verify');p,verifier=self.attempt(v,session='SIMULATION-verifier',completed=True)
        self.bind(v,verifier)
        run=self.b.run(self.r)
        report={key:run[key] for key in ('goal_version','input_version','artifact_version')}
        report.update(original_goal_status='not_satisfied',disposition='VERIFIED_CONTRARY_REQUIRES_GOAL_DECISION',
            checks=[{'id':'correct','passed':False,'evidence':'SIMULATION independently checked contrary result'}],
            missing=[],contrary={'summary':'SIMULATION criterion refuted','evidence':'SIMULATION counterexample',
                                'refutes_criterion_ids':['correct']})
        self.assertEqual(self.b.verify(*v,report)['state'],'WAITING')
        original_goal=self.b.goal(self.b.run(self.r))['body']
        original_evidence=copy.deepcopy(self.b.prerequisites(self.r)[0]['original_evidence'])
        count=self.b._calculation_count(self.r)
        self.b.route('unrelated','\\TANGO_TASK_1 Please retain readable formatting','test_injection')
        recheck=self.claim();p,started=self.attempt(recheck,session='SIMULATION-prerequisite-check')
        self.bind(recheck,started)
        proof={'method':'SIMULATION read new input','result':'No actual goal decision was supplied'}
        with self.assertRaises(TangoError):self.b.recheck_prerequisite(*recheck,'goal_decision',False,proof)
        completed=self.finish(p,started);self.bind(recheck,completed)
        with self.assertRaises(TangoError):self.b.wait_for_prerequisite(*recheck,'goal_decision','No goal decision supplied')
        self.b.recheck_prerequisite(*recheck,'goal_decision',False,proof)
        result=self.b.wait_for_prerequisite(*recheck,'goal_decision','No goal decision supplied')
        self.assertEqual(result['wait_reason'],'goal_decision')
        self.assertEqual(self.b.run(self.r)['state'],'WAITING')
        self.assertIsNone(self.b.run(self.r)['owner'])
        self.assertEqual(self.b.goal(self.b.run(self.r))['body'],original_goal)
        self.assertEqual(self.b.prerequisites(self.r)[0]['original_evidence'],original_evidence)
        self.assertEqual(self.b._calculation_count(self.r),count)
        self.assertEqual(self.b.tick(run_id=self.r)['queued'],[])
        self.assertEqual(self.b.pending(run_id=self.r),[])
        self.b.db.close();self.b=Broker(self.tmp.name)
        self.assertEqual(self.b.tick(run_id=self.r)['queued'],[])
        self.assertEqual(self.b.parent_actions(run_id=self.r)[0]['action'],'await_goal_decision')

    def test_native_request_cannot_use_cli_interruption(self):
        c=self.claim()
        self.b.bind_native(*c,'SIMULATION-native',{'kind':'native','tool':'collaboration.spawn_agent','agent_id':'SIMULATION-native','result':{'simulation':'test fixture'}})
        with self.assertRaises(TangoError):self.b.interrupt_cli_request(*c,'tool','SIMULATION wrong adapter')

    def test_journals_outside_home_and_symlinks_are_rejected(self):
        c=self.claim()
        with tempfile.TemporaryDirectory() as outside:
            with self.assertRaises(TangoError):self.b.register_cli_attempt(*c,str(Path(outside)/'attempt.json'))
        p,receipt=self.attempt(c)
        copy_path=p.with_name('copy.json');p.rename(copy_path);p.symlink_to(copy_path)
        with self.assertRaisesRegex(TangoError,'symlink'):self.bind(c,receipt)

    def test_atomic_claim_marker_recovers_before_first_registration(self):
        self.b.tick(run_id=self.r);q=self.b.pending(run_id=self.r)[0]
        claim=self.b.claim(q['id'],'autonomous:old',adapter='codex_cli')
        c=(q['id'],'autonomous:old',claim['fence'])
        self.b.interrupt_cli_request(*c,'tool','SIMULATION supervisor ended before registration')
        result=self.b.ack_cli_unlaunched(q['id'])
        self.assertFalse(result['process_was_launched'])
        self.assertEqual(self.b.cancellations(run_id=self.r),[])

    def test_expired_atomic_claim_marker_recovers_without_inventing_worker(self):
        self.b.tick(run_id=self.r);q=self.b.pending(run_id=self.r)[0]
        self.b.claim(q['id'],'autonomous:old',adapter='codex_cli')
        # Simulate expiry without waiting for wall time in this unit fixture.
        self.b.db.execute('UPDATE runs SET lease_until=0 WHERE run_id=?',(self.r,))
        self.b.recover(run_id=self.r)
        self.b.ack_cli_unlaunched(q['id'])
        self.assertEqual(self.b.cancellations(run_id=self.r),[])
        self.assertEqual(self.b.run(self.r)['state'],'REPAIRING')

    def test_registered_attempt_cannot_use_unlaunched_marker_recovery(self):
        self.b.tick(run_id=self.r);q=self.b.pending(run_id=self.r)[0]
        claim=self.b.claim(q['id'],'autonomous:old',adapter='codex_cli')
        c=(q['id'],'autonomous:old',claim['fence'])
        p,_=self.attempt(c)
        self.b.interrupt_cli_request(*c,'tool','SIMULATION interrupted real launch')
        with self.assertRaises(TangoError):self.b.ack_cli_unlaunched(q['id'])
        self.assertEqual(len(self.b.cancellations(run_id=self.r)),1)

    def prepare_meeting(self):
        # Existing protocol fixture advances to the meeting gate. It is synthetic.
        self.b.simulation=True
        for i in range(10):
            c=self.claim();session='SIMULATION-computation-'+str(i)
            self.b.bind_native(*c,session,{'kind':'simulation','agent_id':session})
            self.b.fail(*c,'no_progress','SIMULATION substantive attempt retained')
        c=self.claim('prepare_meeting');session='SIMULATION-preparation'
        receipt={'kind':'simulation','agent_id':session}
        self.b.bind_native(*c,session,receipt)
        source=Path(self.tmp.name)/'research.txt';source.write_text('SIMULATION open result')
        cat=self.b.prepare_catalogue(*c,{'roots':[str(source)],'excluded':[]})['catalogue']
        summary={key:[] for key in ('accepted_results','failed_routes','conditional_assumptions','open_obligations','unverified_findings')}
        summary['open_obligations']=[{'id':'O1','statement':'SIMULATION original result remains open',
            'scope':'SIMULATION fixture','verification_status':'open','sources':cat['sources']}]
        coverage={'claim':'scoped_complete','search_method':'SIMULATION fixture enumeration',
                  'unavailable':[],'excluded':[],'caveats':[],'source_dispositions':[]}
        self.b.seal_meeting_summary(*c,cat,summary,coverage,receipt)
        self.b.simulation=False
        c=self.claim('meeting')
        p,receipt=self.attempt(c,session='SIMULATION-coordinator',completed=True)
        self.bind(c,receipt)
        return c,receipt

    def post_content(self,n,i):
        return {'analysis':f'SIMULATION round {n}, member {i}', 'evidence':'SIMULATION source',
                'objections':'SIMULATION objection','open_obligations':['Original result'],
                'next_step':'SIMULATION next calculation'}

    def test_cli_meeting_preserves_five_by_ten_barriers_and_resumed_identity(self):
        c,opening=self.prepare_meeting()
        last={}
        for n in range(1,11):
            context=self.b.meeting_context(*c)
            self.assertEqual(context['round'],n)
            self.assertEqual(len(context['previous_rounds']),5*(n-1))
            for i in range(1,6):
                self.b.meeting_reserve(*c,i)
                p,receipt=self.attempt(c,session=f'SIMULATION-participant-{i}',completed=True,
                                       member=i,tool='codex.exec' if n==1 else 'codex.exec.resume')
                self.b.meeting_bind(*c,i,receipt['agent_id'],receipt)
                last[str(i)]=self.terminal(receipt)
            for i in range(1,6):
                saved=self.b._cli_saved_receipt(c[0]+':member:'+str(i))
                receipt={**saved,'round':n,'context_hash':context['context_hash']}
                result=self.b.meeting_submit(*c,i,n,context['context_hash'],self.post_content(n,i),receipt)
                self.assertEqual(result['round_complete'],i==5)
        # The actual coordinator session resumes only after the participant barrier is complete.
        p,final=self.attempt(c,session=opening['agent_id'],completed=True,tool='codex.exec.resume')
        self.bind(c,final)
        decision={'direction':'SIMULATION next route','rationale':'SIMULATION reviewed evidence',
                  'next_steps':['Next attempt'],'expected_artifacts':['Result'],'open_obligations':['Original goal'],
                  'rejected_routes':['First route'],'dissent':['No dissent recorded in fixture'],
                  'stop_conditions':['User stop'],'original_goal_preserved':True}
        bad=copy.deepcopy(last);bad['5']['status']='failed'
        with self.assertRaises(TangoError):self.b.meeting_decide(*c,decision,bad)
        outcome=self.b.meeting_decide(*c,decision,last)
        self.assertTrue(outcome['meeting_complete'])
        self.assertEqual(self.b.pending(run_id=self.r)[0]['kind'],'execute')
        self.assertNotEqual(self.b.run(self.r)['state'],'COMPLETED')
        self.assertEqual(self.b.db.execute('SELECT COUNT(*) FROM meeting_posts').fetchone()[0],50)

    def test_interrupted_cli_meeting_reconciles_unbound_member(self):
        c,coordinator=self.prepare_meeting();receipts={}
        for i in range(1,6):
            self.b.meeting_reserve(*c,i)
            p,receipt=self.attempt(c,session=f'SIMULATION-member-{i}',completed=True,member=i)
            receipts[i]=receipt
            if i!=5:self.b.meeting_bind(*c,i,receipt['agent_id'],receipt)
        # A crash after member five started, but before binding, is an explicit orphan gap.
        result=self.b.interrupt_cli_request(*c,'quota','SIMULATION account quota unavailable')
        self.assertEqual(result['state'],'WAITING')
        self.assertEqual(len(result['cancellations']),6)
        key=c[0]+':member:5'
        self.b.reconcile_spawn(key,receipts[5]['agent_id'],self.terminal(receipts[5]))
        self.b.ack_cancel(key,self.terminal(receipts[5]))
        for i in range(1,5):self.b.ack_cancel(c[0]+':member:'+str(i),self.terminal(receipts[i]))
        self.b.ack_cancel(c[0],self.terminal(coordinator))
        self.assertEqual(self.b.cancellations(run_id=self.r),[])
        self.assertEqual(self.b.db.execute('SELECT state FROM meetings').fetchone()[0],'open')

    def test_interrupted_cli_meeting_keeps_partial_round_posts(self):
        c,coordinator=self.prepare_meeting();receipts={}
        for i in range(1,6):
            self.b.meeting_reserve(*c,i)
            p,receipt=self.attempt(c,session=f'SIMULATION-member-{i}',completed=True,member=i)
            receipts[i]=receipt
            self.b.meeting_bind(*c,i,receipt['agent_id'],receipt)
        context=self.b.meeting_context(*c)
        receipt={**receipts[1],'round':1,'context_hash':context['context_hash']}
        self.b.meeting_submit(*c,1,1,context['context_hash'],self.post_content(1,1),receipt)
        saved=self.b.db.execute('SELECT content FROM meeting_posts').fetchone()[0]
        self.b.interrupt_cli_request(*c,'network','SIMULATION transport disconnected')
        for i in range(1,6):self.b.ack_cancel(c[0]+':member:'+str(i),self.terminal(receipts[i]))
        self.b.ack_cancel(c[0],self.terminal(coordinator))
        self.assertEqual(self.b.db.execute('SELECT content FROM meeting_posts').fetchone()[0],saved)
        self.assertEqual(self.b.db.execute('SELECT state,round FROM meetings').fetchone()[:],('open',1))

    def test_recovered_unbound_member_can_ack_definite_resume_spawn_failure(self):
        old,coordinator=self.prepare_meeting();receipts={}
        for member in range(1,6):
            self.b.meeting_reserve(*old,member)
            p,receipt=self.attempt(old,session=f'SIMULATION-historical-{member}',completed=True,member=member)
            receipts[member]=receipt
            self.b.meeting_bind(*old,member,receipt['agent_id'],receipt)
        context=self.b.meeting_context(*old)
        self.b.meeting_submit(*old,1,1,context['context_hash'],self.post_content(1,1),
            {**receipts[1],'round':1,'context_hash':context['context_hash']})
        self.b.interrupt_cli_request(*old,'no_progress','SIMULATION meeting restart')
        for member in range(1,6):self.b.ack_cancel(old[0]+':member:'+str(member),self.terminal(receipts[member]))
        self.b.ack_cancel(old[0],self.terminal(coordinator))
        new=self.claim('meeting')
        p,new_coordinator=self.attempt(new,session='SIMULATION-new-coordinator',completed=True)
        self.bind(new,new_coordinator)
        self.b.meeting_reserve(*new,1)
        historical=receipts[1]['agent_id']
        p,resume=self.attempt(new,session=historical,member=1,tool='codex.exec.resume')
        data=json.loads(p.read_text())
        data.update(state='spawn_failed',process_started=False,process_identity=None,
            requested_session=historical,thread_observed=False,process_exited=True,exit_code=None,
            turn_completed=False,error='SIMULATION resume Popen failed before new member binding')
        p.write_text(json.dumps(data))
        self.b.interrupt_cli_request(*new,'no_progress','SIMULATION failed participant launch')
        key=new[0]+':member:1'
        evidence=self.unbound_receipt((key,new[1],new[2]),p,'not_started')
        self.b.ack_cli_not_started(key,evidence)
        self.b.ack_cancel(new[0],self.terminal(new_coordinator))
        self.assertEqual(self.b.cancellations(run_id=self.r),[])
        member=self.b.db.execute('SELECT session FROM meeting_members WHERE request_id=? AND member=1',(new[0],)).fetchone()
        self.assertIsNone(member['session'])
        self.assertEqual(self.b.db.execute('SELECT session FROM meeting_posts').fetchone()['session'],historical)


if __name__=='__main__':unittest.main()
