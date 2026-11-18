import concurrent.futures
import json
import multiprocessing
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"program"))
from tango import Broker,TangoError

def claim_process(home,q,owner):
    try:return Broker(home,simulation=True).claim(q,owner)
    except TangoError:return None

class BrokerTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.b=Broker(self.tmp.name,simulation=True)
    def tearDown(self):self.b.db.close();self.tmp.cleanup()
    def start(self,n=1,goal='produce correct result'):
        return self.b.route('start'+str(n),f'\\TANGO_TASK_{n} 启动：{goal}','test_injection')['run_id']
    def dispatch(self,run,kind=None):
        self.b.tick();q=next(x for x in self.b.pending() if x['run_id']==run)
        if kind:self.assertEqual(q['kind'],kind)
        c=self.b.claim(q['id'],'owner-'+q['id']);s='simulated-'+q['id']
        self.b.bind_native(q['id'],c['owner'],c['fence'],s,{'kind':'simulation','agent_id':s})
        return (q['id'],c['owner'],c['fence'])
    def contract(self,r):self.b.set_contract(r,1,[{'id':'correct','description':'result is correct'}])
    def candidate(self,r,c):
        p=self.b.path(r)/'work'/'answer.txt';p.write_text('42')
        return self.b.candidate(*c,[str(p)],checkpoint={'step':'candidate_saved'})
    def report(self,r,passed=True):
        x=self.b.run(r);return {'goal_version':x['goal_version'],'input_version':x['input_version'],'artifact_version':x['artifact_version'],'checks':[{'id':'correct','passed':passed,'evidence':'simulation: deterministic equality'}],'missing':[]}
    def test_fixed_slots_standby(self):self.assertEqual([s['state'] for s in self.b.status()],['STANDBY']*3)
    def test_top_level_only(self):
        for txt in ['hello \\TANGO_TASK_1 goal','`\\TANGO_TASK_1 goal`','> \\TANGO_TASK_1 goal','```\n\\TANGO_TASK_1 goal\n```','\\TANGO_TASK_10 goal','\\TANGO_TASK_4 goal',' \\TANGO_TASK_1 goal']:
            self.assertFalse(self.b.route('a',txt,'test_injection')['routed'])
        self.assertFalse(self.b.route('a','\\TANGO_TASK_1 goal','test_injection',False)['routed'])
        self.assertFalse(self.b.route('a','\\TANGO_TASK_1 goal','external')['routed'])
    def test_replay_idempotence(self):
        r=self.start();a=self.b.route('start1','\\TANGO_TASK_1 启动：produce correct result','test_injection')
        self.assertTrue(a['duplicate']);self.assertEqual(a['run_id'],r)
        self.assertEqual(self.b.db.execute('SELECT count(*) FROM runs').fetchone()[0],1)
        self.assertEqual(len(self.b.outbox()),1)
        with self.assertRaises(TangoError):self.b.route('start1','\\TANGO_TASK_2 different','test_injection')
    def test_implicit_start(self):
        self.assertTrue(self.b.route('x','\\TANGO_TASK_2 clear goal','test_injection')['run_id'])
    def test_active_slot_refuses_new(self):
        r=self.start();x=self.b.route('n','\\TANGO_TASK_1 新任务：other','test_injection')
        self.assertEqual(x['run_id'],r);self.assertEqual(self.b.goal(self.b.run(r))['body'],'produce correct result')
    def test_pause_priority_stale_candidate(self):
        r=self.start();c=self.dispatch(r)
        self.b.route('p','\\TANGO_TASK_1 暂停','test_injection')
        with self.assertRaises(TangoError):self.candidate(r,c)
        self.assertEqual(self.b.run(r)['state'],'PAUSED');self.b.tick();self.assertEqual(self.b.pending(),[])
        self.assertEqual(len(self.b.cancellations()),1)
    def test_stop_never_recovers(self):
        r=self.start();c=self.dispatch(r);self.b.route('s','\\TANGO_TASK_1 停止','test_injection')
        self.b.recover();self.b.tick();self.assertEqual(self.b.run(r)['state'],'STOPPED');self.assertEqual(self.b.pending(),[])
        self.b.route('re','\\TANGO_TASK_1 继续','test_injection');self.assertEqual(self.b.run(r)['state'],'STOPPED')
    def test_new_run_old_result_is_fenced(self):
        r=self.start();c=self.dispatch(r);self.b.route('s','\\TANGO_TASK_1 停止','test_injection')
        r2=self.b.route('n','\\TANGO_TASK_1 新任务：next','test_injection')['run_id']
        self.assertNotEqual(r,r2)
        with self.assertRaises(TangoError):self.candidate(r,c)
        self.assertEqual(self.b.run(r2)['state'],'RUNNING')
        self.assertTrue(any(o['stale_run'] and o['run_id']==r for o in self.b.outbox()))
    def test_slot_two_input_isolation_order(self):
        rr=[self.start(i) for i in (1,2,3)]
        self.b.route('append2','\\TANGO_TASK_2 UNIQUE_ONLY_TWO','test_injection')
        self.b.tick();pp=self.b.pending();self.assertEqual(len(pp),3)
        for q in pp:
            bodies=[i['body'] for i in q['payload']['inputs']]
            self.assertEqual('UNIQUE_ONLY_TWO' in bodies,q['payload']['slot']==2)
        self.assertEqual([self.b.run(r)['input_version'] for r in rr],[0,1,0])
    def test_goal_version_revision(self):
        r=self.start();self.b.route('g','\\TANGO_TASK_1 修改目标：new goal','test_injection')
        self.assertEqual(self.b.run(r)['goal_version'],2)
        self.assertEqual(self.b.db.execute('SELECT count(*) FROM goals').fetchone()[0],2)
    def test_semantic_revision_idempotent(self):
        r=self.start();self.b.route('a','\\TANGO_TASK_1 also require x','test_injection')
        a=self.b.revise_goal(r,'a','produce result and x',[{'id':'x','description':'x'}],[])
        b=self.b.revise_goal(r,'a','produce result and x',[{'id':'x','description':'x'}],[])
        self.assertEqual(a['goal_version'],b['goal_version']);self.assertTrue(b['duplicate'])
    def test_single_owner_under_process_race(self):
        r=self.start();self.b.tick();qid=self.b.pending()[0]['id']
        with concurrent.futures.ProcessPoolExecutor(max_workers=4) as ex:
            rr=list(ex.map(claim_process,[self.tmp.name]*4,[qid]*4,[f'o{i}' for i in range(4)]))
        self.assertEqual(sum(x is not None for x in rr),1)
    def test_three_active_claims(self):
        rr=[self.start(i) for i in (1,2,3)];cc=[self.dispatch(r) for r in rr]
        self.assertEqual(self.b.db.execute("SELECT count(*) FROM requests WHERE state='claimed'").fetchone()[0],3)
        self.b.route('p','\\TANGO_TASK_1 暂停','test_injection')
        self.assertEqual([self.b.run(r)['state'] for r in rr],['PAUSED','RUNNING','RUNNING'])
    def test_cancel_consumes_capacity_until_ack(self):
        rr=[self.start(i) for i in (1,2,3)];cc=[self.dispatch(r) for r in rr]
        self.b.route('p','\\TANGO_TASK_1 暂停','test_injection');self.b.route('re','\\TANGO_TASK_1 继续','test_injection');self.b.tick()
        q=self.b.pending()[0]
        with self.assertRaises(TangoError):self.b.claim(q['id'],'new')
        self.b.ack_cancel(cc[0][0],{'kind':'simulation','agent_id':'simulated-'+cc[0][0]})
        self.b.claim(q['id'],'new')
    def test_checkpoint_expired_lease_fenced(self):
        r=self.start();c=self.dispatch(r);self.b.heartbeat(*c,checkpoint={'next':7})
        self.b.db.execute('UPDATE runs SET lease_until=0 WHERE run_id=?',(r,));self.b.recover()
        self.assertEqual(json.loads(self.b.run(r)['checkpoint']),{'next':7})
        with self.assertRaises(TangoError):self.b.heartbeat(*c)
        self.b.tick();self.assertEqual(self.b.pending()[0]['payload']['checkpoint'],{'next':7})
    def test_restart_database_persistence(self):
        r=self.start();c=self.dispatch(r);self.b.heartbeat(*c,checkpoint={'step':4})
        b2=Broker(self.tmp.name,simulation=True)
        self.assertEqual(json.loads(b2.run(r)['checkpoint']),{'step':4});self.assertEqual(b2.run(r)['owner'],c[1]);b2.db.close()
    def test_candidate_is_not_completion(self):
        r=self.start();self.contract(r);c=self.dispatch(r);a=self.candidate(r,c)
        self.assertFalse(a['completed']);self.assertEqual(self.b.run(r)['state'],'VERIFYING')
    def test_verifier_independence(self):
        r=self.start();c=self.dispatch(r);self.candidate(r,c);self.b.tick();q=self.b.pending()[0];v=self.b.claim(q['id'],'v')
        session=self.b.run(r)['producer_session']
        with self.assertRaises(TangoError):self.b.bind_native(q['id'],'v',v['fence'],session,{'kind':'simulation','agent_id':session})
        self.assertNotIn('checkpoint',q['payload']);self.assertNotIn('latest_failure',q['payload'])
    def test_failed_verification_repair_and_recheck(self):
        r=self.start();self.contract(r);c=self.dispatch(r);self.candidate(r,c);v=self.dispatch(r,'verify')
        a=self.b.verify(*v,self.report(r,False));self.assertFalse(a['passed']);self.assertEqual(a['state'],'REPAIRING')
        c=self.dispatch(r,'execute');self.candidate(r,c);v=self.dispatch(r,'verify')
        a=self.b.verify(*v,self.report(r));self.assertTrue(a['passed']);self.assertEqual(a['state'],'COMPLETED')
    def test_no_contract_blocks_completion(self):
        r=self.start();c=self.dispatch(r);self.candidate(r,c);v=self.dispatch(r,'verify')
        self.assertFalse(self.b.verify(*v,self.report(r))['passed'])
    def test_missing_evidence_blocks_completion(self):
        r=self.start();self.contract(r);c=self.dispatch(r);self.candidate(r,c);v=self.dispatch(r,'verify');rep=self.report(r);rep['checks'][0]['evidence']=''
        self.assertFalse(self.b.verify(*v,rep)['passed'])
    def test_wrong_version_blocks_completion(self):
        r=self.start();self.contract(r);c=self.dispatch(r);self.candidate(r,c);v=self.dispatch(r,'verify');rep=self.report(r);rep['artifact_version']=99
        with self.assertRaises(TangoError):self.b.verify(*v,rep)
    def test_artifact_mutation_invalidates(self):
        r=self.start();self.contract(r);c=self.dispatch(r);a=self.candidate(r,c);Path(a['manifest'][0]['path']).write_text('tampered');v=self.dispatch(r,'verify')
        self.assertFalse(self.b.verify(*v,self.report(r))['passed'])
    def test_append_fences_old_verification(self):
        r=self.start();self.contract(r);c=self.dispatch(r);self.candidate(r,c);v=self.dispatch(r,'verify')
        self.b.route('a','\\TANGO_TASK_1 new constraint','test_injection')
        with self.assertRaises(TangoError):self.b.verify(*v,self.report(r))
    def test_artifacts_cannot_escape_run(self):
        r=self.start();c=self.dispatch(r)
        with self.assertRaises(TangoError):self.b.candidate(*c,['/etc/hostname'])
    def test_transient_failure_backoff_and_replan(self):
        r=self.start()
        for n in range(3):
            c=self.dispatch(r);a=self.b.fail(*c,'network','simulated network error',{'attempt':n});self.b.db.execute('UPDATE runs SET next_due=0 WHERE run_id=?',(r,))
        self.assertEqual(a['state'],'REPLANNING');self.assertNotEqual(self.b.run(r)['state'],'COMPLETED')
    def test_missing_permission_waits(self):
        r=self.start();c=self.dispatch(r);self.b.fail(*c,'permission','approval missing');self.b.tick()
        self.assertEqual(self.b.run(r)['state'],'WAITING');self.assertEqual(self.b.pending(),[])
    def test_one_failure_does_not_block_others(self):
        rr=[self.start(i) for i in (1,2,3)];c=self.dispatch(rr[0]);self.b.fail(*c,'materials','missing')
        self.assertEqual(len(self.b.pending()),2);self.assertEqual([self.b.run(r)['state'] for r in rr],['WAITING','RUNNING','RUNNING'])
    def test_outbox_send_failure_and_dedup(self):
        r=self.start();o=self.b.outbox('bridge')[0];self.assertTrue(o['text'].startswith('\\TANGO_TASK_1 [run='+r+']'))
        self.b.ack(o['id'],'bridge',error='simulated send outage before dispatch',confirmed_not_sent=True);o2=self.b.outbox('bridge')[0];self.assertEqual(o['id'],o2['id'])
        self.b.ack(o['id'],'bridge',receipt={'simulated_message_id':'m'});self.assertEqual(self.b.outbox(),[])
        self.assertTrue(self.b.ack(o['id'],'bridge',receipt={'simulated_message_id':'m'})['duplicate'])
    def test_side_effect_intent_not_retried(self):
        r=self.start();c=self.dispatch(r);a=self.b.effect(*c,'send','test only');b=self.b.effect(*c,'send','test only')
        self.assertIn('intent_recorded',a['action']);self.assertIn('do_not_repeat',b['action'])
        self.b.effect(*c,'send','test only',receipt={'simulated':True})
        with self.assertRaises(TangoError):self.b.effect(*c,'send','different')
    def test_formal_adapter_rejects_simulated_receipt(self):
        r=self.start();self.b.tick();q=self.b.pending()[0];c=self.b.claim(q['id'],'o');self.b.simulation=False
        with self.assertRaises(TangoError):self.b.bind_native(q['id'],'o',c['fence'],'fake',{'kind':'simulation','agent_id':'fake'})
    def test_standby_preserves_history(self):
        r=self.start();self.b.route('s','\\TANGO_TASK_1 停止','test_injection');self.b.standby()
        self.assertEqual([x['state'] for x in self.b.status()],['STANDBY']*3);self.assertEqual(self.b.run(r)['state'],'STOPPED')
    def test_control_and_nested_text_are_separate(self):
        r=self.start();self.b.route('a','\\TANGO_TASK_1 Log:\n\\TANGO_TASK_2 停止','test_injection')
        self.assertIsNone(self.b.db.execute('SELECT current_run FROM slots WHERE slot=2').fetchone()[0]);self.assertEqual(self.b.run(r)['state'],'REPLANNING')
    def test_expired_send_claim_requires_reconciliation(self):
        self.start();o=self.b.outbox('bridge')[0];self.b.db.execute('UPDATE outbox SET claim_until=0 WHERE id=?',(o['id'],));self.b.recover()
        self.assertEqual(self.b.outbox(),[]);self.assertEqual(self.b.delivery_issues()[0]['status'],'unknown_delivery')
        with self.assertRaises(TangoError):self.b.reconcile_delivery(o['id'],{'method':'lookup','result':'uncertain'},False)
        self.b.reconcile_delivery(o['id'],{'method':'simulation channel lookup','result':'confirmed prior delivery'},True)
        self.assertEqual(self.b.outbox(),[])
    def test_unknown_send_error_not_automatically_retried(self):
        self.start();o=self.b.outbox('bridge')[0];self.b.ack(o['id'],'bridge',error='connection lost after submission')
        self.assertEqual(self.b.outbox(),[])
        self.b.reconcile_delivery(o['id'],{'method':'simulation channel lookup','result':'definitely absent','confirmed_not_sent':True},False)
        self.assertEqual(len(self.b.outbox()),1)
    def test_constraints_immutable_after_contract(self):
        r=self.start();self.contract(r)
        with self.assertRaises(TangoError):self.b.set_contract(r,1,[{'id':'correct','description':'result is correct'}],['weaker constraint'])
    def test_same_slot_replacement_waits_for_actual_cancel(self):
        r=self.start();c=self.dispatch(r);self.b.db.execute('UPDATE runs SET lease_until=0 WHERE run_id=?',(r,));self.b.recover();self.b.tick();q=self.b.pending()[0]
        with self.assertRaises(TangoError):self.b.claim(q['id'],'replacement')
        with self.assertRaises(TangoError):self.b.ack_cancel(c[0],{'kind':'simulation','agent_id':'wrong'})
        self.b.ack_cancel(c[0],{'kind':'simulation','agent_id':'simulated-'+c[0]});self.b.claim(q['id'],'replacement')
    def test_new_input_reevaluates_waiting_without_grant(self):
        for kind in ('materials','permission','quota'):
            with self.subTest(kind=kind):
                slot={'materials':1,'permission':2,'quota':3}[kind]
                r=self.start(slot);c=self.dispatch(r);self.b.fail(*c,kind,'original missing '+kind)
                self.b.route('new-'+kind,f'\\TANGO_TASK_{slot} Here is new information','test_injection')
                self.assertIsNone(self.b.run(r)['wait_reason']);self.assertEqual(self.b.run(r)['state'],'REPLANNING')
                self.b.tick();p=next(q['payload'] for q in self.b.pending() if q['run_id']==r)
                self.assertTrue(p['prerequisite_reevaluation']['input_is_not_authorization'])
                self.assertEqual(p['unresolved_prerequisites'][0]['kind'],kind)
                self.assertEqual(p['unresolved_prerequisites'][0]['original_evidence']['raw_error'],'original missing '+kind)
    def test_unmet_prerequisite_returns_to_wait(self):
        r=self.start();c=self.dispatch(r);self.b.fail(*c,'materials','document missing')
        self.b.route('a','\\TANGO_TASK_1 new but insufficient information','test_injection');c=self.dispatch(r)
        self.b.recheck_prerequisite(*c,'materials',False,{'method':'simulation file lookup','result':'still absent'})
        self.b.fail(*c,'materials','still absent',blocked=True);self.b.tick()
        self.assertEqual(self.b.run(r)['state'],'WAITING');self.assertEqual(self.b.pending(),[])
    def test_revision_reevaluates_existing_blocker(self):
        r=self.start();c=self.dispatch(r);self.b.fail(*c,'quota','unavailable')
        self.b.revise_goal(r,'start1','same goal revised',[{'id':'correct','description':'correct'}],[])
        self.b.tick();self.assertEqual(self.b.pending()[0]['payload']['unresolved_prerequisites'][0]['kind'],'quota')
        self.assertIsNone(self.b.run(r)['wait_reason'])
    def test_candidate_with_unresolved_prerequisite_stays_waiting(self):
        r=self.start();self.contract(r);c=self.dispatch(r);self.b.fail(*c,'permission','not authorized')
        self.b.route('a','\\TANGO_TASK_1 unrelated new input','test_injection');c=self.dispatch(r)
        with self.assertRaises(TangoError):self.b.effect(*c,'side-effect',{'target':'example'})
        candidate=self.candidate(r,c);self.assertFalse(candidate['completed']);self.assertEqual(self.b.run(r)['state'],'WAITING')
        self.b.tick();self.assertEqual(self.b.pending(),[])
    def test_actual_prerequisite_recheck_can_unblock(self):
        r=self.start();self.contract(r);c=self.dispatch(r);self.b.fail(*c,'materials','document missing')
        self.b.route('a','\\TANGO_TASK_1 supplied document','test_injection');c=self.dispatch(r)
        with self.assertRaises(TangoError):self.b.recheck_prerequisite(*c,'materials',True,{})
        self.b.recheck_prerequisite(*c,'materials',True,{'method':'simulation deterministic file read','result':'required document exists and parsed'})
        self.assertEqual(self.b.prerequisites(r),[]);self.candidate(r,c);v=self.dispatch(r,'verify')
        self.assertTrue(self.b.verify(*v,self.report(r))['passed'])
    def test_initial_contract_refreshes_queued_dispatch(self):
        r=self.start();self.b.tick();old=self.b.pending()[0]
        self.b.set_contract(r,1,[{'id':'correct','description':'result is correct'}],['constraint'])
        with self.assertRaises(TangoError):self.b.claim(old['id'],'old')
        self.b.tick();new=self.b.pending()[0]
        self.assertNotEqual(new['id'],old['id']);self.assertEqual(new['payload']['goal']['criteria'][0]['id'],'correct')
        self.assertEqual(new['payload']['goal']['constraints'],['constraint'])
    def test_initial_contract_fences_claimed_dispatch(self):
        r=self.start();c=self.dispatch(r)
        self.b.set_contract(r,1,[{'id':'correct','description':'result is correct'}],['constraint'])
        with self.assertRaises(TangoError):self.candidate(r,c)
        self.b.tick();new=self.b.pending()[0]
        with self.assertRaises(TangoError):self.b.claim(new['id'],'replacement')
        self.b.ack_cancel(c[0],{'kind':'simulation','agent_id':'simulated-'+c[0]})
        claim=self.b.claim(new['id'],'replacement')
        self.assertEqual(claim['payload']['goal']['criteria'][0]['id'],'correct')
    def test_unchanged_contract_does_not_cancel_worker(self):
        r=self.start();self.contract(r);c=self.dispatch(r);fence=self.b.run(r)['fence'];self.contract(r)
        self.assertEqual(self.b.run(r)['fence'],fence);self.b.heartbeat(*c)
    def test_permission_block_allows_existing_effect_reconciliation(self):
        r=self.start();c=self.dispatch(r);self.b.effect(*c,'old-intent',{'purpose':'authorized prior operation'})
        self.b.fail(*c,'permission','new operation requires approval');self.b.route('a','\\TANGO_TASK_1 status context','test_injection');c=self.dispatch(r)
        payload=json.loads(self.b.db.execute('SELECT payload FROM requests WHERE id=?',(c[0],)).fetchone()[0])
        self.assertEqual(payload['side_effects'][0]['effect_key'],'old-intent');self.assertEqual(self.b.export(r)['effects'][0]['effect_key'],'old-intent')
        old=self.b.effect(*c,'old-intent',{'purpose':'authorized prior operation'});self.assertIn('do_not_repeat',old['action'])
        result=self.b.effect(*c,'old-intent',{'purpose':'authorized prior operation'},receipt={'simulated_existing_receipt':True})
        self.assertEqual(result['action'],'confirmed')
        with self.assertRaises(TangoError):self.b.effect(*c,'new-intent',{'purpose':'new operation'})

if __name__=='__main__':unittest.main(verbosity=2)
