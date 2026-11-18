"""Isolated simulation protocol tests. These do not represent actual model discussions."""
import json
import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'program'))
from tango import Broker,TangoError

class Meetings(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='tango-meetings-test-')
        self.b=Broker(self.tmp.name,simulation=True)
        self.r=self.b.route('start','\\TANGO_TASK_1 start: original theorem','test_injection')['run_id']
    def tearDown(self):
        self.b.db.close();self.tmp.cleanup()
    def claim(self,kind='execute'):
        self.b.tick(run_id=self.r);q=self.b.pending(run_id=self.r)[0]
        if kind=='meeting' and q['kind']=='prepare_meeting':
            prep=self.claim('prepare_meeting');self.prepare(prep)
            q=self.b.pending(run_id=self.r)[0]
        self.assertEqual(q['kind'],kind)
        c=self.b.claim(q['id'],'parent')
        self.b.bind_native(q['id'],'parent',c['fence'],'coordinator-'+q['id'],{'kind':'simulation','agent_id':'coordinator-'+q['id']})
        return (q['id'],'parent',c['fence'])
    def preparation_data(self,c):
        source=Path(self.tmp.name)/'research-source.txt'
        if not source.exists():source.write_text('SIMULATION fixture: accepted lemma and open theorem')
        cat=self.b.prepare_catalogue(*c,{'roots':[str(source)],'excluded':[]})['catalogue']
        summary={k:[] for k in ('accepted_results','failed_routes','conditional_assumptions','open_obligations','unverified_findings')}
        summary['open_obligations']=[dict(id='O1',statement='SIMULATION unresolved theorem',scope='SIMULATION fixed model',verification_status='open',sources=cat['sources'])]
        coverage=dict(claim='scoped_complete',search_method='SIMULATION fixture enumeration',unavailable=[],excluded=[],caveats=[],source_dispositions=[])
        receipt=dict(kind='simulation',agent_id='coordinator-'+c[0])
        return cat,summary,coverage,receipt
    def prepare(self,c):
        return self.b.seal_meeting_summary(*c,*self.preparation_data(c))
    def ten(self):
        for i in range(10):self.b.fail(*self.claim(),'no_progress','substantive simulated route ruled out',{'i':i})
    def bind(self,c):
        for i in range(1,6):
            self.b.meeting_reserve(*c,i)
            self.b.meeting_bind(*c,i,c[0]+'-m'+str(i),{'kind':'simulation','agent_id':c[0]+'-m'+str(i)})
    def content(self,n,i):
        return dict(analysis=f'round {n} member {i} analysis',evidence='test-only evidence',objections='test-only objection',open_obligations=['full theorem'],next_step='test-only next step')
    def post(self,c,i,ctx=None):
        ctx=ctx or self.b.meeting_context(*c)
        return self.b.meeting_submit(*c,i,ctx['round'],ctx['context_hash'],self.content(ctx['round'],i),dict(kind='simulation',agent_id=c[0]+'-m'+str(i),round=ctx['round'],context_hash=ctx['context_hash']))
    def decision(self):
        return dict(direction='route B',rationale='route A blocked',next_steps=['compute lemma B'],expected_artifacts=['lemma B proof'],open_obligations=['full theorem'],rejected_routes=['A'],dissent=['member 2 doubts B'],stop_conditions=['user stop'],original_goal_preserved=True)
    def finish(self,c):
        receipts={str(i):dict(kind='simulation',agent_id=c[0]+'-m'+str(i)) for i in range(1,6)}
        return self.b.meeting_decide(*c,self.decision(),receipts)
    def test_full_10_by_5_and_automatic_continuation(self):
        self.ten();c=self.claim('meeting');self.bind(c)
        for n in range(1,11):
            ctx=self.b.meeting_context(*c);self.assertEqual(ctx['round'],n)
            self.assertEqual(len(ctx['previous_rounds']),5*(n-1))
            for i in range(1,6):
                result=self.post(c,i,ctx)
                self.assertEqual(result['round_complete'],i==5)
                if i<5:
                    fresh=self.b.meeting_context(*c)
                    self.assertEqual(fresh['context_hash'],ctx['context_hash'])
                    self.assertEqual(fresh['previous_rounds'],ctx['previous_rounds'])
        result=self.finish(c);self.assertEqual(len(result['next_requests']),1)
        q=self.b.pending(run_id=self.r)[0];self.assertEqual(q['kind'],'execute')
        self.assertEqual(q['payload']['direction_decision']['direction'],'route B')
        self.assertTrue(q['payload']['direction_decision']['not_a_proof_verdict'])
        self.assertNotEqual(self.b.run(self.r)['state'],'COMPLETED')
        for _ in range(9):self.b.fail(*self.claim(),'no_progress','next bounded attempt')
        self.assertEqual(self.b.pending(run_id=self.r)[0]['kind'],'execute')
        self.b.fail(*self.claim(),'no_progress','tenth next attempt')
        self.assertEqual(self.b.pending(run_id=self.r)[0]['kind'],'prepare_meeting')
    def test_early_decision_and_missing_member_rejected(self):
        self.ten();c=self.claim('meeting');self.bind(c)
        with self.assertRaises(TangoError):self.finish(c)
        ctx=self.b.meeting_context(*c)
        for i in range(1,5):self.post(c,i,ctx)
        future={**ctx,'round':2}
        with self.assertRaises(TangoError):self.post(c,5,future)
        self.assertEqual(self.b.meeting_context(*c)['round'],1)
    def test_duplicate_idempotent_but_conflict_rejected(self):
        self.ten();c=self.claim('meeting');self.bind(c);ctx=self.b.meeting_context(*c)
        self.post(c,1,ctx);self.assertTrue(self.post(c,1,ctx)['duplicate'])
        bad=self.content(1,1);bad['analysis']='different'
        with self.assertRaises(TangoError):self.b.meeting_submit(*c,1,1,ctx['context_hash'],bad,dict(kind='simulation',agent_id=c[0]+'-m1',round=1,context_hash=ctx['context_hash']))
    def test_pause_fences_and_cancels_all_six(self):
        self.ten();c=self.claim('meeting');self.bind(c)
        self.post(c,1)
        self.b.route('pause','\\TANGO_TASK_1 pause','test_injection')
        self.assertEqual(len(self.b.cancellations(run_id=self.r)),6)
        with self.assertRaises(TangoError):self.post(c,2)
    def test_recovery_retains_partial_round(self):
        self.ten();c=self.claim('meeting');self.bind(c);self.post(c,1)
        mid=self.b.meeting_context(*c)['meeting_id']
        self.b.db.execute('UPDATE runs SET lease_until=0 WHERE run_id=?',(self.r,))
        self.b.recover(run_id=self.r)
        for item in self.b.cancellations(run_id=self.r):self.b.ack_cancel(item['request_id'],dict(kind='simulation',agent_id=item['native_session']))
        c2=self.claim('meeting');self.bind(c2)
        ctx=self.b.meeting_context(*c2);self.assertEqual(ctx['meeting_id'],mid);self.assertEqual(ctx['submitted_members'],[1])
        for i in range(2,6):self.post(c2,i,ctx)
        self.assertEqual(self.b.meeting_context(*c2)['round'],2)
    def test_new_input_restarts_discussion_with_fresh_snapshot(self):
        self.ten();c=self.claim('meeting');self.bind(c);old=self.b.meeting_context(*c)['meeting_id']
        self.b.route('input','\\TANGO_TASK_1 retain exact constants','test_injection')
        for item in self.b.cancellations(run_id=self.r):self.b.ack_cancel(item['request_id'],dict(kind='simulation',agent_id=item['native_session']))
        c2=self.claim('meeting');self.assertNotEqual(self.b.meeting_context(*c2)['meeting_id'],old)
        self.assertEqual(self.b.meeting_context(*c2)['round'],1)
        self.assertIn('retain exact constants',json.dumps(self.b.meeting_context(*c2)['snapshot']['inputs']))
    def test_distinct_participants_and_production_receipts(self):
        self.ten();c=self.claim('meeting');self.bind(c)
        with self.assertRaises(TangoError):self.b.meeting_bind(*c,2,c[0]+'-m1',dict(kind='simulation',agent_id=c[0]+'-m1'))
        self.b.simulation=False
        with self.assertRaises(TangoError):self.post(c,1)
    def test_meeting_excludes_other_slot_claim(self):
        self.ten();c=self.claim('meeting')
        r2=self.b.route('start2','\\TANGO_TASK_2 start: other','test_injection')['run_id'];self.b.tick(run_id=r2)
        q=self.b.pending(run_id=r2)[0]
        with self.assertRaises(TangoError):self.b.claim(q['id'],'other')
    def test_unbound_spawn_gap_retained(self):
        self.ten();c=self.claim('meeting');self.b.meeting_reserve(*c,1)
        self.b.route('stop','\\TANGO_TASK_1 stop','test_injection')
        cancellations=self.b.cancellations(run_id=self.r)
        self.assertTrue(any(x['native_session'].startswith('unbound:') for x in cancellations))
        self.assertEqual(self.b.run(self.r)['state'],'STOPPED')
    def test_unbound_no_progress_does_not_count(self):
        for i in range(11):
            self.b.tick(run_id=self.r);q=self.b.pending(run_id=self.r)[0];c=self.b.claim(q['id'],'parent')
            self.b.fail(q['id'],'parent',c['fence'],'no_progress','not actually dispatched')
        self.assertEqual(self.b._calculation_count(self.r),0)
        self.assertEqual(self.b.pending(run_id=self.r)[0]['kind'],'execute')
    def test_initial_contract_restarts_stale_snapshot(self):
        self.ten();c=self.claim('meeting');self.bind(c);old=self.b.meeting_context(*c)['meeting_id']
        self.b.set_contract(self.r,1,[dict(id='full',description='prove full goal')],['exact constants'])
        for item in self.b.cancellations(run_id=self.r):self.b.ack_cancel(item['request_id'],dict(kind='simulation',agent_id=item['native_session']))
        c2=self.claim('meeting');ctx=self.b.meeting_context(*c2)
        self.assertNotEqual(ctx['meeting_id'],old)
        self.assertEqual(ctx['snapshot']['goal']['criteria'][0]['id'],'full')
    def test_queued_meeting_prevents_starvation(self):
        self.ten()
        r2=self.b.route('start2','\\TANGO_TASK_2 start: other','test_injection')['run_id'];self.b.tick(run_id=r2)
        q=self.b.pending(run_id=r2)[0]
        with self.assertRaises(TangoError):self.b.claim(q['id'],'other')
    def test_export_retains_meeting_and_participant_history(self):
        self.ten();c=self.claim('meeting');self.bind(c);self.post(c,1)
        exported=self.b.export(self.r)
        self.assertEqual(len(exported['meetings']),1)
        self.assertEqual(len(exported['meeting_members']),5)
        self.assertEqual(len(exported['meeting_posts']),1)
        self.assertIn('analysis',exported['meeting_posts'][0]['content'])
    def test_restart_persists_posts(self):
        self.ten();c=self.claim('meeting');self.bind(c);self.post(c,1)
        self.b.db.close();self.b=Broker(self.tmp.name,simulation=True)
        self.assertEqual(self.b.meeting_context(*c)['submitted_members'],[1])

if __name__=='__main__':unittest.main()
