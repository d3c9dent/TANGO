"""Synthetic local fixtures only; never evidence of a real project inventory."""
import json
import unittest
from pathlib import Path
import test_meetings as meeting_tests
from tango import Broker,TangoError

class Inventory(unittest.TestCase):
    setUp=meeting_tests.Meetings.setUp;tearDown=meeting_tests.Meetings.tearDown;claim=meeting_tests.Meetings.claim;ten=meeting_tests.Meetings.ten
    prepare=meeting_tests.Meetings.prepare;preparation_data=meeting_tests.Meetings.preparation_data;bind=meeting_tests.Meetings.bind
    def prep(self):self.ten();return self.claim('prepare_meeting')
    def test_prepare_required_before_any_discussion(self):
        c=self.prep()
        with self.assertRaises(TangoError):self.b.meeting_reserve(*c,1)
        with self.assertRaises(TangoError):self.b.meeting_context(*c)
        self.assertEqual(self.b._calculation_count(self.r),10)
    def test_empty_sources_and_empty_inventory_rejected(self):
        c=self.prep();cat,summ,cov,rec=self.preparation_data(c)
        with self.assertRaises(TangoError):self.b.seal_meeting_summary(*c,{**cat,'sources':[]},summ,cov,rec)
        summ['open_obligations']=[]
        with self.assertRaises(TangoError):self.b.seal_meeting_summary(*c,cat,summ,cov,rec)
    def test_partial_requires_caveats(self):
        c=self.prep();cat,summ,cov,rec=self.preparation_data(c)
        scope={**cat['scope'],'roots':cat['scope']['roots']+[str(Path(self.tmp.name)/'missing-history')]}
        cat=self.b.prepare_catalogue(*c,scope)['catalogue']
        with self.assertRaises(TangoError):self.b.seal_meeting_summary(*c,cat,summ,cov,rec)
        cov.update(claim='partial',caveats=['Historical archive unavailable; no total-coverage claim'])
        result=self.b.seal_meeting_summary(*c,cat,summ,cov,rec)
        self.assertTrue(result['sealed'])
    def test_unknown_source_and_unaccounted_source_rejected(self):
        c=self.prep();cat,summ,cov,rec=self.preparation_data(c)
        summ['open_obligations'][0]['sources']=[{'path':'missing','sha256':'x'}]
        with self.assertRaises(TangoError):self.b.seal_meeting_summary(*c,cat,summ,cov,rec)
        cat,summ,cov,rec=self.preparation_data(c)
        extra=Path(self.tmp.name)/'extra.txt';extra.write_text('unaccounted fixture')
        cat=self.b.prepare_catalogue(*c,{'roots':cat['scope']['roots']+[str(extra)]})['catalogue']
        with self.assertRaises(TangoError):self.b.seal_meeting_summary(*c,cat,summ,cov,rec)
    def test_source_changes_during_preparation_rejected(self):
        c=self.prep();data=self.preparation_data(c)
        Path(data[0]['sources'][0]['path']).write_text('changed')
        with self.assertRaises(TangoError):self.b.seal_meeting_summary(*c,*data)
        self.assertEqual(self.b.db.execute('SELECT COUNT(*) FROM meeting_summaries').fetchone()[0],0)
    def test_source_changes_commit_fencing_and_rebuild(self):
        self.ten();c=self.claim('meeting');self.bind(c)
        old=self.b.meeting_context(*c)['meeting_id']
        (Path(self.tmp.name)/'research-source.txt').write_text('changed source')
        with self.assertRaises(TangoError):self.b.meeting_context(*c)
        self.assertEqual(self.b.db.execute('SELECT state FROM requests WHERE id=?',(c[0],)).fetchone()[0],'cancelled')
        self.assertEqual(self.b.db.execute('SELECT state FROM meetings WHERE id=?',(old,)).fetchone()[0],'inventory_stale')
        self.assertEqual(len(self.b.cancellations(run_id=self.r)),6)
        self.assertEqual(self.b.pending(run_id=self.r)[0]['kind'],'prepare_meeting')
        self.assertNotEqual(self.b.pending(run_id=self.r)[0]['payload']['meeting_id'],old)
    def test_source_directory_membership_changes(self):
        c=self.prep();cat,summ,cov,rec=self.preparation_data(c)
        folder=Path(self.tmp.name)/'research';folder.mkdir();(folder/'one').write_text('one')
        cat=self.b.prepare_catalogue(*c,{'roots':[str(folder)]})['catalogue']
        summ['open_obligations'][0]['sources']=cat['sources']
        self.b.seal_meeting_summary(*c,cat,summ,cov,rec)
        (folder/'two').write_text('new result')
        q=self.b.pending(run_id=self.r)[0]
        with self.assertRaises(TangoError):self.b.claim(q['id'],'parent')
        self.assertEqual(self.b.pending(run_id=self.r)[0]['kind'],'prepare_meeting')
    def test_tampered_seal_cannot_be_used(self):
        self.ten();c=self.claim('meeting');ctx=self.b.meeting_context(*c)
        Path(ctx['summary_path']).write_text('{}')
        with self.assertRaises(TangoError):self.b.meeting_context(*c)
        self.assertEqual(self.b.pending(run_id=self.r)[0]['kind'],'prepare_meeting')
    def test_prepare_crash_recovery_and_sealed_restart(self):
        c=self.prep();mid=json.loads(self.b.db.execute('SELECT payload FROM requests WHERE id=?',(c[0],)).fetchone()[0])['meeting_id']
        self.b.db.execute('UPDATE runs SET lease_until=0 WHERE run_id=?',(self.r,));self.b.recover(run_id=self.r)
        for x in self.b.cancellations(run_id=self.r):self.b.ack_cancel(x['request_id'],dict(kind='simulation',agent_id=x['native_session']))
        c=self.claim('prepare_meeting');res=self.prepare(c)
        self.b.db.close();self.b=Broker(self.tmp.name,simulation=True)
        c=self.claim('meeting');ctx=self.b.meeting_context(*c)
        self.assertEqual(ctx['meeting_id'],mid);self.assertEqual(ctx['summary_sha256'],res['summary_sha256'])
    def test_legacy_unsealed_meeting_rebuilds(self):
        self.ten();c=self.claim('meeting')
        # Simulate a 0.3.0 open meeting with no summary table row, never a production mutation.
        self.b.db.execute('DELETE FROM meeting_summaries')
        with self.assertRaises(TangoError):self.b.meeting_context(*c)
        self.assertEqual(self.b.pending(run_id=self.r)[0]['kind'],'prepare_meeting')
    def test_export_includes_summary_and_provenance(self):
        self.ten();c=self.claim('meeting')
        x=self.b.export(self.r)['meeting_summaries'][0]
        self.assertEqual(json.loads(x['summary_json'])['provenance']['native_session'],x['native_session'])
    def test_native_receipt_required_for_preparation(self):
        c=self.prep();data=self.preparation_data(c);self.b.simulation=False
        with self.assertRaises(TangoError):self.b.seal_meeting_summary(*c,*data)
    def test_source_addition_before_seal_not_silently_omitted(self):
        c=self.prep();cat,summ,cov,rec=self.preparation_data(c)
        cat['sources'][0]['sha256']='0'*64
        with self.assertRaises(TangoError):self.b.seal_meeting_summary(*c,cat,summ,cov,rec)
