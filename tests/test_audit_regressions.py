"""Independent safety regressions; only TemporaryDirectory homes are mutated."""
import fcntl,json,os,subprocess,sys,tempfile,time,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]/"program"
sys.path.insert(0,str(ROOT))
from tango import Broker,TangoError
class AuditRegressionTests(unittest.TestCase):
 def setUp(self):self.t=tempfile.TemporaryDirectory(prefix='tango-audit-regression-');self.b=Broker(self.t.name,simulation=True)
 def tearDown(self):self.b.db.close();self.t.cleanup()
 def start(self):return self.b.route('start','\\TANGO_TASK_1 produce requested result','test_injection')['run_id']
 def blocked(self,kind):
  r=self.start();self.b.tick();q=self.b.pending()[0];c=self.b.claim(q['id'],'audit');self.b.bind_native(q['id'],'audit',c['fence'],'simulation-audit',{'kind':'simulation','agent_id':'simulation-audit'});self.b.fail(q['id'],'audit',c['fence'],kind,'unresolved prerequisite');return r
 def test_material_input_dispatches_reevaluation(self):
  r=self.blocked('materials');self.b.route('append','\\TANGO_TASK_1 supplied task material','test_injection');self.b.tick();self.assertEqual(len(self.b.pending()),1)
 def test_permission_input_allows_only_reevaluation(self):
  r=self.blocked('permission');self.b.route('append','\\TANGO_TASK_1 unrelated additional context, no authorization granted','test_injection');self.b.tick();pp=self.b.pending();self.assertEqual(len(pp),1);self.assertIn('permission',json.dumps(pp[0]['payload']));self.assertIn('unresolved prerequisite',json.dumps(pp[0]['payload']))
 def test_quota_input_dispatches_reevaluation(self):
  r=self.blocked('quota');self.b.route('append','\\TANGO_TASK_1 new context for checking available quota','test_injection');self.b.tick();self.assertEqual(len(self.b.pending()),1)
 def test_semantic_revision_dispatches_reevaluation(self):
  r=self.blocked('materials');self.b.route('append','\\TANGO_TASK_1 revise goal with new material','test_injection');self.b.revise_goal(r,'append','revised goal',[{'id':'correct','description':'correct output'}],[]);self.b.tick();self.assertEqual(len(self.b.pending()),1)
 def test_contract_is_fresh_in_queued_payload(self):
  r=self.start();self.b.tick();self.b.set_contract(r,1,[{'id':'correct','description':'correct output'}],['native only']);self.b.tick();q=self.b.pending()[0];c=self.b.claim(q['id'],'audit');self.assertEqual(c['payload']['goal']['criteria'],self.b.goal(self.b.run(r))['criteria']);self.assertEqual(c['payload']['goal']['constraints'],['native only'])
 def test_contract_cannot_silently_change_claimed_payload(self):
  r=self.start();self.b.tick();q=self.b.pending()[0];c=self.b.claim(q['id'],'audit')
  try:self.b.set_contract(r,1,[{'id':'correct','description':'correct output'}],['native only'])
  except TangoError:return
  with self.assertRaises(TangoError):self.b.heartbeat(q['id'],'audit',c['fence'])
 def test_prior_effect_reconciliation_allowed_with_permission_blocker(self):
  r=self.start();self.b.tick();q=self.b.pending()[0];c=self.b.claim(q['id'],'audit');self.b.bind_native(q['id'],'audit',c['fence'],'simulation-first',{'kind':'simulation','agent_id':'simulation-first'})
  self.b.effect(q['id'],'audit',c['fence'],'prior-effect',{'destination':'simulation-only'})
  self.b.fail(q['id'],'audit',c['fence'],'permission','future operation not authorized');self.b.route('append','\\TANGO_TASK_1 new context; no new permission','test_injection');self.b.tick();q2=self.b.pending()[0];c2=self.b.claim(q2['id'],'audit2');self.b.bind_native(q2['id'],'audit2',c2['fence'],'simulation-second',{'kind':'simulation','agent_id':'simulation-second'})
  self.assertEqual(q2['payload']['side_effects'][0]['effect_key'],'prior-effect');self.assertEqual(self.b.export(r)['effects'][0]['effect_key'],'prior-effect')
  read=self.b.effect(q2['id'],'audit2',c2['fence'],'prior-effect',{'destination':'simulation-only'});self.assertIn('do_not_repeat',read['action'])
  receipt=self.b.effect(q2['id'],'audit2',c2['fence'],'prior-effect',{'destination':'simulation-only'},receipt={'simulation':'confirmed prior delivery without performing a new action'});self.assertEqual(receipt['action'],'confirmed')
  with self.assertRaises(TangoError):self.b.effect(q2['id'],'audit2',c2['fence'],'new-effect',{'destination':'simulation-only'})
 def test_foreign_namespace_never_signals_matching_local_pid(self):
  # The numeric PID/start-ticks deliberately match an audit-owned local sleeper.
  # A foreign namespace must prevent using that matching number as a signal target.
  victim=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'])
  try:
   home=Path(self.t.name);ticks=Path('/proc/'+str(victim.pid)+'/stat').read_text().split()[21]
   metadata={'pid':victim.pid,'process_start_ticks':ticks,'pid_namespace':'pid:[audit-simulated-foreign]','heartbeat':time.time(),'version':'simulation-only'}
   (home/'scheduler.json').write_text(json.dumps(metadata))
   with open(home/'scheduler.lock','a+') as lock:
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    status=json.loads(subprocess.check_output([sys.executable,str(ROOT/'service.py'),'--home',str(home),'status']))
    self.assertFalse(status['same_pid_namespace']);self.assertIsNone(status['process_alive']);self.assertTrue(status['lock_held']);self.assertTrue(status['healthy']);self.assertFalse(status['survives_owning_exec_end'])
    for command in ('stop','restart'):
     reply=subprocess.run([sys.executable,str(ROOT/'service.py'),'--home',str(home),command],capture_output=True,text=True,timeout=3)
     self.assertNotEqual(reply.returncode,0);self.assertIn('No PID was signalled',reply.stderr);self.assertIsNone(victim.poll())
    fcntl.flock(lock,fcntl.LOCK_UN)
  finally:
   if victim.poll() is None:victim.terminate()
   victim.wait()
 def test_same_namespace_wrong_fingerprint_never_signals_pid(self):
  victim=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'])
  try:
   home=Path(self.t.name);metadata={'pid':victim.pid,'process_start_ticks':'audit-wrong-start-tick','pid_namespace':os.readlink('/proc/self/ns/pid'),'heartbeat':time.time()}
   (home/'scheduler.json').write_text(json.dumps(metadata))
   reply=subprocess.run([sys.executable,str(ROOT/'service.py'),'--home',str(home),'stop'],capture_output=True,text=True,timeout=3)
   self.assertEqual(reply.returncode,0);self.assertIsNone(victim.poll());self.assertFalse(json.loads(reply.stdout)['process_alive'])
  finally:
   if victim.poll() is None:victim.terminate()
   victim.wait()
 def test_stale_heartbeat_stop_targets_matching_process(self):
  with tempfile.TemporaryDirectory(prefix='tango-audit-process-') as d:
   p=subprocess.Popen([sys.executable,str(ROOT/'tango.py'),'--home',d,'daemon','--interval','1'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
   try:
    sf=Path(d)/'scheduler.json'
    for _ in range(100):
     if sf.exists():break
     time.sleep(.02)
    s=json.loads(sf.read_text());s['heartbeat']=time.time()-60;sf.write_text(json.dumps(s))
    reply=subprocess.run([sys.executable,str(ROOT/'service.py'),'--home',d,'stop'],capture_output=True,text=True,timeout=8)
    self.assertEqual(reply.returncode,0,reply.stderr)
    try:p.wait(timeout=.5)
    except subprocess.TimeoutExpired:self.fail('Service stop reported success but matching unhealthy scheduler is still alive')
   finally:
    if p.poll() is None:p.kill()
    p.wait()
if __name__=='__main__':unittest.main(verbosity=2)
