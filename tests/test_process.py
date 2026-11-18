"""Real process/SQLite tests; these do not claim native-model execution."""
import json,os,subprocess,sys,tempfile,time,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"program"))
from tango import Broker
ROOT=Path(__file__).resolve().parents[1]/"program"

class ProcessTests(unittest.TestCase):
    def test_foreign_namespace_status_and_stop_fail_closed(self):
        with tempfile.TemporaryDirectory() as d:
            cmd=[sys.executable,str(ROOT/'service.py'),'--home',d];sf=Path(d)/'scheduler.json'
            try:
                started=json.loads(subprocess.check_output(cmd+['start']));original=json.loads(sf.read_text())
                altered={**original,'pid_namespace':'pid:[foreign-test-namespace]'};sf.write_text(json.dumps(altered))
                status=json.loads(subprocess.check_output(cmd+['status']))
                self.assertIsNone(status['process_alive']);self.assertTrue(status['lock_held']);self.assertTrue(status['healthy']);self.assertFalse(status['same_pid_namespace'])
                denied=subprocess.run(cmd+['stop'],capture_output=True,text=True)
                self.assertNotEqual(denied.returncode,0);self.assertIn('No PID was signalled',denied.stderr)
                duplicate=json.loads(subprocess.check_output(cmd+['start']))
                self.assertEqual(duplicate['instance_id'],started['instance_id']);self.assertTrue(duplicate['lock_held'])
                sf.write_text(json.dumps(original))
            finally:
                if sf.exists():
                    s=json.loads(sf.read_text());s['pid_namespace']=os.readlink('/proc/self/ns/pid');sf.write_text(json.dumps(s))
                subprocess.run(cmd+['stop'],stdout=subprocess.DEVNULL)

    def test_stale_heartbeat_stop_and_restart_use_live_identity(self):
        with tempfile.TemporaryDirectory() as d:
            cmd=[sys.executable,str(ROOT/'service.py'),'--home',d];sf=Path(d)/'scheduler.json'
            try:
                first=json.loads(subprocess.check_output(cmd+['start']))
                s=json.loads(sf.read_text());s['heartbeat']=0;sf.write_text(json.dumps(s))
                unhealthy=json.loads(subprocess.check_output(cmd+['status']))
                self.assertTrue(unhealthy['process_alive']);self.assertFalse(unhealthy['healthy'])
                stopped=json.loads(subprocess.check_output(cmd+['stop']))
                self.assertFalse(stopped['process_alive'])
                second=json.loads(subprocess.check_output(cmd+['start']))
                s=json.loads(sf.read_text());s['heartbeat']=0;sf.write_text(json.dumps(s))
                restarted=json.loads(subprocess.check_output(cmd+['restart']))
                self.assertTrue(restarted['healthy']);self.assertNotEqual(restarted['pid'],second['pid'])
            finally:subprocess.run(cmd+['stop'],stdout=subprocess.DEVNULL)

    def test_crash_rolls_back_uncommitted_transaction(self):
        with tempfile.TemporaryDirectory() as d:
            b=Broker(d)
            code="import sqlite3,os;d=sqlite3.connect("+repr(str(Path(d)/'tango.sqlite3'))+");d.execute('BEGIN IMMEDIATE');d.execute(\"UPDATE slots SET current_run='uncommitted' WHERE slot=1\");os._exit(91)"
            p=subprocess.run([sys.executable,'-c',code]);self.assertEqual(p.returncode,91)
            self.assertEqual(b.status()[0]['state'],'STANDBY');self.assertEqual(b.db.execute('PRAGMA integrity_check').fetchone()[0],'ok');b.db.close()
    def test_actual_scheduler_restart_preserves_queue(self):
        with tempfile.TemporaryDirectory() as d:
            b=Broker(d);r=b.route('p','\\TANGO_TASK_1 process test','test_injection')['run_id']
            cmd=[sys.executable,str(ROOT/'service.py'),'--home',d]
            try:
                start=json.loads(subprocess.check_output(cmd+['start']));self.assertTrue(start['running'])
                pending=b.pending();self.assertEqual(len(pending),1);qid=pending[0]['id']
                second=json.loads(subprocess.check_output(cmd+['start']));self.assertEqual(second['pid'],start['pid'])
                restart=json.loads(subprocess.check_output(cmd+['restart']));self.assertTrue(restart['running']);self.assertNotEqual(restart['pid'],start['pid'])
                self.assertEqual(b.pending()[0]['id'],qid);self.assertEqual(b.run(r)['state'],'RUNNING')
                self.assertFalse(restart['local_can_wake_native'])
            finally:subprocess.run(cmd+['stop'],stdout=subprocess.DEVNULL);b.db.close()

if __name__=='__main__':unittest.main(verbosity=2)
