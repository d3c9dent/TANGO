#!/usr/bin/env python3
"""Start/stop only TANGO's queue scheduler, with verified process identity."""
import argparse,fcntl,json,os,signal,subprocess,sys,time
from pathlib import Path

def fingerprint(pid):
    try:
        fields=Path(f'/proc/{pid}/stat').read_text().split()
        return fields[21] if fields[2]!='Z' else None
    except (OSError,IndexError):return None

def read_status(home):
    h=Path(home);sf=h/'scheduler.json'
    try:s=json.loads(sf.read_text())
    except (OSError,json.JSONDecodeError):return {'running':False,'healthy':False,'process_alive':False,'lock_held':False}
    current_namespace=os.readlink('/proc/self/ns/pid');same_namespace=s.get('pid_namespace')==current_namespace
    held=False
    try:
        with open(h/'scheduler.lock','r') as f:
            try:fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB);fcntl.flock(f,fcntl.LOCK_UN)
            except BlockingIOError:held=True
    except FileNotFoundError:pass
    s['observer_pid_namespace']=current_namespace
    s['same_pid_namespace']=same_namespace
    s['lock_held']=held
    s['process_alive']=bool(s.get('pid') and s.get('process_start_ticks') and fingerprint(s['pid'])==s['process_start_ticks']) if same_namespace else None
    s['healthy']=bool(held and not s.get('stopped') and time.time()-s.get('heartbeat',0)<15)
    s['running']=s['healthy']
    s['survives_owning_exec_end']=False
    return s

def main():
    p=argparse.ArgumentParser();p.add_argument('--home',default='/workspace/shared/tango');p.add_argument('command',choices=['start','stop','status','restart','run']);a=p.parse_args()
    h=Path(a.home).resolve();h.mkdir(parents=True,exist_ok=True);sf=h/'scheduler.json'
    if a.command=='run':
        os.execv(sys.executable,[sys.executable,str(Path(__file__).resolve().with_name('tango.py')),'--home',str(h),'daemon'])
    def status():
        return read_status(h)
    s=status()
    if a.command in {'stop','restart'} and s.get('lock_held') and not s.get('same_pid_namespace'):
        raise SystemExit('Scheduler lock is held in another PID namespace; interrupt its retained exec session. No PID was signalled.')
    if a.command in {'stop','restart'} and s.get('process_alive'):
        # Only a matching PID/start-time pair recorded by this home may be signalled.
        os.kill(s['pid'],signal.SIGTERM)
        for _ in range(50):
            if fingerprint(s['pid'])!=s['process_start_ticks']:break
            time.sleep(.1)
        else:raise SystemExit('TANGO scheduler did not stop; no force kill attempted')
        s=status()
    if a.command in {'start','restart'} and not s.get('process_alive') and not s.get('lock_held'):
        prog=Path(__file__).resolve().with_name('tango.py');logdir=h/'service-logs';logdir.mkdir(exist_ok=True)
        with open(logdir/'scheduler.log','ab',buffering=0) as f:
            process=subprocess.Popen([sys.executable,str(prog),'--home',str(h),'daemon','--lifecycle','detached_within_current_exec'],stdin=subprocess.DEVNULL,stdout=f,stderr=f,start_new_session=True)
        for _ in range(50):
            s=status()
            if s['running']:break
            if process.poll() is not None:raise SystemExit('TANGO scheduler failed; inspect its log')
            time.sleep(.1)
        else:raise SystemExit('No scheduler heartbeat received')
    print(json.dumps(status(),sort_keys=True))

if __name__=='__main__':main()
