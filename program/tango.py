#!/usr/bin/env python3
"""TANGO: durable, native-parent-mediated task broker. No model/network client."""
from __future__ import annotations
import argparse, contextlib, hashlib, json, os, re, signal, sqlite3, sys, time, uuid
from pathlib import Path

VERSION = "0.4.0-autonomous"
DEFAULT_HOME = "/workspace/shared/tango"
ACTIVE = {"RUNNING", "VERIFYING", "REPAIRING", "REPLANNING", "WAITING"}
TERMINAL = {"STOPPED", "COMPLETED"}
FAILURES = {"environment", "dependency", "tool", "network", "timeout", "service", "program", "format", "verification", "no_progress", "permission", "materials", "quota"}

def dump(x): return json.dumps(x, ensure_ascii=False, sort_keys=True)
def uid(): return uuid.uuid4().hex
def now(): return time.time()
def sha(data): return hashlib.sha256(data).hexdigest()
def row(r): return dict(r) if r is not None else None

class TangoError(Exception): pass

class Broker:
    def __init__(self, home=DEFAULT_HOME, simulation=False):
        self._touched = set()
        self.simulation=simulation
        self.home = Path(home).resolve()
        self.home.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.home / "tango.sqlite3", timeout=30, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS slots(slot INTEGER PRIMARY KEY CHECK(slot BETWEEN 1 AND 3), current_run TEXT);
        CREATE TABLE IF NOT EXISTS runs(
          run_id TEXT PRIMARY KEY, slot INTEGER NOT NULL, state TEXT NOT NULL,
          goal_version INTEGER NOT NULL, input_version INTEGER NOT NULL DEFAULT 0,
          artifact_version INTEGER NOT NULL DEFAULT 0, fence INTEGER NOT NULL DEFAULT 0,
          owner TEXT, lease_until REAL, checkpoint TEXT NOT NULL DEFAULT '{}',
          next_kind TEXT NOT NULL DEFAULT 'execute', next_due REAL NOT NULL DEFAULT 0,
          failures INTEGER NOT NULL DEFAULT 0, last_failure TEXT, wait_reason TEXT,
          producer_session TEXT, created REAL NOT NULL, updated REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS goals(run_id TEXT, version INTEGER, body TEXT NOT NULL,
          constraints_json TEXT NOT NULL, criteria_json TEXT NOT NULL, source TEXT NOT NULL,
          created REAL NOT NULL, PRIMARY KEY(run_id,version));
        CREATE TABLE IF NOT EXISTS inputs(id TEXT PRIMARY KEY, slot INTEGER, run_id TEXT,
          seq INTEGER, body TEXT NOT NULL, source TEXT NOT NULL, applied INTEGER DEFAULT 0,
          response TEXT NOT NULL, created REAL NOT NULL);
        CREATE UNIQUE INDEX IF NOT EXISTS input_sequence ON inputs(run_id,seq) WHERE run_id IS NOT NULL;
        CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT,
          kind TEXT NOT NULL, payload TEXT NOT NULL, created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS requests(id TEXT PRIMARY KEY, run_id TEXT NOT NULL,
          kind TEXT NOT NULL, state TEXT NOT NULL, goal_version INTEGER NOT NULL,
          input_version INTEGER NOT NULL, artifact_version INTEGER NOT NULL,
          fence INTEGER, owner TEXT, native_session TEXT, native_receipt TEXT,
          payload TEXT NOT NULL, result TEXT, created REAL NOT NULL, updated REAL NOT NULL);
        CREATE UNIQUE INDEX IF NOT EXISTS one_request ON requests(run_id) WHERE state IN ('queued','claimed');
        CREATE TABLE IF NOT EXISTS artifacts(run_id TEXT, version INTEGER, manifest TEXT NOT NULL,
          producer_session TEXT NOT NULL, created REAL NOT NULL, PRIMARY KEY(run_id,version));
        CREATE TABLE IF NOT EXISTS verifications(id TEXT PRIMARY KEY, run_id TEXT, goal_version INTEGER,
          input_version INTEGER, artifact_version INTEGER, verifier_session TEXT,
          report TEXT NOT NULL, passed INTEGER NOT NULL, created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS outbox(id TEXT PRIMARY KEY, run_id TEXT, slot INTEGER NOT NULL,
          dedup_key TEXT UNIQUE NOT NULL, text TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
          attempts INTEGER NOT NULL DEFAULT 0, delivery_owner TEXT, claim_until REAL,
          receipt TEXT, last_error TEXT, created REAL NOT NULL, updated REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS effects(run_id TEXT, effect_key TEXT, specification TEXT,
          state TEXT NOT NULL, receipt TEXT, created REAL NOT NULL, updated REAL NOT NULL,
          PRIMARY KEY(run_id,effect_key));
        CREATE TABLE IF NOT EXISTS cancellations(request_id TEXT PRIMARY KEY, run_id TEXT,
          native_session TEXT NOT NULL, reason TEXT NOT NULL, receipt TEXT, created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS parent_actions(
          run_id TEXT PRIMARY KEY, action TEXT NOT NULL, request_id TEXT,
          due REAL NOT NULL, reason TEXT NOT NULL, updated REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS parent_presence(
          run_id TEXT PRIMARY KEY, owner TEXT NOT NULL, epoch INTEGER NOT NULL,
          lease_until REAL NOT NULL, heartbeat REAL NOT NULL,
          source TEXT NOT NULL DEFAULT 'native_parent_attestation');
        CREATE TABLE IF NOT EXISTS meeting_summaries(
          meeting_id TEXT PRIMARY KEY, schema_version INTEGER NOT NULL, summary_json TEXT NOT NULL,
          sha256 TEXT NOT NULL, path TEXT NOT NULL, native_session TEXT NOT NULL,
          native_receipt TEXT NOT NULL, created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS meeting_policy(run_id TEXT PRIMARY KEY, enabled INTEGER NOT NULL,
          baseline INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS meetings(id TEXT PRIMARY KEY, run_id TEXT NOT NULL,
          goal_version INTEGER NOT NULL, input_version INTEGER NOT NULL, calculation_count INTEGER NOT NULL,
          round INTEGER NOT NULL DEFAULT 1, state TEXT NOT NULL DEFAULT 'open', snapshot TEXT NOT NULL,
          decision TEXT, created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS meeting_members(request_id TEXT NOT NULL, member INTEGER NOT NULL,
          session TEXT, receipt TEXT, PRIMARY KEY(request_id,member), UNIQUE(request_id,session));
        CREATE TABLE IF NOT EXISTS meeting_posts(meeting_id TEXT NOT NULL, round INTEGER NOT NULL,
          member INTEGER NOT NULL, session TEXT NOT NULL, context_hash TEXT NOT NULL,
          content TEXT NOT NULL, receipt TEXT NOT NULL, created REAL NOT NULL,
          PRIMARY KEY(meeting_id,round,member));
        CREATE TABLE IF NOT EXISTS prerequisites(run_id TEXT, kind TEXT, status TEXT NOT NULL,
          original_evidence TEXT NOT NULL, recheck_evidence TEXT, created REAL NOT NULL,
          updated REAL NOT NULL, PRIMARY KEY(run_id,kind));
        ''')
        with self.tx():
            for i in (1,2,3):
                self.db.execute("INSERT OR IGNORE INTO slots VALUES (?,NULL)",(i,))
                (self.home / "slots" / str(i) / "runs").mkdir(parents=True, exist_ok=True)
            self.db.execute("INSERT INTO meta VALUES ('version',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(VERSION,))
            self.db.execute("INSERT OR IGNORE INTO meta VALUES ('adapter','native_parent')")
            self._touched.update(r[0] for r in self.db.execute('SELECT current_run FROM slots WHERE current_run IS NOT NULL'))
            # Additive recovery of blockers created by the original schema; no goal/state is weakened.
            for r in self.db.execute("SELECT run_id,wait_reason FROM runs WHERE wait_reason IS NOT NULL AND wait_reason!='retry_backoff'").fetchall():
                e=self.db.execute("SELECT payload FROM events WHERE run_id=? AND kind='failure' ORDER BY id DESC LIMIT 1",(r['run_id'],)).fetchone()
                self.db.execute("INSERT OR IGNORE INTO prerequisites VALUES (?,?,'unresolved',?,NULL,?,?)",(r['run_id'],r['wait_reason'],e[0] if e else dump({'kind':r['wait_reason'],'source':'retained scheduler blocker'}),now(),now()))

    @contextlib.contextmanager
    def tx(self):
        self.db.execute("BEGIN IMMEDIATE")
        self._touched = set()
        try:
            yield
            for run_id in sorted(self._touched):
                self._refresh_parent_action(run_id)
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def event(self, run, kind, payload):
        if run is not None: self._touched.add(run)
        self.db.execute("INSERT INTO events(run_id,kind,payload,created) VALUES (?,?,?,?)",(run,kind,dump(payload),now()))

    def run(self, run_id):
        r=row(self.db.execute("SELECT * FROM runs WHERE run_id=?",(run_id,)).fetchone())
        if not r: raise TangoError("Unknown run")
        return r

    def path(self, run_id):
        r=self.run(run_id)
        return self.home / "slots" / str(r['slot']) / "runs" / run_id

    def goal(self,r):
        g=row(self.db.execute("SELECT * FROM goals WHERE run_id=? AND version=?",(r['run_id'],r['goal_version'])).fetchone())
        g['constraints']=json.loads(g.pop('constraints_json'));g['criteria']=json.loads(g.pop('criteria_json'))
        return g

    def emit(self, slot, run, state, message, key=None):
        oid=uid(); text=f"\\TANGO_TASK_{slot} [run={run or '-'}] [{state}] {message}"
        self.db.execute("INSERT OR IGNORE INTO outbox(id,run_id,slot,dedup_key,text,created,updated) VALUES (?,?,?,?,?,?,?)",(oid,run,slot,key or oid,text,now(),now()))
        return text

    def invalidate(self,run,reason):
        for q in self.db.execute("SELECT * FROM requests WHERE run_id=? AND state='claimed'",(run,)).fetchall():
            # A crash between native spawn and binding its receipt is ambiguous, never proof no worker exists.
            self.db.execute("INSERT OR IGNORE INTO cancellations VALUES (?,?,?,?,NULL,?)",(q['id'],run,q['native_session'] or 'unbound:'+q['id'],reason,now()))
        for m in self.db.execute("SELECT m.* FROM meeting_members m JOIN requests q ON q.id=m.request_id WHERE q.run_id=? AND q.state='claimed'",(run,)).fetchall():
            key=m['request_id']+':member:'+str(m['member'])
            self.db.execute("INSERT OR IGNORE INTO cancellations VALUES (?,?,?,?,NULL,?)",(key,run,m['session'] or 'unbound:'+key,reason,now()))
        self.db.execute("UPDATE runs SET fence=fence+1,owner=NULL,lease_until=NULL,updated=? WHERE run_id=?",(now(),run))
        self.db.execute("UPDATE requests SET state='cancelled',updated=? WHERE run_id=? AND state IN ('queued','claimed')",(now(),run))
        self.event(run,"fence",{"reason":reason})

    def create_run(self,slot,goal,source):
        rid=uid(); t=now();p=self.home/'slots'/str(slot)/'runs'/rid
        for n in ('work','contexts','checkpoints','artifacts','verification','logs'): (p/n).mkdir(parents=True,exist_ok=True)
        self.db.execute("INSERT INTO runs(run_id,slot,state,goal_version,created,updated) VALUES (?,?,'RUNNING',1,?,?)",(rid,slot,t,t))
        # Goal text is preserved. The native planner must record explicit completion criteria before completion.
        self.db.execute("INSERT INTO goals VALUES (?,1,?,'[]','[]',?,?)",(rid,goal,source,t))
        self.db.execute("UPDATE slots SET current_run=? WHERE slot=?",(rid,slot))
        self.db.execute("INSERT INTO meeting_policy VALUES (?,1,0)",(rid,))
        self.event(rid,"created",{"goal":goal,"source":source})
        return rid

    def route(self, message_id, text, source="user", top_level=True):
        if source not in {'user','test_injection'}: return {"routed":False,"reason":"Only authenticated user messages or labeled tests are eligible"}
        if not top_level: return {"routed":False,"reason":"Nested content"}
        m=re.match(r"\A\\TANGO_TASK_([123])(?:[ \t\r\n]+|$)([\s\S]*)\Z",text)
        if not m: return {"routed":False,"reason":"No exact top-level prefix"}
        if not message_id: raise TangoError("Stable message ID required")
        slot=int(m[1]);body=m[2].strip()
        with self.tx():
            old=self.db.execute("SELECT * FROM inputs WHERE id=?",(message_id,)).fetchone()
            if old:
                if old['body']!=body or old['slot']!=slot or old['source']!=source: raise TangoError("Message ID conflict")
                result=json.loads(old['response']);result['duplicate']=True;return result
            current=self.db.execute("SELECT current_run FROM slots WHERE slot=?",(slot,)).fetchone()[0]
            r=self.run(current) if current else None
            command=body.casefold(); match=re.match(r"^(启动|新任务|start|new)\s*[:：]\s*([\s\S]+)$",body,re.IGNORECASE)
            out=[]
            if match:
                if r and r['state'] in ACTIVE:
                    out.append(self.emit(slot,current,r['state'],"Slot occupied; pause or stop the current task first",message_id))
                else:
                    if r and r['state']=='PAUSED':
                        self.invalidate(current,"replaced after explicit new task")
                        self.db.execute("UPDATE runs SET state='STOPPED' WHERE run_id=?",(current,))
                    current=self.create_run(slot,match[2],source);r=self.run(current)
                    out.append(self.emit(slot,current,'RUNNING',"Goal: "+match[2]+"; saved and waiting for native adapter dispatch",message_id))
            elif command in ('状态','status'):
                out.append(self.emit(slot,current,r['state'] if r else 'STANDBY',dump(self.summary(r)),message_id))
            elif command in ('暂停','pause','停止','stop','继续','resume'):
                if not r: out.append(self.emit(slot,None,'STANDBY',"No current task",message_id))
                elif command in ('暂停','pause','停止','stop'):
                    if r['state'] in TERMINAL:
                        out.append(self.emit(slot,current,r['state'],"Task has already ended",message_id))
                    else:
                        state='PAUSED' if command in ('暂停','pause') else 'STOPPED'
                        self.invalidate(current,state)
                        self.db.execute("UPDATE runs SET state=?,next_due=0,wait_reason=NULL WHERE run_id=?",(state,current))
                        out.append(self.emit(slot,current,state,"Checkpoint and history retained; previous execution claims invalidated",message_id))
                elif r['state']=='PAUSED':
                    self.db.execute("UPDATE runs SET state='RUNNING',next_due=0,wait_reason=NULL WHERE run_id=?",(current,))
                    out.append(self.emit(slot,current,'RUNNING',"Resuming from the latest valid checkpoint",message_id))
                else: out.append(self.emit(slot,current,r['state'],"Only paused tasks can resume; use new: <goal> to create a new run after stopping",message_id))
            elif not body:
                out.append(self.emit(slot,current,r['state'] if r else 'STANDBY',"Provide a goal or a control command",message_id))
            elif not r:
                current=self.create_run(slot,body,source);r=self.run(current)
                out.append(self.emit(slot,current,'RUNNING',"Goal: "+body+"; saved and waiting for native adapter dispatch",message_id))
            elif r['state'] in TERMINAL:
                out.append(self.emit(slot,current,r['state'],"Task has ended; use new: <goal> to create a new run",message_id))
            else:
                # Every appended constraint invalidates old verdicts. Preserve candidate bytes for repair.
                self.invalidate(current,"new ordered user input")
                self.db.execute("UPDATE runs SET input_version=input_version+1,next_kind='execute',next_due=0,wait_reason=NULL,state=CASE WHEN state='PAUSED' THEN state ELSE 'REPLANNING' END WHERE run_id=?",(current,))
                if self.prerequisites(current):self.event(current,'prerequisite_reevaluation_requested',{'message_id':message_id,'unresolved_prerequisites':self.prerequisites(current),'authorization_granted':False})
                gmatch=re.match(r"^(修改目标|更新目标|目标改为|验收改为|revise goal|update goal|set goal|set acceptance)\s*[:：]\s*([\s\S]+)$",body,re.IGNORECASE)
                if gmatch:
                    gv=r['goal_version']+1; oldgoal=self.goal(r)
                    self.db.execute("INSERT INTO goals VALUES (?,?,?,?,?,?,?)",(current,gv,gmatch[2],dump(oldgoal['constraints']),'[]',source,now()))
                    self.db.execute("UPDATE runs SET goal_version=? WHERE run_id=?",(gv,current))
                out.append(self.emit(slot,current,self.run(current)['state'],"Directed input received; existing results retained and changes will apply at the next execution boundary",message_id))
            seq=(self.db.execute("SELECT COALESCE(MAX(seq),0)+1 FROM inputs WHERE run_id=?",(current,)).fetchone()[0] if current else None)
            result={"routed":True,"duplicate":False,"slot":slot,"run_id":current,"messages":out,"source":source}
            self.db.execute("INSERT INTO inputs(id,slot,run_id,seq,body,source,response,created) VALUES (?,?,?,?,?,?,?,?)",(message_id,slot,current,seq,body,source,dump(result),now()))
            self.event(current,"input",{"id":message_id,"slot":slot,"seq":seq,"source":source,"body":body})
            return result

    def summary(self,r):
        if not r: return {"state":"STANDBY"}
        return {k:r[k] for k in ('run_id','slot','state','goal_version','input_version','artifact_version','fence','owner','lease_until','next_kind','next_due','wait_reason')}

    def status(self):
        return [{"slot":s['slot'],**self.summary(self.run(s['current_run']) if s['current_run'] else None)} for s in self.db.execute("SELECT * FROM slots ORDER BY slot")]

    def set_contract(self,run_id,goal_version,criteria,constraints=None,source="native_planner"):
        if not criteria or any(not isinstance(c,dict) or not c.get('id') or not c.get('description') for c in criteria): raise TangoError("Explicit identified completion criteria required")
        if len({c['id'] for c in criteria})!=len(criteria): raise TangoError("Duplicate criterion ID")
        with self.tx():
            r=self.run(run_id)
            if r['goal_version']!=goal_version: raise TangoError("Stale goal version")
            g=self.goal(r)
            if g['criteria'] and g['criteria']!=criteria: raise TangoError("Existing criteria are immutable; user goal revision required")
            if g['criteria'] and constraints is not None and constraints!=g['constraints']:raise TangoError('Existing constraints are immutable; user goal revision required')
            new_constraints=constraints if constraints is not None else g['constraints']
            if g['criteria']!=criteria or g['constraints']!=new_constraints:
                # The daemon may already have snapshotted the goal. Never retain a stale dispatch.
                self.invalidate(run_id,'initial goal contract recorded; refresh dispatch snapshot')
                self.db.execute("UPDATE runs SET next_kind='execute',state=CASE WHEN state IN ('RUNNING','VERIFYING','REPAIRING','REPLANNING') THEN 'REPLANNING' ELSE state END,updated=? WHERE run_id=?",(now(),run_id))
            self.db.execute("UPDATE goals SET criteria_json=?,constraints_json=? WHERE run_id=? AND version=?",(dump(criteria),dump(constraints if constraints is not None else g['constraints']),run_id,goal_version))
            self.event(run_id,"contract_recorded",{"source":source,"goal_version":goal_version,"criteria":criteria})
        return {"ok":True}

    def revise_goal(self,run_id,message_id,body,criteria,constraints):
        """Native interpreter records a user-authorized semantic revision; source is retained."""
        if not body or not criteria or any(not c.get('id') or not c.get('description') for c in criteria): raise TangoError('Goal and explicit criteria required')
        if len({c['id'] for c in criteria})!=len(criteria): raise TangoError('Duplicate criterion ID')
        with self.tx():
            r=self.run(run_id);i=self.db.execute('SELECT * FROM inputs WHERE id=? AND run_id=?',(message_id,run_id)).fetchone()
            if not i or i['source'] not in {'user','test_injection'}:raise TangoError('User input evidence required')
            source='input:'+message_id
            old=self.db.execute('SELECT version FROM goals WHERE run_id=? AND source=?',(run_id,source)).fetchone()
            if old:return {'goal_version':old[0],'duplicate':True}
            if r['state'] in TERMINAL:raise TangoError('Cannot revise ended run')
            self.invalidate(run_id,'user-authorized goal/acceptance revision')
            version=r['goal_version']+1
            self.db.execute('INSERT INTO goals VALUES (?,?,?,?,?,?,?)',(run_id,version,body,dump(constraints),dump(criteria),source,now()))
            self.db.execute("UPDATE runs SET goal_version=?,next_kind='execute',next_due=0,wait_reason=NULL,state=CASE WHEN state='PAUSED' THEN state ELSE 'REPLANNING' END WHERE run_id=?",(version,run_id))
            self.event(run_id,'goal_revised',{'version':version,'message_id':message_id,'criteria':criteria})
            if self.prerequisites(run_id):self.event(run_id,'prerequisite_reevaluation_requested',{'message_id':message_id,'unresolved_prerequisites':self.prerequisites(run_id),'authorization_granted':False})
        return {'goal_version':version}

    def prerequisites(self,run_id):
        return [{**dict(x),'original_evidence':json.loads(x['original_evidence']),'recheck_evidence':json.loads(x['recheck_evidence']) if x['recheck_evidence'] else None} for x in self.db.execute("SELECT * FROM prerequisites WHERE run_id=? AND status='unresolved' ORDER BY created,kind",(run_id,))]

    def recheck_prerequisite(self,qid,owner,fence,kind,available,evidence):
        if not isinstance(available,bool) or not isinstance(evidence,dict) or not evidence.get('method') or not evidence.get('result'):raise TangoError('Actual prerequisite check and its evidence required; new input alone is not approval')
        with self.tx():
            q,r=self._valid(qid,owner,fence)
            if q['kind'] not in {'execute','prepare_meeting'} or not q['native_session']:raise TangoError('Bound native executor or preparation worker required for prerequisite reevaluation')
            self._cli_completed_request(q)
            if not self.db.execute('SELECT 1 FROM prerequisites WHERE run_id=? AND kind=?',(r['run_id'],kind)).fetchone():raise TangoError('Unknown prerequisite')
            self.db.execute('UPDATE prerequisites SET status=?,recheck_evidence=?,updated=? WHERE run_id=? AND kind=?',('resolved' if available else 'unresolved',dump(evidence),now(),r['run_id'],kind))
            self.event(r['run_id'],'prerequisite_rechecked',{'request_id':qid,'kind':kind,'available':available,'evidence':evidence,'native_session':q['native_session']})
        return {'kind':kind,'available':available,'unresolved':self.prerequisites(r['run_id'])}

    def wait_for_prerequisite(self,qid,owner,fence,kind,error):
        """Return to an existing evidenced wait after a completed, unsuccessful recheck."""
        if not isinstance(kind,str) or not kind or not isinstance(error,str) or not error.strip():
            raise TangoError('An existing prerequisite kind and actual recheck explanation are required')
        with self.tx():
            q,r=self._valid(qid,owner,fence)
            if q['kind'] not in {'execute','prepare_meeting'} or not q['native_session']:
                raise TangoError('A bound prerequisite reevaluation request is required')
            self._cli_completed_request(q)
            prerequisite=self.db.execute("SELECT * FROM prerequisites WHERE run_id=? AND kind=? AND status='unresolved'",(r['run_id'],kind)).fetchone()
            if not prerequisite or not prerequisite['recheck_evidence']:
                raise TangoError('Only an existing unresolved prerequisite may be retained')
            recheck=self.db.execute("SELECT payload FROM events WHERE run_id=? AND kind='prerequisite_rechecked' AND json_extract(payload,'$.request_id')=? AND json_extract(payload,'$.kind')=? ORDER BY id DESC LIMIT 1",(r['run_id'],qid,kind)).fetchone()
            evidence=json.loads(recheck[0]) if recheck else None
            if not evidence or evidence.get('available') is not False or evidence.get('native_session')!=q['native_session']:
                raise TangoError('This request must first record an actual unavailable prerequisite recheck')
            result={'kind':'prerequisite','prerequisite':kind,'error':error,'recheck_evidence':json.loads(prerequisite['recheck_evidence'])}
            # A prerequisite check is not a substantive calculation or a completed meeting.
            self.db.execute("UPDATE requests SET state='failed',result=?,updated=? WHERE id=?",(dump(result),now(),qid))
            self.db.execute("UPDATE runs SET state='WAITING',owner=NULL,lease_until=NULL,next_kind='execute',next_due=0,wait_reason=?,updated=? WHERE run_id=?",(kind,now(),r['run_id']))
            self.event(r['run_id'],'prerequisite_wait_retained',{'request_id':qid,'kind':kind,'error':error,'native_session':q['native_session']})
            self.emit(r['slot'],r['run_id'],'WAITING','Prerequisite remains unresolved after recheck: '+kind)
        return {'state':'WAITING','wait_reason':kind,'next_requests':[],'unresolved':self.prerequisites(r['run_id'])}

    def _scope(self,slot=None,run_id=None,alias=''):
        if slot is not None and (type(slot) is not int or slot not in (1,2,3)):
            raise TangoError('slot must be 1, 2, or 3')
        sql='';params=[]
        if slot is not None:sql+=' AND '+alias+'slot=?';params.append(slot)
        if run_id is not None:
            r=self.run(run_id)
            if slot is not None and r['slot']!=slot:raise TangoError('Run does not belong to selected slot')
            sql+=' AND '+alias+'run_id=?';params.append(run_id)
        return sql,params

    def _refresh_parent_action(self,run_id):
        r=self.run(run_id)
        q=row(self.db.execute("SELECT * FROM requests WHERE run_id=? AND state IN ('queued','claimed')",(run_id,)).fetchone())
        cancel=self.db.execute("SELECT c.request_id FROM cancellations c JOIN runs x ON x.run_id=c.run_id WHERE x.slot=? AND c.receipt IS NULL ORDER BY c.created LIMIT 1",(r['slot'],)).fetchone()
        due=r['next_due'];request_id=q['id'] if q else None
        if cancel:action='reconcile_native';reason='A prior native session must be reconciled before dispatch';request_id=cancel[0];due=0
        elif r['state'] in TERMINAL:action='none';reason='Original goal completed with independent evidence' if r['state']=='COMPLETED' else 'Explicitly stopped';due=0
        elif r['state']=='PAUSED':action='none';reason='Explicitly paused';due=0
        elif q and q['state']=='claimed':action='monitor_native' if q['native_session'] else 'bind_or_reconcile_native';reason='Actual native tool result and lease renewal required';due=r['lease_until'] or 0
        elif r['wait_reason'] and r['wait_reason']!='retry_backoff':action='await_goal_decision' if r['wait_reason']=='goal_decision' else 'await_prerequisite';reason='Unresolved '+r['wait_reason'];due=0
        else:
            action='prepare_meeting' if q and q['kind']=='prepare_meeting' else ('dispatch_meeting' if q and q['kind']=='meeting' else ('dispatch_verify' if r['next_kind']=='verify' else ('replan' if r['state']=='REPLANNING' else 'dispatch_execute')))
            reason='Independent verification required' if r['next_kind']=='verify' else 'Goal still open; perform the next substantive attempt'
            if r['next_due']>now():reason+=' after retry backoff'
        value=(action,request_id,due,reason)
        old=self.db.execute('SELECT action,request_id,due,reason FROM parent_actions WHERE run_id=?',(run_id,)).fetchone()
        if old is None or tuple(old)!=value:
            self.db.execute('INSERT INTO parent_actions VALUES (?,?,?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET action=excluded.action,request_id=excluded.request_id,due=excluded.due,reason=excluded.reason,updated=excluded.updated',(run_id,*value,now()))

    def parent_actions(self,slot=None,run_id=None):
        """Durable next actions, with truthful expiring parent presence (not a wake mechanism)."""
        scope,params=self._scope(slot,run_id,'r.')
        data=[]
        for x in self.db.execute("SELECT a.*,r.slot,r.state,p.owner AS parent_owner,p.epoch AS parent_epoch,p.lease_until AS parent_lease_until FROM parent_actions a JOIN runs r ON r.run_id=a.run_id JOIN slots s ON s.current_run=r.run_id LEFT JOIN parent_presence p ON p.run_id=r.run_id WHERE 1=1"+scope+" ORDER BY a.due,a.updated,r.slot",params):
            d=dict(x);present=bool(d['parent_owner'] and (d['parent_lease_until'] or 0)>now())
            needed=d['action'] not in {'none','await_prerequisite','await_goal_decision'}
            d.update(native_owner_present=present,action_needed=needed and not present,local_can_wake_native=False,
                     supervision_status=('action_needed' if not present else 'native_parent_present') if needed else ('blocked' if d['action'] in {'await_prerequisite','await_goal_decision'} else 'inactive'),
                     ready=d['due']<=now(),presence_is_attestation=True)
            data.append(d)
        return data

    def parent_heartbeat(self,run_id,owner,lease_seconds=300):
        """Call only from an actual active native parent; never from the queue daemon."""
        if not owner or not 0<lease_seconds<=3600:raise TangoError('Owner and 0<lease_seconds<=3600 required')
        with self.tx():
            r=self.run(run_id)
            if self.db.execute('SELECT current_run FROM slots WHERE slot=?',(r['slot'],)).fetchone()[0]!=run_id:raise TangoError('Not the current run')
            old=row(self.db.execute('SELECT * FROM parent_presence WHERE run_id=?',(run_id,)).fetchone())
            if old and old['owner']!=owner and old['lease_until']>now():raise TangoError('Another native parent presence lease is active')
            epoch=old['epoch'] if old and old['owner']==owner and old['lease_until']>now() else (old['epoch']+1 if old else 1)
            t=now();self.db.execute("INSERT INTO parent_presence VALUES (?,?,?,?,?,'native_parent_attestation') ON CONFLICT(run_id) DO UPDATE SET owner=excluded.owner,epoch=excluded.epoch,lease_until=excluded.lease_until,heartbeat=excluded.heartbeat",(run_id,owner,epoch,t+lease_seconds,t))
            self._touched.add(run_id)
        return {'run_id':run_id,'owner':owner,'epoch':epoch,'lease_until':t+lease_seconds,'local_can_wake_native':False}

    def parent_release(self,run_id,owner,epoch):
        with self.tx():
            p=self.db.execute('SELECT * FROM parent_presence WHERE run_id=?',(run_id,)).fetchone()
            if not p or p['owner']!=owner or p['epoch']!=epoch:raise TangoError('Stale native parent presence release')
            self.db.execute('UPDATE parent_presence SET lease_until=0 WHERE run_id=?',(run_id,))
        return {'released':True,'local_can_wake_native':False}

    def recover(self,slot=None,run_id=None):
        scope,params=self._scope(slot,run_id);recovered=[]
        outscope,outparams=self._scope(slot,run_id)
        with self.tx():
            for r0 in self.db.execute("SELECT * FROM runs WHERE owner IS NOT NULL AND lease_until<=?"+scope,(now(),*params)).fetchall():
                r=dict(r0);self.invalidate(r['run_id'],"lease expired; old native session must not publish")
                if r['state'] in ACTIVE:
                    self.db.execute("UPDATE runs SET state='REPAIRING',next_due=0 WHERE run_id=?",(r['run_id'],))
                    self.emit(r['slot'],r['run_id'],'REPAIRING',"Execution lease expired; previous executor fenced and checkpoint retained for native adapter recovery")
                recovered.append(r['run_id'])
                self.event(r['run_id'],"recovered",{"old_owner":r['owner'],"old_fence":r['fence'],"checkpoint":json.loads(r['checkpoint'])})
            self.db.execute("UPDATE outbox SET status='unknown_delivery',last_error='Delivery claim expired; reconcile before any retry',delivery_owner=NULL,claim_until=NULL WHERE status='sending' AND claim_until<=?"+outscope,(now(),*outparams))
        return recovered

    def _queue_ready(self,slot=None,run_id=None):
        made=[]
        scope,params=self._scope(slot,run_id,"r.")
        rr=self.db.execute("SELECT r.* FROM runs r JOIN slots s ON s.current_run=r.run_id WHERE r.state IN ('RUNNING','VERIFYING','REPAIRING','REPLANNING','WAITING') AND r.owner IS NULL AND r.next_due<=? AND (r.wait_reason IS NULL OR r.wait_reason='retry_backoff')"+scope+" ORDER BY r.next_due,r.updated,r.slot",(now(),*params)).fetchall()
        for z in rr:
            r=dict(z)
            if self.db.execute("SELECT 1 FROM requests WHERE run_id=? AND state IN ('queued','claimed')",(r['run_id'],)).fetchone(): continue
            kind=r['next_kind'];g=self.goal(r)
            meeting=self._meeting_due(r) if kind=='execute' else None
            if meeting:kind='meeting' if self._summary_problem(meeting) is None else 'prepare_meeting'
            # Verification receives no generator conversation or private execution context.
            payload={"protocol":"tango.native-parent.v1","kind":kind,"run_id":r['run_id'],"slot":r['slot'],"prefix":f"\\TANGO_TASK_{r['slot']}","goal":g,"input_version":r['input_version'],"artifact_version":r['artifact_version'],"workdir":str(self.path(r['run_id'])/'work'),"constraints":{"native_only":True,"max_active_slots":3,"no_external_models":True},"inputs":[dict(x) for x in self.db.execute("SELECT id,seq,body,source FROM inputs WHERE run_id=? ORDER BY seq",(r['run_id'],))]}
            payload['continuation']={'action':'verify' if kind=='verify' else ('replan' if r['state']=='REPLANNING' else 'execute'), 'original_goal_preserved':True, 'partial_result_is_completion':False, 'no_progress_is_terminal':False, 'next_attempt_required':True, 'failures':r['failures'], 'native_parent_required':True, 'instructions':'Consume the previous result and update the checkpoint. For replan, perform a bounded substantive route search; do not repeat a rejected mechanism without identifying new structural input. A partial proof or no-new-mechanism result leaves the original goal open. Queue existence does not prove native execution.'}
            payload['unresolved_prerequisites']=self.prerequisites(r['run_id'])
            if payload['unresolved_prerequisites']:
                payload['prerequisite_reevaluation']={'required':True,'input_is_not_authorization':True,'instructions':'Recheck each prerequisite using actual materials, availability or valid permission evidence. Do not perform dependent operations until met. Record recheck_prerequisite evidence. If unmet, retain WAITING with fail(blocked=True); independent safe steps may continue and be checkpointed.'}
            if kind=='execute':
                payload['checkpoint']=json.loads(r['checkpoint'])
                payload['side_effects']=[dict(x) for x in self.db.execute('SELECT * FROM effects WHERE run_id=? ORDER BY created,effect_key',(r['run_id'],))]
                payload['latest_failure']=row(self.db.execute("SELECT kind,payload,created FROM events WHERE run_id=? AND kind IN ('failure','verification_failed','verified_contrary_requires_goal_decision') ORDER BY id DESC LIMIT 1",(r['run_id'],)).fetchone())
            elif kind=='verify':
                a=self.db.execute("SELECT manifest FROM artifacts WHERE run_id=? AND version=?",(r['run_id'],r['artifact_version'])).fetchone()
                payload['artifacts']=json.loads(a[0]) if a else []
                payload['producer_session']=r['producer_session']
                payload['independent_context_required']=True
            if meeting:
                payload['continuation']['action']=kind
                payload['meeting_id']=meeting['id']
                payload['meeting_protocol']={'participants':5,'rounds':10,'barrier':'all five validated posts before next round','native_parent_required':True,'tools':'prepare_catalogue, seal_meeting_summary, meeting_context, meeting_reserve, meeting_bind, meeting_submit, meeting_decide','mandatory_preparation':True,'preparation_instructions':'Inventory ALL accessible existing project results, not only the last ten calculations. Read and deduplicate accepted results, failed routes, conditional assumptions, open obligations and unverified findings; cite source hashes and declare coverage gaps. Seal a native-authored summary before discussion.'}
            elif kind=='execute':
                last=self.db.execute("SELECT decision FROM meetings WHERE run_id=? AND state='decided' AND goal_version=? AND input_version=? ORDER BY calculation_count DESC LIMIT 1",(r['run_id'],r['goal_version'],r['input_version'])).fetchone()
                if last:payload['direction_decision']=json.loads(last[0])
            q=uid();self.db.execute("INSERT INTO requests(id,run_id,kind,state,goal_version,input_version,artifact_version,payload,created,updated) VALUES (?,?,?,'queued',?,?,?,?,?,?)",(q,r['run_id'],kind,r['goal_version'],r['input_version'],r['artifact_version'],dump(payload),now(),now()))
            self.event(r['run_id'],'dispatch_queued',{"request_id":q,"kind":kind,"native_execution_started":False});made.append(q)
        return made

    def tick(self,slot=None,run_id=None):
        self.recover(slot,run_id)
        with self.tx():made=self._queue_ready(slot,run_id)
        return {"queued":made,"adapter":"native_parent","local_can_wake_native":False,"parent_actions":self.parent_actions(slot,run_id)}

    def pending(self,slot=None,run_id=None):
        scope,params=self._scope(slot,run_id,'r.')
        return [{**dict(q),"payload":json.loads(q['payload'])} for q in self.db.execute("SELECT q.* FROM requests q JOIN runs r ON r.run_id=q.run_id WHERE q.state='queued'"+scope+" ORDER BY q.created,r.slot,q.id",params)]

    def claim(self,request_id,owner,lease_seconds=300,adapter=None):
        self._preflight_summary(request_id)
        if not owner or not 0<lease_seconds<=3600: raise TangoError("Owner and 0<lease_seconds<=3600 required")
        if adapter not in {None,'native_parent','codex_cli'}:raise TangoError('Unknown execution adapter')
        with self.tx():
            q=row(self.db.execute("SELECT * FROM requests WHERE id=?",(request_id,)).fetchone())
            if not q or q['state']!='queued': raise TangoError("Request is not queued")
            r=self.run(q['run_id'])
            if r['state'] not in ACTIVE or r['owner'] is not None: raise TangoError("Run cannot be claimed")
            if self.db.execute('SELECT 1 FROM cancellations c JOIN runs r ON r.run_id=c.run_id WHERE r.slot=? AND c.receipt IS NULL',(r['slot'],)).fetchone():raise TangoError('Previous native session in this slot must be confirmed interrupted/finished before a replacement starts')
            occupied=self.db.execute("SELECT COUNT(*) FROM requests WHERE state='claimed'").fetchone()[0]+self.db.execute("SELECT COUNT(*) FROM cancellations WHERE receipt IS NULL").fetchone()[0]
            queued_meeting=self.db.execute("SELECT id FROM requests WHERE kind IN ('meeting','prepare_meeting') AND state='queued' ORDER BY created,id LIMIT 1").fetchone()
            if queued_meeting and queued_meeting['id']!=request_id:raise TangoError('Queued meeting has priority; finish in-flight work then dispatch the meeting')
            meeting_active=self.db.execute("SELECT 1 FROM requests WHERE kind='meeting' AND state='claimed'").fetchone()
            if meeting_active or (q['kind']=='meeting' and occupied):raise TangoError('Meeting requires an exclusive six-agent cohort; wait for other native work to finish')
            if occupied>=3: raise TangoError("Three-slot native capacity occupied; acknowledge stopped/finished native sessions first")
            f=r['fence']+1;t=now()
            self.db.execute("UPDATE runs SET owner=?,fence=?,lease_until=?,state=?,wait_reason=NULL,updated=? WHERE run_id=?",(owner,f,t+lease_seconds,'VERIFYING' if q['kind']=='verify' else 'RUNNING',t,r['run_id']))
            self.db.execute("UPDATE requests SET owner=?,fence=?,state='claimed',updated=? WHERE id=?",(owner,f,t,request_id))
            if adapter=='codex_cli':
                self.db.execute('UPDATE requests SET native_receipt=? WHERE id=?',(dump({'kind':'codex_cli_unlaunched','launch_registered':False}),request_id))
            self.event(r['run_id'],'claimed',{"request_id":request_id,"owner":owner,"fence":f,"adapter":adapter or 'native_parent'})
            return {"request_id":request_id,"owner":owner,"fence":f,"lease_until":t+lease_seconds,"native_task_name":"tango_"+q['kind']+"_"+request_id,"payload":json.loads(q['payload'])}

    def _cli_journal(self, path):
        """Read a local attempt record; these are structural attestations, not signatures."""
        if not isinstance(path,str) or not Path(path).is_absolute():
            raise TangoError('An absolute CLI attempt journal path is required')
        p=Path(path)
        if not p.resolve().is_relative_to(self.home):
            raise TangoError('CLI attempt journals must remain inside this TANGO home')
        for part in (p,*p.parents):
            if part.is_symlink():raise TangoError('CLI attempt journal symlinks are forbidden')
            if part.resolve()==self.home:break
        try:
            if not p.is_file() or p.stat().st_size>4*1024*1024:
                raise TangoError('CLI attempt journal is missing or too large')
            raw=p.read_bytes();data=json.loads(raw)
        except (OSError,ValueError) as e:
            raise TangoError('CLI attempt journal cannot be read: '+str(e)) from e
        if not isinstance(data,dict) or data.get('schema_version')!=1 or not isinstance(data.get('attempt_id'),str) or not data['attempt_id']:
            raise TangoError('Invalid CLI attempt journal identity')
        return data,sha(raw)

    @staticmethod
    def _cli_terminal_journal(data):
        if data.get('state') not in {'completed','failed','interrupted','spawn_failed'} or data.get('process_exited') is not True:
            raise TangoError('CLI process termination is not established; reconcile before replacement')
        if data.get('exit_code') is not None and type(data['exit_code']) is not int:
            raise TangoError('CLI process exit code must be an integer or unknown')
        if type(data.get('turn_completed')) is not bool:
            raise TangoError('CLI terminal record needs an explicit turn completion flag')
        if data['state']=='spawn_failed':
            if data.get('process_started') is not False or data.get('process_identity') is not None or data.get('session')!=data.get('requested_session') or data.get('thread_observed') is not False or not data.get('error'):
                raise TangoError('CLI spawn failure needs definite evidence that no child started')
        elif data.get('process_started') is not True:
            raise TangoError('CLI terminal record must identify an actually started process')
        if data['state']=='completed' and (data.get('exit_code')!=0 or data.get('turn_completed') is not True):
            raise TangoError('CLI completion requires a successful exit and completed turn')

    @staticmethod
    def _cli_receipt_path(receipt):
        if not isinstance(receipt,dict):return None
        if receipt.get('kind')=='codex_cli_pending':return receipt.get('journal_path')
        result=receipt.get('result')
        return result.get('journal_path') if receipt.get('kind')=='codex_cli' and isinstance(result,dict) else None

    @staticmethod
    def _cli_process_identity(identity):
        if not isinstance(identity,dict) or type(identity.get('pid')) is not int or identity['pid']<=0 or not isinstance(identity.get('start'),str) or not identity['start'] or identity.get('platform') not in {'linux','darwin'} or identity.get('process_group')!=identity['pid']:
            raise TangoError('CLI receipt needs a process identity and isolated process group')

    def _cli_saved_receipt(self,request_id):
        qid,separator,member=request_id.partition(':member:')
        if separator:
            if member not in {'1','2','3','4','5'}:raise TangoError('Invalid meeting member cancellation ID')
            record=self.db.execute('SELECT receipt FROM meeting_members WHERE request_id=? AND member=?',(qid,int(member))).fetchone()
        else:
            record=self.db.execute('SELECT native_receipt FROM requests WHERE id=?',(request_id,)).fetchone()
        return json.loads(record[0]) if record and record[0] else None

    def _cli_registered(self,request_id,receipt):
        expected=self._cli_receipt_path(self._cli_saved_receipt(request_id))
        actual=self._cli_receipt_path(receipt)
        if not expected or not actual or Path(actual).resolve()!=Path(expected).resolve():
            raise TangoError('CLI receipt must match the journal registered for this request')

    def _cli_receipt(self,session,receipt,completed=False,terminal=False):
        if not isinstance(receipt,dict) or receipt.get('kind')!='codex_cli' or receipt.get('agent_id')!=session:
            raise TangoError('Session-bound Codex CLI receipt required')
        result=receipt.get('result')
        tools={'codex.process'} if terminal else {'codex.exec','codex.exec.resume'}
        if receipt.get('tool') not in tools or not isinstance(result,dict) or result.get('thread_id')!=session:
            raise TangoError('CLI tool and returned thread identity must match')
        if not terminal and result.get('event')!='thread.started':
            raise TangoError('Actual CLI thread.started evidence is required')
        identity=result.get('process_identity')
        self._cli_process_identity(identity)
        data,_=self._cli_journal(result.get('journal_path'))
        if data.get('attempt_id')!=result.get('attempt_id') or data.get('session')!=session or data.get('process_identity')!=identity or data.get('process_started') is not True:
            raise TangoError('CLI receipt disagrees with its durable attempt journal')
        if not terminal and data.get('thread_observed') is not True:
            raise TangoError('CLI dispatch requires an observed thread.started event')
        if terminal or completed:
            self._cli_terminal_journal(data)
            if (result.get('exit_code') is not None and type(result['exit_code']) is not int) or type(result.get('turn_completed')) is not bool or result.get('process_exited') is not True or result.get('exit_code')!=data.get('exit_code') or result.get('turn_completed')!=data.get('turn_completed'):
                raise TangoError('CLI terminal receipt disagrees with the process result')
        if completed and (result.get('exit_code')!=0 or result.get('turn_completed') is not True or data.get('state')!='completed'):
            raise TangoError('A successful completed CLI turn is required')
        return data

    def register_cli_attempt(self,request_id,owner,fence,journal_path,member=None):
        """Reserve one durable launch record before spawning; never invent a session ID."""
        if not isinstance(journal_path,str) or not Path(journal_path).is_absolute() or not Path(journal_path).resolve().is_relative_to(self.home):
            raise TangoError('CLI attempt journal must be an absolute path inside this home')
        with self.tx():
            q,r=self._valid(request_id,owner,fence)
            if member is not None:
                if type(member) is not int or member not in range(1,6):raise TangoError('Exactly members 1 through 5')
                self._meeting_valid(request_id,owner,fence)
                record=self.db.execute('SELECT receipt FROM meeting_members WHERE request_id=? AND member=?',(request_id,member)).fetchone()
                if not record:raise TangoError('Reserve the meeting member before registering a CLI launch')
                previous=json.loads(record[0]) if record[0] else None
            else:previous=json.loads(q['native_receipt']) if q['native_receipt'] else None
            previous_path=self._cli_receipt_path(previous)
            if previous_path and Path(previous_path).resolve()==Path(journal_path).resolve():
                return {'registered':True,'duplicate':True,'journal_path':journal_path}
            if previous and previous.get('kind')!='codex_cli_unlaunched':
                if not previous_path:raise TangoError('Cannot replace another execution adapter with a CLI launch')
                data,_=self._cli_journal(previous_path);self._cli_terminal_journal(data)
            # Reusing any existing attempt would make launch/recovery identity ambiguous.
            if Path(journal_path).exists():raise TangoError('A new CLI attempt journal path is required')
            pending={'kind':'codex_cli_pending','journal_path':journal_path}
            if member is None:self.db.execute('UPDATE requests SET native_receipt=? WHERE id=?',(dump(pending),request_id))
            else:self.db.execute('UPDATE meeting_members SET receipt=? WHERE request_id=? AND member=?',(dump(pending),request_id,member))
            self.event(r['run_id'],'cli_attempt_registered',{'request_id':request_id,'member':member,'journal_path':journal_path,'previous_receipt':previous})
        return {'registered':True,'journal_path':journal_path}

    def _cli_completed_request(self,q):
        receipt=json.loads(q['native_receipt']) if q['native_receipt'] else None
        if receipt and receipt.get('kind') in {'codex_cli_pending','codex_cli_unlaunched'}:raise TangoError('CLI execution is pending; a completed result is required')
        if receipt and receipt.get('kind')=='codex_cli':self._cli_receipt(q['native_session'],receipt,completed=True)

    def bind_native(self,request_id,owner,fence,session,receipt):
        if not session or not receipt: raise TangoError("Real native session ID and tool receipt required")
        if not isinstance(receipt,dict) or receipt.get('agent_id')!=session:raise TangoError('Receipt agent_id must match native session')
        if receipt.get('kind') not in {'native','codex_cli'} and not (self.simulation and receipt.get('kind')=='simulation'):raise TangoError('Production adapter accepts native or genuine Codex CLI receipts only')
        if receipt.get('kind')=='native' and receipt.get('tool') not in {'collaboration.spawn_agent','collaboration.followup_task'}:raise TangoError('Unsupported native execution tool')
        with self.tx():
            q,r=self._valid(request_id,owner,fence)
            if receipt.get('kind')=='codex_cli':
                self._cli_registered(request_id,receipt);self._cli_receipt(session,receipt)
            if q['kind']=='verify' and session==r['producer_session']: raise TangoError("Verifier must use a separate native session")
            if q['native_session'] and q['native_session']!=session: raise TangoError("Native session already bound")
            self.db.execute("UPDATE requests SET native_session=?,native_receipt=?,updated=? WHERE id=?",(session,dump(receipt),now(),request_id))
            self.event(r['run_id'],'native_bound',{"request_id":request_id,"session":session,"receipt":receipt})
        return {"ok":True}

    def cancellations(self,slot=None,run_id=None):
        scope,params=self._scope(slot,run_id,'r.')
        return [dict(x) for x in self.db.execute('SELECT c.* FROM cancellations c JOIN runs r ON r.run_id=c.run_id WHERE c.receipt IS NULL'+scope+' ORDER BY c.created',params)]

    def reconcile_spawn(self,request_id,session,receipt):
        if isinstance(receipt,dict) and receipt.get('kind')=='codex_cli':
            self._cli_registered(request_id,receipt);self._cli_receipt(session,receipt,terminal=True)
        elif not isinstance(receipt,dict) or receipt.get('kind')!='native' or receipt.get('tool')!='collaboration.list_agents' or receipt.get('agent_id')!=session or receipt.get('result') is None:raise TangoError('Native lookup evidence identifying the orphan session is required')
        with self.tx():
            c=self.db.execute('SELECT * FROM cancellations WHERE request_id=?',(request_id,)).fetchone()
            if not c or c['receipt'] is not None or c['native_session']!='unbound:'+request_id:raise TangoError('No unresolved spawn gap for this request')
            self.db.execute('UPDATE cancellations SET native_session=? WHERE request_id=?',(session,request_id))
            qid,separator,member=request_id.partition(':member:')
            if separator:self.db.execute('UPDATE meeting_members SET session=?,receipt=? WHERE request_id=? AND member=?',(session,dump(receipt),qid,int(member)))
            else:self.db.execute('UPDATE requests SET native_session=?,native_receipt=? WHERE id=?',(session,dump(receipt),request_id))
            self.event(c['run_id'],'orphan_native_identified',{'request_id':request_id,'session':session,'receipt':receipt})
        return {'ok':True,'next_action':'interrupt or establish terminal state, then ack_cancel'}

    def ack_cancel(self,request_id,receipt):
        if not receipt:raise TangoError('Actual native interrupt/finished receipt required')
        with self.tx():
            c=self.db.execute('SELECT * FROM cancellations WHERE request_id=?',(request_id,)).fetchone()
            if not c:raise TangoError('Unknown cancellation')
            if not isinstance(receipt,dict) or receipt.get('agent_id')!=c['native_session']:raise TangoError('Cancellation receipt session mismatch')
            if receipt.get('kind')=='simulation' and self.simulation:pass
            elif receipt.get('kind')=='codex_cli':
                self._cli_registered(request_id,receipt)
                if receipt.get('status')=='not_started' or (c['native_session'].startswith('unbound:') and receipt.get('status')=='unbound_terminal'):
                    result=receipt.get('result',{});data,digest=self._cli_journal(result.get('journal_path'))
                    self._cli_terminal_journal(data)
                    unbound=c['native_session'].startswith('unbound:')
                    if receipt.get('tool')!='codex.process' or receipt.get('request_id')!=request_id or result.get('state')!=data.get('state') or result.get('attempt_id')!=data['attempt_id'] or result.get('journal_sha256')!=digest:
                        raise TangoError('Unbound CLI cancellation needs registered, exact journal evidence')
                    if receipt['status']=='not_started':
                        if data.get('state')!='spawn_failed' or result.get('process_started') is not False:
                            raise TangoError('CLI not_started requires definite evidence that no process began')
                        # A fresh request may intend to resume a historical session before binding it.
                        # Definite Popen failure proves no new child existed; do not invent a binding.
                        if not unbound and data.get('session')!=c['native_session']:
                            raise TangoError('Failed resume must target the already bound CLI session')
                    else:
                        if data.get('session') is not None or data.get('state') not in {'failed','interrupted'} or result.get('process_started') is not True or result.get('process_exited') is not True:
                            raise TangoError('Unbound CLI process termination must be established')
                        self._cli_process_identity(data.get('process_identity'))
                else:
                    if receipt.get('status') not in {'completed','failed','interrupted'}:raise TangoError('Actual CLI terminal status required')
                    self._cli_receipt(c['native_session'],receipt,completed=receipt.get('status')=='completed',terminal=True)
            elif receipt.get('kind')=='native' and receipt.get('tool')=='collaboration.interrupt_agent' and receipt.get('result') is not None:pass
            elif receipt.get('kind')=='native' and receipt.get('tool')=='collaboration.list_agents' and receipt.get('status') in {'completed','finished','done','interrupted','failed'} and receipt.get('result') is not None:pass
            elif c['native_session'].startswith('unbound:') and receipt.get('kind')=='native' and receipt.get('tool')=='collaboration.list_agents' and receipt.get('status')=='not_found' and receipt.get('request_id')==request_id and receipt.get('result') is not None:pass
            else:raise TangoError('Confirmed native interrupt/terminal-state receipt required')
            self.db.execute('UPDATE cancellations SET receipt=? WHERE request_id=? AND receipt IS NULL',(dump(receipt),request_id))
            for r0 in self.db.execute('SELECT run_id FROM runs WHERE slot=(SELECT slot FROM runs WHERE run_id=?)',(c['run_id'],)):self._touched.add(r0[0])
        return {'ok':True}

    def ack_cli_not_started(self,request_id,receipt):
        if not isinstance(receipt,dict) or receipt.get('kind')!='codex_cli' or receipt.get('status')!='not_started':
            raise TangoError('Explicit CLI spawn-failure receipt required')
        return self.ack_cancel(request_id,receipt)

    def ack_cli_unbound(self,request_id,receipt):
        if not isinstance(receipt,dict) or receipt.get('kind')!='codex_cli' or receipt.get('status')!='unbound_terminal' or receipt.get('agent_id')!='unbound:'+request_id:
            raise TangoError('Explicit unbound CLI process-termination receipt required')
        return self.ack_cancel(request_id,receipt)

    def ack_cli_unlaunched(self,request_id):
        """Close a claim-to-registration gap proved by the broker's atomic launch marker."""
        with self.tx():
            c=self.db.execute('SELECT * FROM cancellations WHERE request_id=?',(request_id,)).fetchone()
            q=self.db.execute('SELECT * FROM requests WHERE id=?',(request_id,)).fetchone()
            marker=self._cli_saved_receipt(request_id)
            if not c or not q or c['native_session']!='unbound:'+request_id or q['native_session'] is not None or marker!={'kind':'codex_cli_unlaunched','launch_registered':False}:
                raise TangoError('No atomic never-registered CLI launch evidence exists for this cancellation')
            receipt={'kind':'codex_cli','tool':'codex.launch_registry','agent_id':c['native_session'],
                     'request_id':request_id,'status':'not_started',
                     'result':{'launch_registered':False,'source':'atomic broker claim marker'}}
            self.db.execute('UPDATE cancellations SET receipt=? WHERE request_id=? AND receipt IS NULL',(dump(receipt),request_id))
            self.event(c['run_id'],'cli_unlaunched_reconciled',{'request_id':request_id,'receipt':receipt})
            for r0 in self.db.execute('SELECT run_id FROM runs WHERE slot=(SELECT slot FROM runs WHERE run_id=?)',(c['run_id'],)):self._touched.add(r0[0])
        return {'ok':True,'process_was_launched':False}

    def interrupt_cli_request(self,qid,owner,fence,kind,error,blocked=False):
        """Fence a CLI turn first; replacement remains gated on actual process termination."""
        if kind not in FAILURES or not isinstance(error,str) or not error.strip():
            raise TangoError('A known failure class and actual interruption evidence are required')
        if blocked and kind not in {'permission','materials','quota'}:
            raise TangoError('CLI prerequisite interruption needs a permission, materials or quota blocker')
        blocked=blocked or kind in {'permission','materials','quota'}
        with self.tx():
            q,r=self._valid(qid,owner,fence)
            receipt=json.loads(q['native_receipt']) if q['native_receipt'] else None
            if not self._cli_receipt_path(receipt) and receipt!={'kind':'codex_cli_unlaunched','launch_registered':False}:raise TangoError('Only an explicitly claimed CLI request may use CLI interruption')
            self.invalidate(r['run_id'],'CLI interruption: '+error)
            n=r['failures']+1
            state='WAITING' if blocked else ('REPLANNING' if n>=3 or kind=='no_progress' else 'REPAIRING')
            delay=0 if blocked or kind=='no_progress' else min(300,2**min(n,8))
            self.db.execute("UPDATE runs SET state=?,failures=?,last_failure=?,wait_reason=?,next_due=?,next_kind='execute',updated=? WHERE run_id=?",(state,n,kind,kind if blocked else 'retry_backoff',now()+delay,now(),r['run_id']))
            if blocked:
                self.db.execute("INSERT INTO prerequisites VALUES (?,?,'unresolved',?,NULL,?,?) ON CONFLICT(run_id,kind) DO UPDATE SET status='unresolved',original_evidence=excluded.original_evidence,updated=excluded.updated",(r['run_id'],kind,dump({'kind':kind,'raw_error':error,'request_id':qid,'attempt':n}),now(),now()))
            self.event(r['run_id'],'cli_interrupted',{'request_id':qid,'kind':kind,'raw_error':error,'retry_after':None if blocked else delay,'meeting_posts_preserved':q['kind']=='meeting'})
            self.emit(r['slot'],r['run_id'],state,'CLI execution fenced; retained work requires process reconciliation before continuation')
        return {'state':state,'retry_after':None if blocked else delay,'cancellations':self.cancellations(run_id=r['run_id'])}

    def _valid(self,qid,owner,fence):
        q=row(self.db.execute("SELECT * FROM requests WHERE id=?",(qid,)).fetchone())
        if not q: raise TangoError("Unknown request")
        r=self.run(q['run_id'])
        current=self.db.execute("SELECT current_run FROM slots WHERE slot=?",(r['slot'],)).fetchone()[0]
        if q['state']!='claimed' or q['owner']!=owner or q['fence']!=fence or r['owner']!=owner or r['fence']!=fence or r['state'] not in ACTIVE or current!=r['run_id'] or (r['lease_until'] or 0)<=now(): raise TangoError("Fenced, expired, paused, stopped, or stale result")
        if q['goal_version']!=r['goal_version'] or q['input_version']!=r['input_version']: raise TangoError("Goal/input version changed")
        return q,r

    def heartbeat(self,qid,owner,fence,checkpoint=None,lease_seconds=300):
        if not 0<lease_seconds<=3600: raise TangoError("Invalid lease duration")
        with self.tx():
            q,r=self._valid(qid,owner,fence)
            self.db.execute("UPDATE runs SET lease_until=?,checkpoint=?,updated=? WHERE run_id=?",(now()+lease_seconds,dump(checkpoint) if checkpoint is not None else r['checkpoint'],now(),r['run_id']))
            self._touched.add(r['run_id'])
            if checkpoint is not None:self.event(r['run_id'],'checkpoint',{"request_id":qid,"checkpoint":checkpoint})
        return {"ok":True}

    def _safe_file(self,run,path):
        p=Path(path)
        if not p.is_absolute(): p=self.path(run)/'work'/p
        p=p.resolve();root=self.path(run).resolve()
        if not p.is_relative_to(root) or not p.is_file(): raise TangoError("Artifact must be a regular file within this run")
        return p

    def candidate(self,qid,owner,fence,paths,checkpoint=None,summary="Candidate saved; awaiting independent verification"):
        with self.tx():
            q,r=self._valid(qid,owner,fence)
            self._cli_completed_request(q)
            if q['kind']!='execute' or not q['native_session']: raise TangoError("Bound native executor required")
            if not paths: raise TangoError("Candidate artifacts required")
            version=r['artifact_version']+1; manifest=[];publication=uid()
            for index,p0 in enumerate(paths):
                p=self._safe_file(r['run_id'],p0);data=p.read_bytes();dest=self.path(r['run_id'])/'artifacts'/str(version)/publication/f"{index:03d}-{p.name}"
                dest.parent.mkdir(parents=True,exist_ok=True)
                with open(dest,'xb') as f:f.write(data);f.flush();os.fsync(f.fileno())
                manifest.append({"path":str(dest),"source_path":str(p),"sha256":sha(data),"size":len(data)})
            self.db.execute("INSERT INTO artifacts VALUES (?,?,?,?,?)",(r['run_id'],version,dump(manifest),q['native_session'],now()))
            self.db.execute("UPDATE requests SET state='returned',result=?,updated=? WHERE id=?",(dump({"manifest":manifest,"summary":summary}),now(),qid))
            self.db.execute("UPDATE runs SET state='VERIFYING',artifact_version=?,producer_session=?,owner=NULL,lease_until=NULL,next_kind='verify',next_due=0,checkpoint=?,updated=? WHERE run_id=?",(version,q['native_session'],dump(checkpoint) if checkpoint is not None else r['checkpoint'],now(),r['run_id']))
            blockers=self.prerequisites(r['run_id'])
            if blockers:
                self.db.execute("UPDATE runs SET state='WAITING',next_kind='execute',wait_reason=? WHERE run_id=?",(blockers[0]['kind'],r['run_id']))
            self.event(r['run_id'],'candidate',{"version":version,"manifest":manifest,"native_session":q['native_session']})
            text=self.emit(r['slot'],r['run_id'],'WAITING' if blockers else 'VERIFYING',summary+("; prerequisites remain unresolved; candidate retained" if blockers else ''))
            successors=self._queue_ready(run_id=r['run_id'])
        return {"artifact_version":version,"manifest":manifest,"message":text,"completed":False,"unresolved_prerequisites":blockers,"next_requests":successors}

    def verify(self,qid,owner,fence,report):
        with self.tx():
            q,r=self._valid(qid,owner,fence)
            self._cli_completed_request(q)
            if q['kind']!='verify' or not q['native_session'] or q['native_session']==r['producer_session']: raise TangoError("Independent native verifier required")
            if report.get('goal_version')!=r['goal_version'] or report.get('input_version')!=r['input_version'] or report.get('artifact_version')!=r['artifact_version']: raise TangoError("Verdict version binding mismatch")
            criteria=self.goal(r)['criteria']; expected={c['id'] for c in criteria}
            items=report.get('checks',[]); got=[x.get('id') for x in items]
            manifest=json.loads(self.db.execute("SELECT manifest FROM artifacts WHERE run_id=? AND version=?",(r['run_id'],r['artifact_version'])).fetchone()[0])
            integrity=all(Path(a['path']).is_file() and sha(Path(a['path']).read_bytes())==a['sha256'] for a in manifest)
            passed=report.get('original_goal_status','satisfied')=='satisfied' and report.get('disposition') in (None,'VERIFIED_GOAL_SATISFIED') and bool(criteria) and set(got)==expected and len(got)==len(expected) and all(x.get('passed') is True and x.get('evidence') for x in items) and not report.get('missing') and integrity and not self.prerequisites(r['run_id'])
            contrary=report.get('disposition')=='VERIFIED_CONTRARY_REQUIRES_GOAL_DECISION'
            if contrary:
                c=report.get('contrary',{});failed={x.get('id') for x in items if x.get('passed') is False}
                valid=bool(criteria) and set(got)==expected and len(got)==len(expected) and all(type(x.get('passed')) is bool and x.get('evidence') for x in items) and integrity and not self.prerequisites(r['run_id'])
                valid=valid and not report.get('missing') and report.get('original_goal_status')=='not_satisfied' and isinstance(c,dict) and c.get('summary') and c.get('evidence') and c.get('refutes_criterion_ids') and set(c['refutes_criterion_ids'])<=failed
                if not valid:raise TangoError('Contrary outcome needs independent, version-bound refutation evidence and complete original-criterion status; original goal remains not_satisfied')
                passed=False
            report={**report,"integrity_passed":integrity,"criteria_complete":bool(criteria) and set(got)==expected and len(got)==len(expected),"accepted":passed,"contrary_verified":bool(contrary),"original_goal_status":"satisfied" if passed else "not_satisfied"}
            vid=uid();self.db.execute("INSERT INTO verifications VALUES (?,?,?,?,?,?,?,?,?)",(vid,r['run_id'],r['goal_version'],r['input_version'],r['artifact_version'],q['native_session'],dump(report),int(passed),now()))
            self.db.execute("UPDATE requests SET state='returned',result=?,updated=? WHERE id=?",(dump(report),now(),qid))
            count=0 if passed else r['failures']+1;state='COMPLETED' if passed else ('WAITING' if contrary else ('REPLANNING' if count>=3 else 'REPAIRING'))
            self.db.execute("UPDATE runs SET state=?,owner=NULL,lease_until=NULL,next_kind='execute',next_due=0,failures=?,last_failure=?,updated=? WHERE run_id=?",(state,count,None if passed else 'verification',now(),r['run_id']))
            if contrary:
                self.db.execute("UPDATE runs SET wait_reason='goal_decision' WHERE run_id=?",(r['run_id'],))
                self.db.execute("INSERT INTO prerequisites VALUES (?,'goal_decision','unresolved',?,NULL,?,?) ON CONFLICT(run_id,kind) DO UPDATE SET status='unresolved',original_evidence=excluded.original_evidence,recheck_evidence=NULL,updated=excluded.updated",(r['run_id'],dump({'verification_id':vid,'report':report}),now(),now()))
            self.event(r['run_id'],'verified_contrary_requires_goal_decision' if contrary else ('verification_passed' if passed else 'verification_failed'),{"verification_id":vid,"report":report})
            self.emit(r['slot'],r['run_id'],state,"All acceptance criteria are supported by evidence" if passed else ("Verified contrary outcome; original goal is not satisfied. A goal decision is required" if contrary else "Verification failed; goal retained for repair or replanning against failed checks"))
            successors=self._queue_ready(run_id=r['run_id'])
        return {"passed":passed,"state":state,"verification_id":vid,"report":report,"next_requests":successors}

    def fail(self,qid,owner,fence,kind,error,checkpoint=None,blocked=False,blocker=None):
        if kind not in FAILURES: raise TangoError("Unknown failure class")
        if not isinstance(error,str) or not error.strip():raise TangoError('Actual failure evidence required')
        if blocked and kind in {'no_progress','verification','format','program'}:raise TangoError('Incomplete work or no progress requires replanning, not a prerequisite block')
        if blocked and kind not in {'permission','materials','quota'}:
            if not isinstance(blocker,dict) or blocker.get('category') not in {'resource','permission','materials'} or not all(blocker.get(k) for k in ('method','result','needed')):raise TangoError('Genuine resource/material/permission blocker evidence required')
        with self.tx():
            q,r=self._valid(qid,owner,fence)
            if q['kind']=='meeting':raise TangoError('Meeting failures require lease recovery and native reconciliation; no fake completion')
            receipt=json.loads(q['native_receipt']) if q['native_receipt'] else None
            path=self._cli_receipt_path(receipt)
            if path:
                data,_=self._cli_journal(path);self._cli_terminal_journal(data)
            n=r['failures']+1
            blocked=blocked or kind in {'permission','materials','quota'}
            same=r['last_failure']==kind
            state='WAITING' if blocked else ('REPLANNING' if n>=3 or kind=='no_progress' else 'REPAIRING')
            delay=0 if blocked or kind=='no_progress' else min(300,2**min(n,8))
            self.db.execute("UPDATE requests SET state='failed',result=?,updated=? WHERE id=?",(dump({'kind':kind,'error':error,'blocker':blocker}),now(),qid))
            self.db.execute("UPDATE runs SET state=?,owner=NULL,lease_until=NULL,failures=?,last_failure=?,wait_reason=?,next_due=?,next_kind='execute',checkpoint=?,updated=? WHERE run_id=?",(state,n,kind,kind if blocked else 'retry_backoff',now()+delay,dump(checkpoint) if checkpoint is not None else r['checkpoint'],now(),r['run_id']))
            self.event(r['run_id'],'failure',{"kind":kind,"raw_error":error,"attempt":n,"same_class":same,"next_action":"await_requirement" if blocked else ('replan' if state=='REPLANNING' else 'diagnose_repair'),"delay_seconds":delay,"blocker":blocker})
            if blocked:self.db.execute("INSERT INTO prerequisites VALUES (?,?,'unresolved',?,NULL,?,?) ON CONFLICT(run_id,kind) DO UPDATE SET status='unresolved',original_evidence=excluded.original_evidence,updated=excluded.updated",(r['run_id'],kind,dump({'kind':kind,'raw_error':error,'request_id':qid,'attempt':n,'blocker':blocker}),now(),now()))
            self.emit(r['slot'],r['run_id'],state,f"{kind}: {error}; "+("Waiting for prerequisites" if blocked else "Checkpoint saved; diagnosis and reverification will follow"))
            successors=self._queue_ready(run_id=r['run_id'])
        return {"state":state,"retry_after":None if blocked else delay,"next_requests":successors}



    def _catalogue(self, scope):
        """Filesystem coverage only; relevance and correctness remain native judgments."""
        if not isinstance(scope,dict) or not isinstance(scope.get('roots'),list) or not scope['roots']:
            raise TangoError('Explicit nonempty project source roots required')
        excluded=scope.get('excluded',[])
        if not isinstance(excluded,list) or any(not isinstance(x,dict) or not x.get('path') or not x.get('reason') for x in excluded):raise TangoError('Each excluded path needs an explicit reason')
        roots=sorted(set(str(Path(x).absolute()) for x in scope['roots']))
        excludes={str(Path(x['path']).absolute()):x['reason'] for x in excluded}
        entries={};unavailable=[];omitted=[]
        def visit(p):
            name=str(p.absolute())
            if name in excludes:
                omitted.append({'path':name,'reason':excludes[name]});return
            if name in entries:return
            try:
                if p.is_symlink():
                    unavailable.append({'path':name,'reason':'symlink not traversed'});return
                if p.is_dir():
                    for child in sorted(p.iterdir()):visit(child)
                elif p.is_file():
                    h=hashlib.sha256();size=0
                    with p.open('rb') as f:
                        for block in iter(lambda:f.read(1024*1024),b''):h.update(block);size+=len(block)
                    entries[name]={'path':name,'sha256':h.hexdigest(),'size':size}
                else:unavailable.append({'path':name,'reason':'missing or nonregular source'})
            except OSError as e:unavailable.append({'path':name,'reason':type(e).__name__})
        for root in roots:visit(Path(root))
        return {'scope':{'roots':roots,'excluded':excluded},'sources':[entries[x] for x in sorted(entries)],
                'unavailable':sorted(unavailable,key=lambda x:x['path']),
                'excluded':sorted(omitted,key=lambda x:x['path'])}

    def _prepare_valid(self,qid,owner,fence):
        q,r=self._valid(qid,owner,fence)
        if q['kind']!='prepare_meeting' or not q['native_session']:raise TangoError('Bound actual native preparation worker required')
        m=dict(self.db.execute('SELECT * FROM meetings WHERE id=?',(json.loads(q['payload'])['meeting_id'],)).fetchone())
        if m['state']!='open' or (m['goal_version'],m['input_version'])!=(r['goal_version'],r['input_version']) or json.loads(m['snapshot'])['goal']!=self.goal(r):raise TangoError('Preparation is stale')
        return q,r,m

    def prepare_catalogue(self,qid,owner,fence,scope):
        with self.tx():
            self._prepare_valid(qid,owner,fence)
            catalogue=self._catalogue(scope)
            return {'catalogue':catalogue,'catalogue_sha256':sha(dump(catalogue).encode()),
                    'semantic_inventory_required':True,'complete_project_coverage_not_certified':True}

    def _summary_problem(self,m):
        seal=self.db.execute('SELECT * FROM meeting_summaries WHERE meeting_id=?',(m['id'],)).fetchone()
        if not seal:return 'No sealed inventory summary'
        try:
            data=Path(seal['path']).read_bytes()
            if sha(data)!=seal['sha256'] or data.decode()!=seal['summary_json']:return 'Sealed summary bytes changed'
            document=json.loads(seal['summary_json'])
            if document['meeting_snapshot']!=json.loads(m['snapshot']):return 'Summary snapshot mismatch'
            catalogue=self._catalogue(document['catalogue']['scope'])
            if catalogue!=document['catalogue']:return 'Source content, membership or accessibility changed'
        except (OSError,ValueError,KeyError,TangoError) as e:return 'Summary validation failed: '+str(e)
        return None

    def _preflight_summary(self,qid,owner=None,fence=None):
        # Stale fencing must COMMIT before raising. Never raise inside this transaction.
        problem=None
        with self.tx():
            q=row(self.db.execute('SELECT * FROM requests WHERE id=?',(qid,)).fetchone())
            if not q or q['kind']!='meeting' or q['state'] not in {'queued','claimed'}:return
            r=self.run(q['run_id'])
            if owner is not None:self._valid(qid,owner,fence)
            m=dict(self.db.execute('SELECT * FROM meetings WHERE id=?',(json.loads(q['payload'])['meeting_id'],)).fetchone())
            problem=self._summary_problem(m)
            if json.loads(m['snapshot'])['goal']!=self.goal(r) or (m['goal_version'],m['input_version'])!=(r['goal_version'],r['input_version']):problem='Goal/input/contract changed'
            if problem:
                self.invalidate(r['run_id'],'sealed inventory stale: '+problem)
                # Keep open so _meeting_due creates a replacement immediately when snapshot changed;
                # removing the invalid seal is forbidden. Mark rebuild explicitly through new snapshot ID.
                self.db.execute("UPDATE meetings SET state='inventory_stale' WHERE id=?",(m['id'],))
                self.event(r['run_id'],'inventory_invalidated',{'meeting_id':m['id'],'reason':problem})
                self._queue_ready(run_id=r['run_id'])
        if problem:raise TangoError('Inventory invalidated and preparation queued; reconcile native sessions: '+problem)

    def seal_meeting_summary(self,qid,owner,fence,catalogue,summary,coverage,receipt):
        required=('accepted_results','failed_routes','conditional_assumptions','open_obligations','unverified_findings')
        if not isinstance(summary,dict) or any(not isinstance(summary.get(k),list) for k in required):raise TangoError('All five inventory categories must be explicit lists')
        if not isinstance(coverage,dict) or coverage.get('claim') not in {'scoped_complete','partial'} or not coverage.get('search_method') or not isinstance(coverage.get('unavailable'),list) or not isinstance(coverage.get('excluded'),list) or not isinstance(coverage.get('caveats'),list):raise TangoError('Explicit scoped coverage, method, unavailable/excluded portions and caveats required')
        if not isinstance(catalogue,dict) or not catalogue.get('sources'):raise TangoError('Empty source materials cannot be sealed')
        actual=self._catalogue(catalogue['scope'])
        if actual!=catalogue:raise TangoError('Sources changed during preparation; rebuild the inventory')
        if (actual['unavailable'] or actual['excluded'] or coverage['unavailable'] or coverage['excluded']) and (coverage['claim']!='partial' or not coverage['caveats']):raise TangoError('Incomplete access requires partial coverage and explicit caveats')
        if coverage['claim']=='partial' and not coverage['caveats']:raise TangoError('Partial coverage needs explicit caveats')
        refs={x['path']:x['sha256'] for x in actual['sources']};ids=set();used=set();items=[]
        for category in required:
            for item in summary[category]:
                if not isinstance(item,dict) or not item.get('id') or not item.get('statement') or not isinstance(item.get('sources'),list) or not item['sources']:raise TangoError('Every inventory item needs stable ID, statement and source references')
                if item['id'] in ids:raise TangoError('Duplicate inventory ID; deduplicate explicitly')
                if not item.get('scope') or item.get('verification_status') not in {'independently_verified','claimed','unverified','conditional','rejected','open'}:raise TangoError('Explicit mathematical scope and verification status required')
                ids.add(item['id']);items.append(item)
                for ref in item['sources']:
                    if not isinstance(ref,dict) or refs.get(ref.get('path'))!=ref.get('sha256') or ref.get('path') not in refs:raise TangoError('Unknown or stale source reference')
                    used.add(ref['path'])
        if not items:raise TangoError('Empty substantive summary cannot be sealed')
        for item in items:
            if item.get('duplicate_of') and (item['duplicate_of'] not in ids or item['duplicate_of']==item['id']):raise TangoError('Invalid deduplication reference')
        by_id={item['id']:item for item in items}
        for item in items:
            seen=set();cursor=item
            while cursor.get('duplicate_of'):
                if cursor['id'] in seen:raise TangoError('Cyclic deduplication reference')
                seen.add(cursor['id']);cursor=by_id[cursor['duplicate_of']]
        dispositions=coverage.get('source_dispositions',[])
        if not isinstance(dispositions,list):raise TangoError('Source dispositions must be a list')
        for item in dispositions:
            if not isinstance(item,dict) or item.get('path') not in refs or not item.get('reason'):raise TangoError('Unrepresented sources need an explicit disposition')
            used.add(item['path'])
        if used!=set(refs):raise TangoError('Every accessible source must be represented or explicitly accounted for')
        with self.tx():
            q,r,m=self._prepare_valid(qid,owner,fence)
            self._cli_completed_request(q)
            if receipt.get('kind')=='codex_cli':self._cli_registered(qid,receipt)
            self._meeting_receipt(q['native_session'],receipt,completed=True)
            if self.prerequisites(r['run_id']):raise TangoError('Resolve evidenced prerequisites before sealing; partial coverage is not permission')
            if self.db.execute('SELECT 1 FROM meeting_summaries WHERE meeting_id=?',(m['id'],)).fetchone():raise TangoError('Summary already sealed; immutable')
            document={'schema_version':1,'meeting_id':m['id'],'meeting_snapshot':json.loads(m['snapshot']),
                      'catalogue':actual,'summary':summary,'coverage':coverage,
                      'provenance':{'request_id':qid,'native_session':q['native_session'],'receipt':receipt},
                      'semantic_completeness_is_native_attestation':True,'not_a_proof_verdict':True}
            data=dump(document).encode();digest=sha(data)
            dest=self.path(r['run_id'])/'artifacts'/'meeting-inventories'/m['id']/uid()/'summary.json'
            dest.parent.mkdir(parents=True,exist_ok=True)
            with dest.open('xb') as f:f.write(data);f.flush();os.fsync(f.fileno())
            self.db.execute('INSERT INTO meeting_summaries VALUES (?,1,?,?,?,?,?,?)',(m['id'],data.decode(),digest,str(dest),q['native_session'],dump(receipt),now()))
            self.db.execute("UPDATE requests SET state='returned',result=?,updated=? WHERE id=?",(dump({'summary_sha256':digest,'summary_path':str(dest)}),now(),qid))
            self.db.execute("UPDATE runs SET owner=NULL,lease_until=NULL,next_kind='execute',next_due=0,state='REPLANNING',updated=? WHERE run_id=?",(now(),r['run_id']))
            self.event(r['run_id'],'meeting_inventory_sealed',{'meeting_id':m['id'],'sha256':digest,'path':str(dest),'coverage':coverage['claim']})
            successors=self._queue_ready(run_id=r['run_id'])
            return {'sealed':True,'summary_sha256':digest,'summary_path':str(dest),'next_requests':successors,'native_discussion_started':False}

    def _calculation_count(self, run_id):
        # Candidate publication is a computation; verification is not another computation.
        # A no-progress result counts only with an actual bound executor.
        return self.db.execute("SELECT COUNT(*) FROM requests WHERE run_id=? AND kind='execute' AND native_session IS NOT NULL AND (state='returned' OR (state='failed' AND json_extract(result,'$.kind')='no_progress'))",(run_id,)).fetchone()[0]

    def configure_meetings(self, run_id, enabled=True):
        if type(enabled) is not bool:raise TangoError('enabled must be boolean')
        with self.tx():
            r=self.run(run_id)
            if r['owner'] or self.db.execute("SELECT 1 FROM requests WHERE run_id=? AND state IN ('queued','claimed')",(run_id,)).fetchone():raise TangoError('Configure at a quiescent request boundary')
            if self.db.execute("SELECT 1 FROM meetings WHERE run_id=? AND state='open'",(run_id,)).fetchone():raise TangoError('An open meeting cannot be bypassed')
            old=self.db.execute('SELECT * FROM meeting_policy WHERE run_id=?',(run_id,)).fetchone()
            baseline=old['baseline'] if old else self._calculation_count(run_id)
            self.db.execute('INSERT INTO meeting_policy VALUES (?,?,?) ON CONFLICT(run_id) DO UPDATE SET enabled=excluded.enabled',(run_id,int(enabled),baseline))
            self.event(run_id,'meeting_policy',{'enabled':enabled,'baseline':baseline})
        return {'enabled':enabled,'interval':10,'participants':5,'rounds':10,'baseline':baseline}

    def _meeting_due(self, r):
        policy=self.db.execute('SELECT * FROM meeting_policy WHERE run_id=?',(r['run_id'],)).fetchone()
        if not policy or not policy['enabled']:return None
        count=self._calculation_count(r['run_id'])
        last=self.db.execute("SELECT * FROM meetings WHERE run_id=? ORDER BY calculation_count DESC,created DESC LIMIT 1",(r['run_id'],)).fetchone()
        if last and last['state']=='open':
            if (last['goal_version'],last['input_version'])==(r['goal_version'],r['input_version']) and json.loads(last['snapshot'])['goal']==self.goal(r):
                seal=self.db.execute('SELECT 1 FROM meeting_summaries WHERE meeting_id=?',(last['id'],)).fetchone()
                if not seal or self._summary_problem(dict(last)) is None:return dict(last)
            self.db.execute("UPDATE meetings SET state='superseded' WHERE id=?",(last['id'],))
            # New instructions invalidate the discussion, not the requirement to hold it.
        elif last and last['state']=='inventory_stale':pass
        elif count-(last['calculation_count'] if last else policy['baseline'])<10:return None
        snapshot={'goal':self.goal(r),'goal_version':r['goal_version'],'input_version':r['input_version'],
                  'inputs':[dict(x) for x in self.db.execute('SELECT id,seq,body,source FROM inputs WHERE run_id=? ORDER BY seq',(r['run_id'],))],
                  'checkpoint':json.loads(r['checkpoint']),
                  'calculation_count':count,'artifact_version':r['artifact_version'],
                  'history':[dict(x) for x in self.db.execute("SELECT kind,payload,created FROM events WHERE run_id=? AND kind IN ('candidate','failure','verification_failed') ORDER BY id",(r['run_id'],))]}
        mid=uid()
        self.db.execute("INSERT INTO meetings(id,run_id,goal_version,input_version,calculation_count,snapshot,created) VALUES (?,?,?,?,?,?,?)",(mid,r['run_id'],r['goal_version'],r['input_version'],count,dump(snapshot),now()))
        self.event(r['run_id'],'meeting_started',{'meeting_id':mid,'calculation_count':count,'actual_discussion_started':False})
        return dict(self.db.execute('SELECT * FROM meetings WHERE id=?',(mid,)).fetchone())

    def _meeting_valid(self, qid, owner, fence):
        q,r=self._valid(qid,owner,fence)
        if q['kind']!='meeting' or not q['native_session']:raise TangoError('Bound native meeting coordinator required')
        mid=json.loads(q['payload'])['meeting_id']
        m=dict(self.db.execute('SELECT * FROM meetings WHERE id=?',(mid,)).fetchone())
        if m['state']!='open' or (m['goal_version'],m['input_version'])!=(r['goal_version'],r['input_version']):raise TangoError('Stale meeting')
        if json.loads(m['snapshot'])['goal']!=self.goal(r) or self._summary_problem(m):raise TangoError('Stale or missing sealed summary; repeat action to rebuild preparation')
        return q,r,m

    def _meeting_context(self,m):
        posts=[dict(x) for x in self.db.execute('SELECT round,member,session,content FROM meeting_posts WHERE meeting_id=? AND round<? ORDER BY round,member',(m['id'],m['round']))]
        seal=dict(self.db.execute('SELECT * FROM meeting_summaries WHERE meeting_id=?',(m['id'],)).fetchone())
        context={'sealed_summary':json.loads(seal['summary_json']),'summary_sha256':seal['sha256'],'summary_path':seal['path'],'meeting_id':m['id'],'round':m['round'],'snapshot':json.loads(m['snapshot']),'previous_rounds':posts,
                 'roles':['route inventory','constructive route','counterexample and obstruction','assumption audit','proof and implementation audit'],
                 'instructions':'Read every previous-round statement. Answer concrete objections, identify evidence, unresolved obligations and a next computation. New assumptions must be explicit; consensus is not proof. Do not change the original goal. No current-round peer posts are visible until all five finish.'}
        return {**context,'context_hash':sha(dump(context).encode())}

    def meeting_context(self,qid,owner,fence):
        self._preflight_summary(qid,owner,fence)
        with self.tx():
            q,r,m=self._meeting_valid(qid,owner,fence)
            result=self._meeting_context(m)
            result['submitted_members']=[x[0] for x in self.db.execute('SELECT member FROM meeting_posts WHERE meeting_id=? AND round=? ORDER BY member',(m['id'],m['round']))]
            return result

    def meeting_reserve(self,qid,owner,fence,member):
        self._preflight_summary(qid,owner,fence)
        if type(member) is not int or member not in range(1,6):raise TangoError('Exactly members 1 through 5')
        with self.tx():
            q,r,m=self._meeting_valid(qid,owner,fence)
            self.db.execute('INSERT OR IGNORE INTO meeting_members(request_id,member) VALUES (?,?)',(qid,member))
        return {'member':member,'native_task_name':qid+'_member_'+str(member),'spawn_gap_must_be_reconciled':True}

    def _meeting_receipt(self,session,receipt,completed=False):
        if not isinstance(receipt,dict) or receipt.get('agent_id')!=session:raise TangoError('Session-bound actual native tool receipt required')
        if self.simulation and receipt.get('kind')=='simulation':return
        if receipt.get('kind')=='codex_cli':
            self._cli_receipt(session,receipt,completed=completed)
            return
        if receipt.get('kind')!='native' or receipt.get('tool') not in {'collaboration.spawn_agent','collaboration.followup_task'} or receipt.get('result') is None:raise TangoError('Actual native dispatch result required; synthetic receipts are forbidden')

    def meeting_bind(self,qid,owner,fence,member,session,receipt):
        self._preflight_summary(qid,owner,fence)
        if not isinstance(session,str) or not session:raise TangoError('Session required')
        self._meeting_receipt(session,receipt)
        with self.tx():
            q,r,m=self._meeting_valid(qid,owner,fence)
            if receipt.get('kind')=='codex_cli':self._cli_registered(qid+':member:'+str(member),receipt)
            old=self.db.execute('SELECT * FROM meeting_members WHERE request_id=? AND member=?',(qid,member)).fetchone()
            if not old:raise TangoError('Reserve before spawn to make crash gaps recoverable')
            if session==q['native_session']:raise TangoError('Five separate participants, not the coordinator')
            if old['session'] and old['session']!=session:raise TangoError('Member already bound; recover/reconcile before replacement')
            if self.db.execute('SELECT 1 FROM meeting_members WHERE request_id=? AND member!=? AND session=?',(qid,member,session)).fetchone():raise TangoError('Five distinct native sessions required')
            self.db.execute('UPDATE meeting_members SET session=?,receipt=? WHERE request_id=? AND member=?',(session,dump(receipt),qid,member))
        return {'bound':True}

    def meeting_submit(self,qid,owner,fence,member,round,context_hash,content,receipt):
        self._preflight_summary(qid,owner,fence)
        required=('analysis','evidence','objections','open_obligations','next_step')
        if not isinstance(content,dict) or any(not content.get(k) for k in required):raise TangoError('Full substantive analysis, evidence, objections, open_obligations and next_step required')
        with self.tx():
            q,r,m=self._meeting_valid(qid,owner,fence)
            bound=self.db.execute('SELECT * FROM meeting_members WHERE request_id=? AND member=?',(qid,member)).fetchone()
            if not bound or not bound['session']:raise TangoError('Bound participant required')
            self._meeting_receipt(bound['session'],receipt,completed=True)
            if receipt.get('kind')=='codex_cli':self._cli_registered(qid+':member:'+str(member),receipt)
            if receipt.get('round')!=round or receipt.get('context_hash')!=context_hash:raise TangoError('Turn receipt must bind the exact round and common context')
            old=self.db.execute('SELECT * FROM meeting_posts WHERE meeting_id=? AND round=? AND member=?',(m['id'],round,member)).fetchone()
            if old:
                if old['content']==dump(content) and old['context_hash']==context_hash and old['session']==bound['session']:return {'duplicate':True,'round':m['round']}
                raise TangoError('Conflicting duplicate statement')
            if round!=m['round'] or round>10 or context_hash!=self._meeting_context(m)['context_hash']:raise TangoError('Out-of-round or stale context')
            if self.db.execute('SELECT COUNT(*) FROM meeting_members WHERE request_id=? AND session IS NOT NULL',(qid,)).fetchone()[0]!=5:raise TangoError('Bind all five participants before accepting discussion')
            self.db.execute('INSERT INTO meeting_posts VALUES (?,?,?,?,?,?,?,?)',(m['id'],round,member,bound['session'],context_hash,dump(content),dump(receipt),now()))
            if receipt.get('kind')=='codex_cli':self.db.execute('UPDATE meeting_members SET receipt=? WHERE request_id=? AND member=?',(dump(receipt),qid,member))
            n=self.db.execute('SELECT COUNT(*) FROM meeting_posts WHERE meeting_id=? AND round=?',(m['id'],round)).fetchone()[0]
            if n==5:
                self.db.execute('UPDATE meetings SET round=round+1 WHERE id=?',(m['id'],))
                self.event(r['run_id'],'meeting_round_completed',{'meeting_id':m['id'],'round':round,'participants':5})
            return {'accepted':True,'round_complete':n==5,'next_round':round+1 if n==5 and round<10 else None,'decision_ready':n==5 and round==10}

    def meeting_decide(self,qid,owner,fence,decision,terminal_receipts):
        self._preflight_summary(qid,owner,fence)
        required=('direction','rationale','next_steps','expected_artifacts','open_obligations','rejected_routes','dissent','stop_conditions')
        if not isinstance(decision,dict) or any(not decision.get(k) for k in required):raise TangoError('Complete actionable direction and dissent/obligation ledger required')
        if decision.get('original_goal_preserved') is not True:raise TangoError('Original goal must be preserved; goal revisions require user input')
        with self.tx():
            q,r,m=self._meeting_valid(qid,owner,fence)
            self._cli_completed_request(q)
            if m['round']!=11 or self.db.execute('SELECT COUNT(*) FROM meeting_posts WHERE meeting_id=?',(m['id'],)).fetchone()[0]!=50:raise TangoError('All 50 contributions across 10 synchronized rounds are required')
            members=self.db.execute('SELECT * FROM meeting_members WHERE request_id=? ORDER BY member',(qid,)).fetchall()
            if len(members)!=5 or not isinstance(terminal_receipts,dict):raise TangoError('All five participants must be reconciled before continuation')
            for member in members:
                receipt=terminal_receipts.get(str(member['member']))
                if not isinstance(receipt,dict) or receipt.get('agent_id')!=member['session']:raise TangoError('Participant terminal receipt mismatch')
                if self.simulation and receipt.get('kind')=='simulation':continue
                if receipt.get('kind')=='codex_cli':
                    if receipt.get('status')!='completed':raise TangoError('CLI participant must have completed successfully')
                    self._cli_registered(qid+':member:'+str(member['member']),receipt)
                    self._cli_receipt(member['session'],receipt,completed=True,terminal=True)
                    continue
                if receipt.get('kind')!='native' or receipt.get('tool')!='collaboration.list_agents' or receipt.get('status') not in {'completed','finished','done'} or receipt.get('result') is None:raise TangoError('Actual participant completion evidence required')
            decision={**decision,'meeting_id':m['id'],'calculation_count':m['calculation_count'],'not_a_proof_verdict':True}
            self.db.execute("UPDATE meetings SET state='decided',decision=? WHERE id=?",(dump(decision),m['id']))
            self.db.execute("UPDATE requests SET state='returned',result=?,updated=? WHERE id=?",(dump(decision),now(),qid))
            self.db.execute("UPDATE runs SET owner=NULL,lease_until=NULL,state='REPLANNING',next_kind='execute',next_due=0,wait_reason=NULL,updated=? WHERE run_id=?",(now(),r['run_id']))
            self.event(r['run_id'],'meeting_decided',decision)
            successors=self._queue_ready(run_id=r['run_id'])
            return {'meeting_complete':True,'next_requests':successors,'native_parent_required':True,'local_can_wake_native':False}

    def effect(self,qid,owner,fence,key,specification,receipt=None):
        with self.tx():
            q,r=self._valid(qid,owner,fence)
            e=row(self.db.execute("SELECT * FROM effects WHERE run_id=? AND effect_key=?",(r['run_id'],key)).fetchone())
            if e and e['specification']!=dump(specification): raise TangoError("Side-effect identity conflict")
            if e and not receipt:return {**e,"action":"do_not_repeat; reconcile external outcome"}
            if receipt:
                if not e:raise TangoError("Record intent before side effect")
                self.db.execute("UPDATE effects SET state='confirmed',receipt=?,updated=? WHERE run_id=? AND effect_key=?",(dump(receipt),now(),r['run_id'],key))
                return {"action":"confirmed"}
            if any(x['kind']=='permission' for x in self.prerequisites(r['run_id'])):raise TangoError('Permission prerequisite remains unresolved; no new side-effect intent may be started')
            self.db.execute("INSERT INTO effects VALUES (?,?,?,'intent',NULL,?,?)",(r['run_id'],key,dump(specification),now(),now()))
            return {"action":"intent_recorded; authorization still required by native policy"}

    def outbox(self,claim_owner=None,lease_seconds=120,slot=None,run_id=None,current_only=False,ids=None):
        if not 0<lease_seconds<=3600:raise TangoError('Invalid delivery lease duration')
        scope,params=self._scope(slot,run_id,'outbox.')
        if current_only:scope+=' AND (outbox.run_id IS NULL OR outbox.run_id=(SELECT current_run FROM slots WHERE slot=outbox.slot))'
        if ids is not None:
            if not isinstance(ids,list) or any(not isinstance(i,str) for i in ids):raise TangoError('ids must be a list of outbox IDs')
            if not ids:return []
            scope+=' AND outbox.id IN ('+','.join('?' for _ in ids)+')';params.extend(ids)
        with self.tx():
            self.db.execute("UPDATE outbox SET status='unknown_delivery',last_error='Delivery claim expired; reconcile before any retry',delivery_owner=NULL,claim_until=NULL WHERE status='sending' AND claim_until<=?"+scope,(now(),*params))
            rows=[dict(x) for x in self.db.execute("SELECT * FROM outbox WHERE status='pending'"+scope+" ORDER BY created,id",params)]
            for x in rows:
                s=self.db.execute("SELECT current_run FROM slots WHERE slot=?",(x['slot'],)).fetchone()[0]
                x['stale_run']=x['run_id'] is not None and x['run_id']!=s
                if x['stale_run']: x['text']+=" [historical run message]"
                if claim_owner:
                    self.db.execute("UPDATE outbox SET status='sending',delivery_owner=?,claim_until=?,attempts=attempts+1,updated=? WHERE id=?",(claim_owner,now()+lease_seconds,now(),x['id']))
                    x['delivery_owner']=claim_owner;x['status']='sending'
            return rows

    def ack(self,outbox_id,owner,receipt=None,error=None,confirmed_not_sent=False):
        if not receipt and not error:raise TangoError("Actual delivery receipt or error required")
        with self.tx():
            x=row(self.db.execute("SELECT * FROM outbox WHERE id=?",(outbox_id,)).fetchone())
            if not x:raise TangoError("Unknown outbox item")
            if x['status']=='delivered':return {"duplicate":True}
            if x['status']!='sending' or x['delivery_owner']!=owner or x['claim_until']<=now():raise TangoError("Delivery claim expired or wrong owner")
            state='delivered' if receipt else ('pending' if confirmed_not_sent else 'unknown_delivery')
            self.db.execute("UPDATE outbox SET status=?,receipt=?,last_error=?,delivery_owner=NULL,claim_until=NULL,updated=? WHERE id=?",(state,dump(receipt) if receipt else None,error,now(),outbox_id))
        return {"delivered":bool(receipt)}

    def delivery_issues(self,slot=None,run_id=None):
        scope,params=self._scope(slot,run_id)
        return [dict(x) for x in self.db.execute("SELECT * FROM outbox WHERE status='unknown_delivery'"+scope+" ORDER BY created",params)]

    def reconcile_delivery(self,outbox_id,evidence,delivered):
        if not isinstance(evidence,dict) or not evidence.get('method') or not evidence.get('result'):raise TangoError('Actual channel lookup/idempotency evidence required')
        with self.tx():
            x=self.db.execute('SELECT * FROM outbox WHERE id=?',(outbox_id,)).fetchone()
            if not x or x['status']!='unknown_delivery':raise TangoError('Outbox item is not awaiting reconciliation')
            if not delivered and not evidence.get('confirmed_not_sent'):raise TangoError('Uncertain delivery cannot be retried')
            self.db.execute('UPDATE outbox SET status=?,receipt=?,last_error=NULL,updated=? WHERE id=?',('delivered' if delivered else 'pending',dump(evidence),now(),outbox_id))
            self.event(x['run_id'],'delivery_reconciled',{'outbox_id':outbox_id,'delivered':delivered,'evidence':evidence})
        return {'delivered':delivered,'retry_allowed':not delivered}

    def standby(self):
        with self.tx():
            if self.db.execute("SELECT 1 FROM runs r JOIN slots s ON s.current_run=r.run_id WHERE r.state NOT IN ('STOPPED','COMPLETED')").fetchone(): raise TangoError("Stop/complete all current runs before standby")
            self.db.execute("UPDATE slots SET current_run=NULL")
        return self.status()

    def export(self,run_id):
        r=self.run(run_id)
        return {"run":r,
                "meeting_summaries":[dict(x) for x in self.db.execute("SELECT s.* FROM meeting_summaries s JOIN meetings m ON m.id=s.meeting_id WHERE m.run_id=? ORDER BY s.created",(run_id,))],"meetings":[dict(x) for x in self.db.execute('SELECT * FROM meetings WHERE run_id=? ORDER BY created',(run_id,))],
                "meeting_policy":row(self.db.execute('SELECT * FROM meeting_policy WHERE run_id=?',(run_id,)).fetchone()),
                "meeting_members":[dict(x) for x in self.db.execute('SELECT m.* FROM meeting_members m JOIN requests q ON q.id=m.request_id WHERE q.run_id=? ORDER BY m.request_id,m.member',(run_id,))],
                "meeting_posts":[dict(x) for x in self.db.execute('SELECT p.* FROM meeting_posts p JOIN meetings m ON m.id=p.meeting_id WHERE m.run_id=? ORDER BY m.created,p.round,p.member',(run_id,))],
                "goals":[dict(x) for x in self.db.execute("SELECT * FROM goals WHERE run_id=? ORDER BY version",(run_id,))],"inputs":[dict(x) for x in self.db.execute("SELECT * FROM inputs WHERE run_id=? ORDER BY seq",(run_id,))],"events":[dict(x) for x in self.db.execute("SELECT * FROM events WHERE run_id=? ORDER BY id",(run_id,))],"requests":[dict(x) for x in self.db.execute("SELECT * FROM requests WHERE run_id=? ORDER BY created",(run_id,))],"outbox":[dict(x) for x in self.db.execute("SELECT * FROM outbox WHERE run_id=? ORDER BY created",(run_id,))],"effects":[dict(x) for x in self.db.execute('SELECT * FROM effects WHERE run_id=? ORDER BY created',(run_id,))],"prerequisites":[dict(x) for x in self.db.execute('SELECT * FROM prerequisites WHERE run_id=? ORDER BY created',(run_id,))]}

def daemon(b,interval,lifecycle='retained_foreground'):
    import fcntl
    lock=open(b.home/'scheduler.lock','a+')
    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:raise TangoError('Scheduler already running')
    stop=False
    def end(*_):
        nonlocal stop
        stop=True
    signal.signal(signal.SIGTERM,end);signal.signal(signal.SIGINT,end)
    pidfile=b.home/'scheduler.json'
    start_ticks=Path(f'/proc/{os.getpid()}/stat').read_text().split()[21]
    pid_namespace=os.readlink('/proc/self/ns/pid');instance=uid()
    while not stop:
        result=b.tick()
        status={'pid':os.getpid(),'process_start_ticks':start_ticks,'pid_namespace':pid_namespace,'instance_id':instance,'lifecycle':lifecycle,'heartbeat':now(),'version':VERSION,'mode':'queue-only native-parent broker','local_can_wake_native':False,'survives_owning_exec_end':False,**result}
        temp=pidfile.with_suffix('.tmp');temp.write_text(dump(status));os.replace(temp,pidfile)
        time.sleep(interval)
    status['stopped']=now();pidfile.write_text(dump(status))

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--home',default=DEFAULT_HOME);p.add_argument('--version',action='version',version=VERSION)
    sp=p.add_subparsers(dest='cmd',required=True)
    for name in ('init','status','standby','doctor'):sp.add_parser(name)
    for name in ('tick','pending','recover','cancellations','delivery_issues','parent-actions'):
        a=sp.add_parser(name);a.add_argument('--slot',type=int,choices=[1,2,3]);a.add_argument('--run-id')
    a=sp.add_parser('route');a.add_argument('--id',required=True);a.add_argument('--text',required=True);a.add_argument('--source',choices=['user','test_injection'],required=True);a.add_argument('--nested',action='store_true')
    a=sp.add_parser('call',help='Typed JSON method call from stdin');a.add_argument('method',choices=['set_contract','revise_goal','claim','register_cli_attempt','bind_native','heartbeat','candidate','verify','fail','effect','ack','export','ack_cancel','ack_cli_not_started','ack_cli_unbound','ack_cli_unlaunched','interrupt_cli_request','reconcile_delivery','reconcile_spawn','recheck_prerequisite','wait_for_prerequisite','parent_heartbeat','parent_release','configure_meetings','prepare_catalogue','seal_meeting_summary','meeting_context','meeting_reserve','meeting_bind','meeting_submit','meeting_decide'])
    a=sp.add_parser('outbox');a.add_argument('--claim-owner');a.add_argument('--slot',type=int,choices=[1,2,3]);a.add_argument('--run-id');a.add_argument('--current-only',action='store_true');a.add_argument('--id',dest='ids',action='append')
    a=sp.add_parser('daemon');a.add_argument('--interval',type=float,default=2);a.add_argument('--lifecycle',choices=['retained_foreground','detached_within_current_exec'],default='retained_foreground')
    a=sp.add_parser('service-status')
    args=p.parse_args();b=Broker(args.home)
    try:
        if args.cmd=='route':result=b.route(args.id,args.text,args.source,not args.nested)
        elif args.cmd=='init':result={'version':VERSION,'home':str(b.home),'slots':b.status(),'adapter':'native_parent'}
        elif args.cmd=='call':result=getattr(b,args.method)(**json.load(sys.stdin))
        elif args.cmd=='outbox':result=b.outbox(args.claim_owner,slot=args.slot,run_id=args.run_id,current_only=args.current_only,ids=args.ids)
        elif args.cmd in ('tick','pending','recover','cancellations','delivery_issues','parent-actions'):result=getattr(b,args.cmd.replace('-','_'))(slot=args.slot,run_id=args.run_id)
        elif args.cmd=='daemon':daemon(b,args.interval,args.lifecycle);return
        elif args.cmd=='service-status':
            f=b.home/'scheduler.json';result=json.loads(f.read_text()) if f.exists() else {'running':False}
            # Keep one namespace-aware status implementation for both entrypoints.
            from service import read_status
            result=read_status(b.home)
        elif args.cmd=='doctor':result={'version':VERSION,'python':sys.version,'home':str(b.home),'sqlite_integrity':b.db.execute('PRAGMA integrity_check').fetchone()[0],'adapter':'native_parent','autonomous_entrypoint':'program/autonomous.py','autonomous_status_is_separate':True,'external_model_clients':False,'local_can_wake_native':False,'scheduler_lifecycle':'retained foreground exec session only; detached child does not survive owning exec','automatic_scheduler_restart':False,'vm_replacement_persistence':'NOT_GUARANTEED','ready':False,'blocking_gates':['local_to_native_ingress','autonomous_background_continuity','authenticated_user_prefix_e2e','native_full_acceptance']}
        else:result=getattr(b,args.cmd)()
        print(dump(result))
    except (TangoError,ValueError,KeyError,TypeError) as e:
        print(dump({'error':str(e),'type':type(e).__name__}),file=sys.stderr);raise SystemExit(2)

if __name__=='__main__':main()
