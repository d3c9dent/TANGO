#!/usr/bin/env python3
"""TANGO: durable, native-parent-mediated task broker. No model/network client."""
from __future__ import annotations
import argparse, contextlib, hashlib, json, os, re, signal, sqlite3, sys, time, uuid
from pathlib import Path

VERSION = "0.1.3-native-broker"
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
            # Additive recovery of blockers created by the original schema; no goal/state is weakened.
            for r in self.db.execute("SELECT run_id,wait_reason FROM runs WHERE wait_reason IS NOT NULL AND wait_reason!='retry_backoff'").fetchall():
                e=self.db.execute("SELECT payload FROM events WHERE run_id=? AND kind='failure' ORDER BY id DESC LIMIT 1",(r['run_id'],)).fetchone()
                self.db.execute("INSERT OR IGNORE INTO prerequisites VALUES (?,?,'unresolved',?,NULL,?,?)",(r['run_id'],r['wait_reason'],e[0] if e else dump({'kind':r['wait_reason'],'source':'retained scheduler blocker'}),now(),now()))

    @contextlib.contextmanager
    def tx(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def event(self, run, kind, payload):
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
            if q['kind']!='execute' or not q['native_session']:raise TangoError('Bound native executor required for prerequisite reevaluation')
            if not self.db.execute('SELECT 1 FROM prerequisites WHERE run_id=? AND kind=?',(r['run_id'],kind)).fetchone():raise TangoError('Unknown prerequisite')
            self.db.execute('UPDATE prerequisites SET status=?,recheck_evidence=?,updated=? WHERE run_id=? AND kind=?',('resolved' if available else 'unresolved',dump(evidence),now(),r['run_id'],kind))
            self.event(r['run_id'],'prerequisite_rechecked',{'kind':kind,'available':available,'evidence':evidence,'native_session':q['native_session']})
        return {'kind':kind,'available':available,'unresolved':self.prerequisites(r['run_id'])}

    def recover(self):
        recovered=[]
        with self.tx():
            for r0 in self.db.execute("SELECT * FROM runs WHERE owner IS NOT NULL AND lease_until<=?",(now(),)).fetchall():
                r=dict(r0);self.invalidate(r['run_id'],"lease expired; old native session must not publish")
                if r['state'] in ACTIVE:
                    self.db.execute("UPDATE runs SET state='REPAIRING',next_due=0 WHERE run_id=?",(r['run_id'],))
                    self.emit(r['slot'],r['run_id'],'REPAIRING',"Execution lease expired; previous executor fenced and checkpoint retained for native adapter recovery")
                recovered.append(r['run_id'])
                self.event(r['run_id'],"recovered",{"old_owner":r['owner'],"old_fence":r['fence'],"checkpoint":json.loads(r['checkpoint'])})
            self.db.execute("UPDATE outbox SET status='unknown_delivery',last_error='Delivery claim expired; reconcile before any retry',delivery_owner=NULL,claim_until=NULL WHERE status='sending' AND claim_until<=?",(now(),))
        return recovered

    def tick(self):
        self.recover();made=[]
        with self.tx():
            rr=self.db.execute("SELECT r.* FROM runs r JOIN slots s ON s.current_run=r.run_id WHERE r.state IN ('RUNNING','VERIFYING','REPAIRING','REPLANNING','WAITING') AND r.owner IS NULL AND r.next_due<=? AND (r.wait_reason IS NULL OR r.wait_reason='retry_backoff') ORDER BY r.updated,r.slot",(now(),)).fetchall()
            for z in rr:
                r=dict(z)
                if self.db.execute("SELECT 1 FROM requests WHERE run_id=? AND state IN ('queued','claimed')",(r['run_id'],)).fetchone(): continue
                kind=r['next_kind'];g=self.goal(r)
                # Verification receives no generator conversation or private execution context.
                payload={"protocol":"tango.native-parent.v1","kind":kind,"run_id":r['run_id'],"slot":r['slot'],"prefix":f"\\TANGO_TASK_{r['slot']}","goal":g,"input_version":r['input_version'],"artifact_version":r['artifact_version'],"workdir":str(self.path(r['run_id'])/'work'),"constraints":{"native_only":True,"max_active_slots":3,"no_external_models":True},"inputs":[dict(x) for x in self.db.execute("SELECT id,seq,body,source FROM inputs WHERE run_id=? ORDER BY seq",(r['run_id'],))]}
                payload['unresolved_prerequisites']=self.prerequisites(r['run_id'])
                if payload['unresolved_prerequisites']:
                    payload['prerequisite_reevaluation']={'required':True,'input_is_not_authorization':True,'instructions':'Recheck each prerequisite using actual materials, availability or valid permission evidence. Do not perform dependent operations until met. Record recheck_prerequisite evidence. If unmet, retain WAITING with fail(blocked=True); independent safe steps may continue and be checkpointed.'}
                if kind=='execute':
                    payload['checkpoint']=json.loads(r['checkpoint'])
                    payload['side_effects']=[dict(x) for x in self.db.execute('SELECT * FROM effects WHERE run_id=? ORDER BY created,effect_key',(r['run_id'],))]
                    payload['latest_failure']=row(self.db.execute("SELECT kind,payload,created FROM events WHERE run_id=? AND kind IN ('failure','verification_failed') ORDER BY id DESC LIMIT 1",(r['run_id'],)).fetchone())
                else:
                    a=self.db.execute("SELECT manifest FROM artifacts WHERE run_id=? AND version=?",(r['run_id'],r['artifact_version'])).fetchone()
                    payload['artifacts']=json.loads(a[0]) if a else []
                    payload['producer_session']=r['producer_session']
                    payload['independent_context_required']=True
                q=uid();self.db.execute("INSERT INTO requests(id,run_id,kind,state,goal_version,input_version,artifact_version,payload,created,updated) VALUES (?,?,?,'queued',?,?,?,?,?,?)",(q,r['run_id'],kind,r['goal_version'],r['input_version'],r['artifact_version'],dump(payload),now(),now()))
                self.event(r['run_id'],'dispatch_queued',{"request_id":q,"kind":kind,"native_execution_started":False});made.append(q)
        return {"queued":made,"adapter":"native_parent","local_can_wake_native":False}

    def pending(self):
        return [{**dict(r),"payload":json.loads(r['payload'])} for r in self.db.execute("SELECT * FROM requests WHERE state='queued' ORDER BY created,id")]

    def claim(self,request_id,owner,lease_seconds=300):
        if not owner or not 0<lease_seconds<=3600: raise TangoError("Owner and 0<lease_seconds<=3600 required")
        with self.tx():
            q=row(self.db.execute("SELECT * FROM requests WHERE id=?",(request_id,)).fetchone())
            if not q or q['state']!='queued': raise TangoError("Request is not queued")
            r=self.run(q['run_id'])
            if r['state'] not in ACTIVE or r['owner'] is not None: raise TangoError("Run cannot be claimed")
            if self.db.execute('SELECT 1 FROM cancellations c JOIN runs r ON r.run_id=c.run_id WHERE r.slot=? AND c.receipt IS NULL',(r['slot'],)).fetchone():raise TangoError('Previous native session in this slot must be confirmed interrupted/finished before a replacement starts')
            occupied=self.db.execute("SELECT COUNT(*) FROM requests WHERE state='claimed'").fetchone()[0]+self.db.execute("SELECT COUNT(*) FROM cancellations WHERE receipt IS NULL").fetchone()[0]
            if occupied>=3: raise TangoError("Three-slot native capacity occupied; acknowledge stopped/finished native sessions first")
            f=r['fence']+1;t=now()
            self.db.execute("UPDATE runs SET owner=?,fence=?,lease_until=?,state=?,wait_reason=NULL,updated=? WHERE run_id=?",(owner,f,t+lease_seconds,'VERIFYING' if q['kind']=='verify' else 'RUNNING',t,r['run_id']))
            self.db.execute("UPDATE requests SET owner=?,fence=?,state='claimed',updated=? WHERE id=?",(owner,f,t,request_id))
            self.event(r['run_id'],'claimed',{"request_id":request_id,"owner":owner,"fence":f})
            return {"request_id":request_id,"owner":owner,"fence":f,"lease_until":t+lease_seconds,"native_task_name":"tango_"+q['kind']+"_"+request_id,"payload":json.loads(q['payload'])}

    def bind_native(self,request_id,owner,fence,session,receipt):
        if not session or not receipt: raise TangoError("Real native session ID and tool receipt required")
        if not isinstance(receipt,dict) or receipt.get('agent_id')!=session:raise TangoError('Receipt agent_id must match native session')
        if receipt.get('kind')!='native' and not (self.simulation and receipt.get('kind')=='simulation'):raise TangoError('Production adapter accepts native receipts only')
        if receipt.get('kind')=='native' and receipt.get('tool') not in {'collaboration.spawn_agent','collaboration.followup_task'}:raise TangoError('Unsupported native execution tool')
        with self.tx():
            q,r=self._valid(request_id,owner,fence)
            if q['kind']=='verify' and session==r['producer_session']: raise TangoError("Verifier must use a separate native session")
            if q['native_session'] and q['native_session']!=session: raise TangoError("Native session already bound")
            self.db.execute("UPDATE requests SET native_session=?,native_receipt=?,updated=? WHERE id=?",(session,dump(receipt),now(),request_id))
            self.event(r['run_id'],'native_bound',{"request_id":request_id,"session":session,"receipt":receipt})
        return {"ok":True}

    def cancellations(self):
        return [dict(x) for x in self.db.execute('SELECT * FROM cancellations WHERE receipt IS NULL ORDER BY created')]

    def reconcile_spawn(self,request_id,session,receipt):
        if not isinstance(receipt,dict) or receipt.get('kind')!='native' or receipt.get('tool')!='collaboration.list_agents' or receipt.get('agent_id')!=session or receipt.get('result') is None:raise TangoError('Native lookup evidence identifying the orphan session is required')
        with self.tx():
            c=self.db.execute('SELECT * FROM cancellations WHERE request_id=?',(request_id,)).fetchone()
            if not c or c['receipt'] is not None or c['native_session']!='unbound:'+request_id:raise TangoError('No unresolved spawn gap for this request')
            self.db.execute('UPDATE cancellations SET native_session=? WHERE request_id=?',(session,request_id))
            self.db.execute('UPDATE requests SET native_session=?,native_receipt=? WHERE id=?',(session,dump(receipt),request_id))
            self.event(c['run_id'],'orphan_native_identified',{'request_id':request_id,'session':session,'receipt':receipt})
        return {'ok':True,'next_action':'interrupt or establish terminal state, then ack_cancel'}

    def ack_cancel(self,request_id,receipt):
        if not receipt:raise TangoError('Actual native interrupt/finished receipt required')
        with self.tx():
            c=self.db.execute('SELECT * FROM cancellations WHERE request_id=?',(request_id,)).fetchone()
            if not c:raise TangoError('Unknown cancellation')
            if not isinstance(receipt,dict) or receipt.get('agent_id')!=c['native_session']:raise TangoError('Cancellation receipt session mismatch')
            if receipt.get('kind')=='simulation' and self.simulation:pass
            elif receipt.get('kind')=='native' and receipt.get('tool')=='collaboration.interrupt_agent' and receipt.get('result') is not None:pass
            elif receipt.get('kind')=='native' and receipt.get('tool')=='collaboration.list_agents' and receipt.get('status') in {'completed','finished','done','interrupted','failed'} and receipt.get('result') is not None:pass
            elif c['native_session'].startswith('unbound:') and receipt.get('kind')=='native' and receipt.get('tool')=='collaboration.list_agents' and receipt.get('status')=='not_found' and receipt.get('request_id')==request_id and receipt.get('result') is not None:pass
            else:raise TangoError('Confirmed native interrupt/terminal-state receipt required')
            self.db.execute('UPDATE cancellations SET receipt=? WHERE request_id=? AND receipt IS NULL',(dump(receipt),request_id))
        return {'ok':True}

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
        return {"artifact_version":version,"manifest":manifest,"message":text,"completed":False,"unresolved_prerequisites":blockers}

    def verify(self,qid,owner,fence,report):
        with self.tx():
            q,r=self._valid(qid,owner,fence)
            if q['kind']!='verify' or not q['native_session'] or q['native_session']==r['producer_session']: raise TangoError("Independent native verifier required")
            if report.get('goal_version')!=r['goal_version'] or report.get('input_version')!=r['input_version'] or report.get('artifact_version')!=r['artifact_version']: raise TangoError("Verdict version binding mismatch")
            criteria=self.goal(r)['criteria']; expected={c['id'] for c in criteria}
            items=report.get('checks',[]); got=[x.get('id') for x in items]
            manifest=json.loads(self.db.execute("SELECT manifest FROM artifacts WHERE run_id=? AND version=?",(r['run_id'],r['artifact_version'])).fetchone()[0])
            integrity=all(Path(a['path']).is_file() and sha(Path(a['path']).read_bytes())==a['sha256'] for a in manifest)
            passed=bool(criteria) and set(got)==expected and len(got)==len(expected) and all(x.get('passed') is True and x.get('evidence') for x in items) and not report.get('missing') and integrity and not self.prerequisites(r['run_id'])
            report={**report,"integrity_passed":integrity,"criteria_complete":bool(criteria) and set(got)==expected,"accepted":passed}
            vid=uid();self.db.execute("INSERT INTO verifications VALUES (?,?,?,?,?,?,?,?,?)",(vid,r['run_id'],r['goal_version'],r['input_version'],r['artifact_version'],q['native_session'],dump(report),int(passed),now()))
            self.db.execute("UPDATE requests SET state='returned',result=?,updated=? WHERE id=?",(dump(report),now(),qid))
            count=0 if passed else r['failures']+1;state='COMPLETED' if passed else ('REPLANNING' if count>=3 else 'REPAIRING')
            self.db.execute("UPDATE runs SET state=?,owner=NULL,lease_until=NULL,next_kind='execute',next_due=0,failures=?,last_failure=?,updated=? WHERE run_id=?",(state,count,None if passed else 'verification',now(),r['run_id']))
            self.event(r['run_id'],'verification_passed' if passed else 'verification_failed',{"verification_id":vid,"report":report})
            self.emit(r['slot'],r['run_id'],state,"All acceptance criteria are supported by evidence" if passed else "Verification failed; goal retained for repair or replanning against failed checks")
        return {"passed":passed,"state":state,"verification_id":vid,"report":report}

    def fail(self,qid,owner,fence,kind,error,checkpoint=None,blocked=False):
        if kind not in FAILURES: raise TangoError("Unknown failure class")
        with self.tx():
            q,r=self._valid(qid,owner,fence);n=r['failures']+1
            blocked=blocked or kind in {'permission','materials','quota'}
            same=r['last_failure']==kind
            state='WAITING' if blocked else ('REPLANNING' if n>=3 or kind=='no_progress' else 'REPAIRING')
            delay=0 if blocked else min(300,2**min(n,8))
            self.db.execute("UPDATE requests SET state='failed',result=?,updated=? WHERE id=?",(dump({'kind':kind,'error':error}),now(),qid))
            self.db.execute("UPDATE runs SET state=?,owner=NULL,lease_until=NULL,failures=?,last_failure=?,wait_reason=?,next_due=?,next_kind='execute',checkpoint=?,updated=? WHERE run_id=?",(state,n,kind,kind if blocked else 'retry_backoff',now()+delay,dump(checkpoint) if checkpoint is not None else r['checkpoint'],now(),r['run_id']))
            self.event(r['run_id'],'failure',{"kind":kind,"raw_error":error,"attempt":n,"same_class":same,"next_action":"await_requirement" if blocked else ('replan' if state=='REPLANNING' else 'diagnose_repair'),"delay_seconds":delay})
            if blocked:self.db.execute("INSERT INTO prerequisites VALUES (?,?,'unresolved',?,NULL,?,?) ON CONFLICT(run_id,kind) DO UPDATE SET status='unresolved',original_evidence=excluded.original_evidence,updated=excluded.updated",(r['run_id'],kind,dump({'kind':kind,'raw_error':error,'request_id':qid,'attempt':n}),now(),now()))
            self.emit(r['slot'],r['run_id'],state,f"{kind}: {error}; "+("Waiting for prerequisites" if blocked else "Checkpoint saved; diagnosis and reverification will follow"))
        return {"state":state,"retry_after":None if blocked else delay}

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

    def outbox(self,claim_owner=None,lease_seconds=120):
        with self.tx():
            self.db.execute("UPDATE outbox SET status='unknown_delivery',last_error='Delivery claim expired; reconcile before any retry',delivery_owner=NULL,claim_until=NULL WHERE status='sending' AND claim_until<=?",(now(),))
            rows=[dict(x) for x in self.db.execute("SELECT * FROM outbox WHERE status='pending' ORDER BY created,id")]
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

    def delivery_issues(self):
        return [dict(x) for x in self.db.execute("SELECT * FROM outbox WHERE status='unknown_delivery' ORDER BY created")]

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
        return {"run":r,"goals":[dict(x) for x in self.db.execute("SELECT * FROM goals WHERE run_id=? ORDER BY version",(run_id,))],"inputs":[dict(x) for x in self.db.execute("SELECT * FROM inputs WHERE run_id=? ORDER BY seq",(run_id,))],"events":[dict(x) for x in self.db.execute("SELECT * FROM events WHERE run_id=? ORDER BY id",(run_id,))],"requests":[dict(x) for x in self.db.execute("SELECT * FROM requests WHERE run_id=? ORDER BY created",(run_id,))],"outbox":[dict(x) for x in self.db.execute("SELECT * FROM outbox WHERE run_id=? ORDER BY created",(run_id,))],"effects":[dict(x) for x in self.db.execute('SELECT * FROM effects WHERE run_id=? ORDER BY created',(run_id,))],"prerequisites":[dict(x) for x in self.db.execute('SELECT * FROM prerequisites WHERE run_id=? ORDER BY created',(run_id,))]}

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
    for name in ('init','status','tick','pending','recover','standby','doctor','cancellations','delivery_issues'):sp.add_parser(name)
    a=sp.add_parser('route');a.add_argument('--id',required=True);a.add_argument('--text',required=True);a.add_argument('--source',choices=['user','test_injection'],required=True);a.add_argument('--nested',action='store_true')
    a=sp.add_parser('call',help='Typed JSON method call from stdin');a.add_argument('method',choices=['set_contract','revise_goal','claim','bind_native','heartbeat','candidate','verify','fail','effect','ack','export','ack_cancel','reconcile_delivery','reconcile_spawn','recheck_prerequisite'])
    a=sp.add_parser('outbox');a.add_argument('--claim-owner')
    a=sp.add_parser('daemon');a.add_argument('--interval',type=float,default=2);a.add_argument('--lifecycle',choices=['retained_foreground','detached_within_current_exec'],default='retained_foreground')
    a=sp.add_parser('service-status')
    args=p.parse_args();b=Broker(args.home)
    try:
        if args.cmd=='route':result=b.route(args.id,args.text,args.source,not args.nested)
        elif args.cmd=='init':result={'version':VERSION,'home':str(b.home),'slots':b.status(),'adapter':'native_parent'}
        elif args.cmd=='call':result=getattr(b,args.method)(**json.load(sys.stdin))
        elif args.cmd=='outbox':result=b.outbox(args.claim_owner)
        elif args.cmd=='daemon':daemon(b,args.interval,args.lifecycle);return
        elif args.cmd=='service-status':
            f=b.home/'scheduler.json';result=json.loads(f.read_text()) if f.exists() else {'running':False}
            # Keep one namespace-aware status implementation for both entrypoints.
            from service import read_status
            result=read_status(b.home)
        elif args.cmd=='doctor':result={'version':VERSION,'python':sys.version,'home':str(b.home),'sqlite_integrity':b.db.execute('PRAGMA integrity_check').fetchone()[0],'adapter':'native_parent','external_model_clients':False,'local_can_wake_native':False,'scheduler_lifecycle':'retained foreground exec session only; detached child does not survive owning exec','automatic_scheduler_restart':False,'vm_replacement_persistence':'NOT_GUARANTEED','ready':False,'blocking_gates':['local_to_native_ingress','autonomous_background_continuity','authenticated_user_prefix_e2e','native_full_acceptance']}
        else:result=getattr(b,args.cmd)()
        print(dump(result))
    except (TangoError,ValueError,KeyError,TypeError) as e:
        print(dump({'error':str(e),'type':type(e).__name__}),file=sys.stderr);raise SystemExit(2)

if __name__=='__main__':main()
