#!/usr/bin/env python3
"""Offline, idle-safe, code-only TANGO installer. Never imports the broker."""
from __future__ import annotations
import argparse
import base64
import contextlib
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import sqlite3
import stat
import sys
import uuid

sys.dont_write_bytecode = True
from package import ROOT, PackageError, inventory, verify

CODE_PATHS = ("program", "docs", "bin", "README.md")
PROTECTED_TREES = ("slots", "runs", "goals", "checkpoints", "artifacts", "autonomous")
REQUIRED_COLUMNS = {
    "slots": {"slot", "current_run"},
    "runs": {"run_id", "owner", "lease_until", "checkpoint"},
    "goals": {"run_id", "version", "body"},
    "requests": {"id", "state"},
    "cancellations": {"request_id", "receipt"},
    "outbox": {"id", "status"},
}

class InstallError(RuntimeError):
    pass

def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))

def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()

def encoded(value):
    return {"sqlite_blob_base64": base64.b64encode(value).decode()} if isinstance(value, bytes) else value

def qname(name):
    return '"' + name.replace('"', '""') + '"'

def db_connect(path, writable=False):
    if path.is_symlink() or not path.is_file():
        raise InstallError(f"Database is not a regular existing file: {path}")
    c = sqlite3.connect(path.resolve().as_uri() + ("?mode=rw" if writable else "?mode=ro"), uri=True, timeout=0, isolation_level=None)
    c.execute("PRAGMA busy_timeout=0")
    if not writable:
        c.execute("PRAGMA query_only=ON")
    return c

def db_snapshot(c):
    schema = [list(row) for row in c.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name")]
    tables = {}
    for _, name, _, _ in (row for row in schema if row[0] == "table"):
        columns = [row[1] for row in c.execute(f"PRAGMA table_info({qname(name)})")]
        rows = [[encoded(v) for v in row] for row in c.execute(f"SELECT * FROM {qname(name)}")]
        rows.sort(key=canonical)
        tables[name] = {"columns": columns, "rows": rows}
    # AUTOINCREMENT state matters even when a table currently has no rows.
    if c.execute("SELECT 1 FROM sqlite_master WHERE name='sqlite_sequence'").fetchone():
        tables["sqlite_sequence"] = {"columns": ["name", "seq"], "rows": sorted([list(r) for r in c.execute("SELECT name,seq FROM sqlite_sequence")], key=canonical)}
    return {"schema": schema, "tables": tables}

def file_hash(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()

def tree_snapshot(path):
    if not path.exists() and not path.is_symlink():
        return None
    entries = []
    def visit(p):
        s = p.lstat()
        item = {"path": "." if p == path else p.relative_to(path).as_posix(), "mode": stat.S_IMODE(s.st_mode)}
        if stat.S_ISLNK(s.st_mode):
            item.update(type="symlink", target=os.readlink(p))
        elif stat.S_ISREG(s.st_mode):
            item.update(type="file", size=s.st_size, sha256=file_hash(p))
        elif stat.S_ISDIR(s.st_mode):
            item.update(type="directory")
        else:
            raise InstallError(f"Protected tree has unsupported special file: {p}")
        entries.append(item)
        if stat.S_ISDIR(s.st_mode):
            for child in sorted(p.iterdir()):
                visit(child)
    visit(path)
    return entries

def protected_state(home, connection=None):
    dbfile = home / "tango.sqlite3"
    if connection is not None:
        database = db_snapshot(connection)
    elif dbfile.exists():
        with contextlib.closing(db_connect(dbfile)) as c:
            c.execute("BEGIN")
            database = db_snapshot(c)
            c.execute("ROLLBACK")
    else:
        database = None
    trees = {name: tree_snapshot(home / name) for name in PROTECTED_TREES}
    return {
        "database_sha256": digest(database),
        "tables": {name: {"rows": len(data["rows"]), "sha256": digest(data)} for name, data in (database or {}).get("tables", {}).items()},
        "tree_sha256": {name: digest(value) for name, value in trees.items()},
        "protected_sha256": digest({"database": database, "trees": trees}),
    }

def no_symlinks(path):
    if path.is_symlink():
        raise InstallError(f"Symlink refused in installation path: {path}")
    if path.exists() and path.is_dir():
        for p in path.rglob("*"):
            if p.is_symlink() or (not p.is_file() and not p.is_dir()):
                raise InstallError(f"Symlink or special file refused in installation path: {p}")

def validate_home(home, package):
    if home.is_symlink():
        raise InstallError("Home itself must not be a symlink")
    home = home.resolve()
    if home == package.resolve() or any(home.is_relative_to(package.resolve() / p) for p in ("program", "docs", "installer")):
        raise InstallError("Destination would overwrite source package files")
    if home.exists() and not home.is_dir():
        raise InstallError("Home is not a directory")
    for name in CODE_PATHS + ("tango.sqlite3", "scheduler.lock", "autonomous.lock", ".tango-upgrades"):
        no_symlinks(home / name)
    has_db = (home / "tango.sqlite3").exists()
    if not has_db and home.exists() and any(home.iterdir()):
        # A code-only fresh install has no DB until the operator runs init.
        if not (home / ".tango-upgrades" / "code-only-home.json").is_file():
            raise InstallError("Nonempty destination without a TANGO database is not an initialized or recognized code-only home")
    return home

@contextlib.contextmanager
def scheduler_guard(home, create, name="scheduler.lock"):
    path = home / name
    no_symlinks(path)
    if not path.exists() and not create:
        yield
        return
    try:
        f = path.open("a+b" if create else "rb")
    except OSError as e:
        raise InstallError(f"Cannot open {name}: {e}") from e
    with f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as e:
            message = "Active scheduler lock; stop its owning retained execution session first" if name == "scheduler.lock" else "Active autonomous runner lock; stop its user service and foreground runner before upgrading"
            raise InstallError(message) from e
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)

def idle_check(c):
    names = {row[0] for row in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    for table, required in REQUIRED_COLUMNS.items():
        if table not in names:
            raise InstallError(f"Cannot establish idle safety: required table {table} is missing")
        columns = {row[1] for row in c.execute(f"PRAGMA table_info({qname(table)})")}
        if not required <= columns:
            raise InstallError(f"Cannot establish idle safety: {table} schema is unsupported")
    checks = {
        "claimed_native_requests": "SELECT COUNT(*) FROM requests WHERE state='claimed'",
        "unresolved_cancellations": "SELECT COUNT(*) FROM cancellations WHERE receipt IS NULL",
        "sending_outbox": "SELECT COUNT(*) FROM outbox WHERE status='sending'",
        "owned_native_runs": "SELECT COUNT(*) FROM runs WHERE owner IS NOT NULL",
    }
    blockers = {name: c.execute(sql).fetchone()[0] for name, sql in checks.items()}
    if any(blockers.values()):
        raise InstallError("Home is not idle-safe: " + canonical(blockers))
    integrity = c.execute("PRAGMA integrity_check").fetchall()
    if integrity != [("ok",)]:
        raise InstallError("SQLite integrity check failed")
    return {**blockers, "queued_requests_allowed": True, "unfinished_logical_tasks_allowed": True}

@contextlib.contextmanager
def reserved_home(home, apply):
    with scheduler_guard(home, create=apply), scheduler_guard(home, create=apply, name="autonomous.lock"):
        dbfile = home / "tango.sqlite3"
        if not dbfile.exists():
            yield None, {"fresh_code_only_home": True}
            return
        with contextlib.closing(db_connect(dbfile, writable=apply)) as c:
            try:
                c.execute("BEGIN IMMEDIATE" if apply else "BEGIN")
            except sqlite3.OperationalError as e:
                raise InstallError("Database writer is active; quiesce all writers and retry") from e
            try:
                result = idle_check(c)
                yield c, result
            finally:
                if c.in_transaction:
                    c.execute("ROLLBACK")

def write_json(path, value):
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        f.write(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    fsync_dir(path.parent)

def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)

def fsync_tree(path):
    for p in path.rglob("*"):
        if p.is_file():
            with p.open("rb") as f:
                os.fsync(f.fileno())
    directories = sorted((p for p in path.rglob("*") if p.is_dir()), key=lambda p: len(p.parts), reverse=True)
    for p in directories:
        fsync_dir(p)
    fsync_dir(path)

def copy_path(source, target):
    if source.is_dir():
        shutil.copytree(source, target)
    else:
        shutil.copy2(source, target)

def remove_path(path):
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    elif path.exists() or path.is_symlink():
        path.unlink()

def code_state(home):
    return {name: tree_snapshot(home / name) for name in CODE_PATHS}

def backup_home(home, transaction):
    prior = transaction / "prior"
    prior.mkdir(mode=0o700)
    existed = {}
    for name in CODE_PATHS:
        source = home / name
        existed[name] = source.exists()
        if existed[name]:
            copy_path(source, prior / name)
    dbfile = home / "tango.sqlite3"
    if dbfile.exists():
        # A separate reader is necessary: backup from the connection holding
        # BEGIN IMMEDIATE can block itself. The write reservation stays held.
        with contextlib.closing(db_connect(dbfile)) as source:
            with sqlite3.connect(prior / "tango.sqlite3") as destination:
                source.backup(destination)
                if destination.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                    raise InstallError("Backup SQLite integrity check failed")
    fsync_tree(prior)
    fsync_dir(transaction)
    return existed

def build_payload(home, package, destination):
    destination.mkdir()
    # Copy only the verified manifest payload, excluding bytecode and runtime files.
    files = inventory(package)
    for name in files:
        if name.startswith("program/") or name.startswith("docs/") or name == "README.md":
            target = destination / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(package / name, target)
    # Retain older documentation and unrelated launchers; replace bundled names.
    if (home / "docs").exists():
        for p in (home / "docs").rglob("*"):
            target = destination / "docs" / p.relative_to(home / "docs")
            if not target.exists():
                if p.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(p, target)
    if (home / "bin").exists():
        shutil.copytree(home / "bin", destination / "bin")
    else:
        (destination / "bin").mkdir()
    for name, script in (("tango", "tango.py"), ("tango-auto", "autonomous.py"), ("tango-supervisor", "supervisor.py")):
        if not (destination / "program" / script).is_file():
            continue
        launcher = destination / "bin" / name
        launcher.write_text("#!/bin/sh\nexec " + shlex.quote(sys.executable) + " -B " + shlex.quote(str(home / "program" / script)) + " --home " + shlex.quote(str(home)) + ' "$@"\n', encoding="utf-8")
        launcher.chmod(0o755)
    fsync_tree(destination)

def switch_payload(home, destination, transaction, metadata):
    displaced = transaction / "displaced"
    displaced.mkdir()
    for name in CODE_PATHS:
        metadata["switching"] = name
        write_json(transaction / "transaction.json", metadata)
        old, new = home / name, destination / name
        if old.exists():
            os.replace(old, displaced / name)
            fsync_dir(displaced)
        if new.exists():
            os.replace(new, old)
        fsync_dir(home)
    metadata.pop("switching", None)

def restore_prior(home, transaction, metadata):
    # Never copy tango.sqlite3 back. Rollback restores only these code/doc paths.
    recovery = transaction / ("restore-" + uuid.uuid4().hex)
    recovery.mkdir()
    for name, existed in metadata["prior_existed"].items():
        if existed:
            copy_path(transaction / "prior" / name, recovery / name)
    fsync_tree(recovery)
    displaced = recovery / "replaced"
    displaced.mkdir()
    for name in CODE_PATHS:
        if (home / name).exists():
            os.replace(home / name, displaced / name)
        if (recovery / name).exists():
            os.replace(recovery / name, home / name)
        fsync_dir(home)

def check(home, package):
    integrity = verify(package)
    home = validate_home(home, package)
    with reserved_home(home, apply=False) as (c, idle):
        before = protected_state(home, c)
        return {"stage": "check", "safe_snapshot": True, "home": str(home), "package": integrity, "idle": idle, "protected_state": before, "database_migrated": False, "note": "Read-only snapshot; apply repeats all checks. Quiesce all parent, scheduler, CLI and native writers before apply."}

def apply_install(home, package, quiesced=False):
    integrity = verify(package)
    home = validate_home(home, package)
    existing = (home / "tango.sqlite3").exists()
    if existing and not quiesced:
        raise InstallError("Existing-home apply requires --quiesced after all parent/CLI/native writers are stopped")
    home.mkdir(parents=True, exist_ok=True, mode=0o700)
    with reserved_home(home, apply=True) as (c, idle):
        before = protected_state(home, c)
        root = home / ".tango-upgrades"
        root.mkdir(exist_ok=True, mode=0o700)
        if not existing:
            write_json(root / "code-only-home.json", {"format": "tango-code-only-home-v1", "initialized_database": False})
        identity = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:12]
        transaction = root / identity
        transaction.mkdir(mode=0o700)
        fsync_dir(root)
        metadata = {"format": "tango-code-only-upgrade-v1", "home": str(home), "package": integrity, "operation": "install", "status": "preparing", "protected_before": before, "code_before": digest(code_state(home))}
        metadata["prior_existed"] = backup_home(home, transaction)
        write_json(transaction / "transaction.json", metadata)
        payload = transaction / "new"
        build_payload(home, package, payload)
        # Recheck source and state immediately before the switch.
        if verify(package) != integrity:
            raise InstallError("Source package changed during staging")
        if protected_state(home, c) != before:
            raise InstallError("Protected files changed during staging; no code switched")
        metadata["status"] = "switching"
        try:
            switch_payload(home, payload, transaction, metadata)
            after = protected_state(home, c)
            if before != after:
                raise InstallError("Protected state changed during installation; restoring code only")
            metadata.update(status="installed", protected_after=after, code_after=digest(code_state(home)))
            write_json(transaction / "transaction.json", metadata)
            if not existing:
                write_json(root / "code-only-home.json", {"format": "tango-code-only-home-v1", "initialized_database": False})
            return {"stage": "apply", "installed": True, "home": str(home), "backup": str(transaction), "idle": idle, "protected_before": before, "protected_after": after, "database_migrated": False, "scheduler_started": False}
        except BaseException as e:
            restore_prior(home, transaction, metadata)
            metadata.update(status="failed_code_restored", error=str(e), protected_after=protected_state(home, c))
            write_json(transaction / "transaction.json", metadata)
            raise

def rollback(home, package, backup, quiesced, apply):
    verify(package)
    home = validate_home(home, package)
    backup = backup.resolve()
    if backup.parent != home / ".tango-upgrades":
        raise InstallError("Rollback backup must be a direct transaction directory inside this home's .tango-upgrades")
    no_symlinks(backup)
    metadata = json.loads((backup / "transaction.json").read_text())
    if metadata.get("format") != "tango-code-only-upgrade-v1" or metadata.get("home") != str(home):
        raise InstallError("Backup identity does not match this home")
    if (home / "tango.sqlite3").exists() and apply and not quiesced:
        raise InstallError("Code-only rollback requires --quiesced")
    if set(metadata.get("prior_existed", {})) != set(CODE_PATHS):
        raise InstallError("Incomplete backup metadata")
    expected = {name: tree_snapshot(backup / "prior" / name) if metadata["prior_existed"][name] else None for name in CODE_PATHS}
    if digest(expected) != metadata["code_before"]:
        raise InstallError("Backup code checksum mismatch")
    with reserved_home(home, apply=apply) as (c, idle):
        before = protected_state(home, c)
        if apply:
            restore_prior(home, backup, metadata)
            after = protected_state(home, c)
            if after != before:
                raise InstallError("Protected state changed during code rollback; database was not restored")
            metadata["rollback"] = {"protected_before": before, "protected_after": after, "code_only": True}
            write_json(backup / "transaction.json", metadata)
        return {"stage": "rollback-apply" if apply else "rollback-check", "code_only": True, "database_restored": False, "idle": idle, "protected_before": before, "protected_after": protected_state(home, c)}

def main():
    p = argparse.ArgumentParser(description=__doc__)
    stage = p.add_mutually_exclusive_group(required=True)
    stage.add_argument("--check", action="store_true")
    stage.add_argument("--apply", action="store_true")
    p.add_argument("--home", type=Path, required=True, help="Explicit destination; there is deliberately no live-home default")
    p.add_argument("--package", type=Path, default=ROOT)
    p.add_argument("--quiesced", action="store_true", help="Acknowledge all parent/CLI/native writers have been stopped; not a force option")
    p.add_argument("--rollback", type=Path, metavar="BACKUP", help="Restore prior code/docs/launcher only, retaining the current DB and slot histories")
    a = p.parse_args()
    try:
        result = rollback(a.home, a.package, a.rollback, a.quiesced, a.apply) if a.rollback else (check(a.home, a.package) if a.check else apply_install(a.home, a.package, a.quiesced))
        print(json.dumps(result, indent=2, sort_keys=True))
    except (OSError, sqlite3.Error, ValueError, InstallError, PackageError) as e:
        print(json.dumps({"ok": False, "error": str(e), "database_restore_attempted": False}, sort_keys=True), file=sys.stderr)
        raise SystemExit(2)

if __name__ == "__main__":
    main()
