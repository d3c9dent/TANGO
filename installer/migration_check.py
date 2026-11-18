#!/usr/bin/env python3
"""Rehearse broker initialization on an isolated copy; never migrate the original home."""
from __future__ import annotations
import argparse
import collections
import contextlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys

sys.dont_write_bytecode = True
from install import (InstallError, PROTECTED_TREES, canonical, db_connect, db_snapshot,
                     digest, idle_check, no_symlinks, protected_state, reserved_home, tree_snapshot,
                     validate_home, write_json)
from package import ROOT, PackageError, verify

def additive_comparison(before, after):
    problems = []
    additions = {"tables": [], "columns": {}}
    old_objects = {(r[0], r[1]): r for r in before["schema"]}
    new_objects = {(r[0], r[1]): r for r in after["schema"]}
    for key, old in old_objects.items():
        new = new_objects.get(key)
        if new is None:
            problems.append(f"Schema object removed: {key}")
        elif old != new:
            problems.append(f"Existing schema object modified: {key}")
    for key in new_objects.keys() - old_objects.keys():
        if key[0] in {"trigger", "view"}:
            problems.append(f"New executable schema object needs manual review: {key}")
    for name, old in before["tables"].items():
        new = after["tables"].get(name)
        if new is None:
            problems.append(f"Table removed: {name}")
            continue
        columns = old["columns"]
        if not set(columns) <= set(new["columns"]):
            problems.append(f"Columns removed or renamed: {name}")
            continue
        indexes = [new["columns"].index(c) for c in columns]
        old_rows = old["rows"]
        projected = [[r[i] for i in indexes] for r in new["rows"]]
        if name == "meta" and "key" in columns:
            key_index = columns.index("key")
            previous_version = [r for r in old_rows if r[key_index] == "version"]
            current_version = [r for r in projected if r[key_index] == "version"]
            if len(previous_version) != len(current_version) or len(current_version) != 1:
                problems.append("meta.version must remain a single existing row")
            old_rows = [r for r in old_rows if r[key_index] != "version"]
            projected = [r for r in projected if r[key_index] != "version"]
        previous = collections.Counter(canonical(r) for r in old_rows)
        current = collections.Counter(canonical(r) for r in projected)
        if previous != current:
            problems.append(f"Existing row data changed: {name}")
        extra = [c for c in new["columns"] if c not in columns]
        if extra:
            additions["columns"][name] = extra
    additions["tables"] = sorted(after["tables"].keys() - before["tables"].keys())
    return {"additive_data_check_passed": not problems, "problems": problems, "additions": additions}

def schema_details(c):
    result = {}
    for (name,) in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"):
        quoted = '"' + name.replace('"', '""') + '"'
        result[name] = {"columns": [list(r) for r in c.execute(f"PRAGMA table_info({quoted})")], "foreign_keys": [list(r) for r in c.execute(f"PRAGMA foreign_key_list({quoted})")]}
    return result

def compare_details(before, after):
    errors = []
    for table, prior in before.items():
        if table not in after:
            continue
        current = after[table]
        if current["columns"][:len(prior["columns"])] != prior["columns"]:
            errors.append(f"Existing column definition changed: {table}")
        if current["foreign_keys"] != prior["foreign_keys"]:
            errors.append(f"Existing foreign-key definition changed: {table}")
    return errors

def rehearse(home, package, output, quiesced=False):
    integrity = verify(package)
    home = validate_home(home, package)
    if not quiesced:
        raise InstallError("Copy rehearsal requires --quiesced so the artifact and database snapshots are consistent")
    if output.is_symlink():
        raise InstallError("Rehearsal output cannot be a symlink")
    output = output.resolve()
    if output.exists() or output == home or output.is_relative_to(home):
        raise InstallError("Rehearsal output must be a new directory outside the original home")
    if not (home / "tango.sqlite3").is_file():
        raise InstallError("Rehearsal needs an existing initialized home")
    with reserved_home(home, apply=False) as (c, idle):
        if c is None:
            raise InstallError("No database")
        original_before = protected_state(home, c)
        database_before = db_snapshot(c)
        details_before = schema_details(c)
        output.mkdir(parents=True, mode=0o700)
        isolated = output / "SIMULATION_HOME"
        isolated.mkdir(mode=0o700)
        with sqlite3.connect(isolated / "tango.sqlite3") as destination:
            c.backup(destination)
        for name in PROTECTED_TREES:
            source = home / name
            no_symlinks(source)
            if source.exists():
                shutil.copytree(source, isolated / name)
        original_after_copy = protected_state(home, c)
        # A second, independent reader sees commits after our read snapshot.
        current_original = protected_state(home)
        if original_before != original_after_copy or current_original != original_before:
            raise InstallError("Original state changed during copy; rehearsal was not run")
    trees_before = {name: tree_snapshot(isolated / name) for name in PROTECTED_TREES}
    # Only the broker's local init command runs, against the named isolated copy.
    # No daemon, tick, dispatch, adapter, native worker or delivery action runs.
    env = {"PYTHONDONTWRITEBYTECODE": "1", "PYTHONIOENCODING": "utf-8"}
    process = subprocess.run([sys.executable, "-B", str(package.resolve() / "program" / "tango.py"), "--home", str(isolated), "init"], capture_output=True, text=True, timeout=60, env=env)
    (output / "SIMULATION_init.stdout.txt").write_text(process.stdout, encoding="utf-8")
    (output / "SIMULATION_init.stderr.txt").write_text(process.stderr, encoding="utf-8")
    with contextlib.closing(db_connect(isolated / "tango.sqlite3")) as c:
        database_after = db_snapshot(c)
        details_after = schema_details(c)
        integrity_ok = c.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
    comparison = additive_comparison(database_before, database_after)
    comparison["problems"].extend(compare_details(details_before, details_after))
    trees_after = {name: tree_snapshot(isolated / name) for name in PROTECTED_TREES}
    if trees_before != trees_after:
        comparison["problems"].append("Protected artifact/slot tree changed during init")
    if process.returncode:
        comparison["problems"].append(f"Broker init exited {process.returncode}")
    if not integrity_ok:
        comparison["problems"].append("Migrated-copy SQLite integrity check failed")
    if verify(package) != integrity:
        comparison["problems"].append("Package changed during rehearsal")
    original_after = protected_state(home)
    if original_after != original_before:
        comparison["problems"].append("Original home changed during rehearsal; investigate concurrent writers")
    comparison["additive_data_check_passed"] = not comparison["problems"]
    report = {"label": "SIMULATION: isolated-copy initialization, not production migration or native E2E", "original_home": str(home), "isolated_home": str(isolated), "original_database_migrated": False, "original_before": original_before, "original_after": original_after, "copy_database_before": digest(database_before), "copy_database_after": digest(database_after), "package": integrity, **comparison}
    write_json(output / "SIMULATION_migration_report.json", report)
    return report

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--home", type=Path, required=True)
    p.add_argument("--package", type=Path, default=ROOT)
    p.add_argument("--output", type=Path, required=True, help="New isolated output directory outside the original home")
    p.add_argument("--quiesced", action="store_true")
    a = p.parse_args()
    try:
        result = rehearse(a.home, a.package, a.output, a.quiesced)
        print(json.dumps(result, indent=2, sort_keys=True))
        raise SystemExit(0 if result["additive_data_check_passed"] else 3)
    except (OSError, sqlite3.Error, ValueError, subprocess.TimeoutExpired, InstallError, PackageError) as e:
        print(json.dumps({"ok": False, "error": str(e), "original_database_migrated": False}), file=sys.stderr)
        raise SystemExit(2)

if __name__ == "__main__":
    main()
