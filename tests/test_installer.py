"""SIMULATION ONLY: installer tests use synthetic homes inside the staging tree."""
import contextlib
import fcntl
import importlib.util
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "installer"))
import install
import migration_check
import package

spec = importlib.util.spec_from_file_location("tango_installer_v013_baseline", ROOT / "baseline" / "tango_v013.py")
baseline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(baseline)

class InstallerSimulation(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="SIMULATION_installer_", dir=ROOT / "tests")
        self.root = Path(self.temporary.name)
        self.package = self.root / "package"
        self.package.mkdir()
        for directory in ("program", "docs", "installer"):
            shutil.copytree(ROOT / directory, self.package / directory, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        shutil.copy2(ROOT / "install.sh", self.package / "install.sh")
        readme = ROOT / "README.md"
        shutil.copy2(readme if readme.exists() else ROOT / "baseline" / "README_v013.md", self.package / "README.md")
        package.seal(self.package)
        self.home = self.root / "SIMULATION_HOME"

    def tearDown(self):
        self.temporary.cleanup()

    def make_home(self):
        b = baseline.Broker(self.home, simulation=True)
        result = b.route("SIMULATED-ingress-1", "\\TANGO_TASK_1 start: Preserve this synthetic goal", "test_injection")
        run = b.db.execute("SELECT current_run FROM slots WHERE slot=1").fetchone()[0]
        b.route("SIMULATED-ingress-2", "\\TANGO_TASK_1 Additional synthetic input", "test_injection")
        b.db.execute("UPDATE runs SET checkpoint=? WHERE run_id=?", ('{"SIMULATION":"checkpoint"}', run))
        b.tick()
        work = b.path(run) / "work"
        work.mkdir(exist_ok=True)
        (work / "SIMULATED_artifact.txt").write_text("Synthetic retained artifact\n")
        b.db.close()
        (self.home / "program").mkdir()
        shutil.copy2(ROOT / "baseline" / "tango_v013.py", self.home / "program" / "tango.py")
        (self.home / "program" / "old_component.py").write_text("# SIMULATION old release\n")
        (self.home / "program" / "__pycache__").mkdir()
        (self.home / "program" / "__pycache__" / "old.pyc").write_bytes(b"SIMULATED legacy bytecode")
        (self.home / "docs").mkdir()
        (self.home / "docs" / "historical.md").write_text("SIMULATION historical document\n")
        (self.home / "bin").mkdir()
        (self.home / "bin" / "unrelated-tool").write_text("SIMULATION launcher\n")
        (self.home / "README.md").write_text("SIMULATION previous README\n")
        return run

    def sql(self, statement, params=()):
        with sqlite3.connect(self.home / "tango.sqlite3") as c:
            c.execute(statement, params)

    def test_fresh_check_creates_nothing(self):
        result = install.check(self.home, self.package)
        self.assertTrue(result["safe_snapshot"])
        self.assertFalse(self.home.exists())

    def test_fresh_apply_installs_only_code_and_explicit_init_works(self):
        report = install.apply_install(self.home, self.package)
        self.assertFalse((self.home / "tango.sqlite3").exists())
        self.assertFalse((self.home / "slots").exists())
        self.assertEqual(report["protected_before"], report["protected_after"])
        p = subprocess.run([str(self.home / "bin" / "tango"), "--version"], text=True, capture_output=True)
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertFalse((self.home / "tango.sqlite3").exists())
        p = subprocess.run([str(self.home / "bin" / "tango"), "init"], text=True, capture_output=True)
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertTrue((self.home / "tango.sqlite3").exists())

    def test_update_preserves_replanning_queued_goal_checkpoint_and_artifact(self):
        run = self.make_home()
        before = install.protected_state(self.home)
        report = install.apply_install(self.home, self.package, quiesced=True)
        self.assertEqual(before, install.protected_state(self.home))
        self.assertEqual(before, report["protected_after"])
        self.assertTrue((self.home / "docs" / "historical.md").exists())
        self.assertTrue((self.home / "bin" / "unrelated-tool").exists())
        self.assertFalse((self.home / "program" / "old_component.py").exists())
        with sqlite3.connect(self.home / "tango.sqlite3") as c:
            self.assertEqual(c.execute("SELECT value FROM meta WHERE key='version'").fetchone()[0], baseline.VERSION)
            self.assertEqual(c.execute("SELECT state FROM runs WHERE run_id=?", (run,)).fetchone()[0], "REPLANNING")
            self.assertEqual(c.execute("SELECT state FROM requests").fetchone()[0], "queued")
        with contextlib.closing(install.db_connect(Path(report["backup"]) / "prior" / "tango.sqlite3")) as c:
            self.assertEqual(install.digest(install.db_snapshot(c)), before["database_sha256"])

    def test_shell_entrypoint_has_explicit_check_and_apply_stages(self):
        script = str(self.package / "install.sh")
        check = subprocess.run([script, "--check", "--home", str(self.home)], capture_output=True, text=True)
        self.assertEqual(check.returncode, 0, check.stderr)
        self.assertEqual(json.loads(check.stdout)["stage"], "check")
        self.assertFalse(self.home.exists())
        applied = subprocess.run([script, "--apply", "--home", str(self.home)], capture_output=True, text=True)
        self.assertEqual(applied.returncode, 0, applied.stderr)
        self.assertEqual(json.loads(applied.stdout)["stage"], "apply")
        neither = subprocess.run([script, "--home", str(self.home)], capture_output=True, text=True)
        self.assertNotEqual(neither.returncode, 0)

    def test_protected_state_change_during_copy_prevents_switch(self):
        run = self.make_home()
        before = install.digest(install.code_state(self.home))
        original = install.build_payload
        def changed_artifact(home, package_root, destination):
            original(home, package_root, destination)
            (home / "slots" / "1" / "runs" / run / "work" / "SIMULATED_artifact.txt").write_text("SIMULATION concurrent native file write")
        with mock.patch.object(install, "build_payload", side_effect=changed_artifact):
            with self.assertRaisesRegex(install.InstallError, "Protected files changed"):
                install.apply_install(self.home, self.package, True)
        self.assertEqual(install.digest(install.code_state(self.home)), before)

    def test_rehearsal_refuses_protected_symlink(self):
        run = self.make_home()
        target = self.home / "slots" / "1" / "runs" / run / "work" / "SIMULATED_link"
        target.symlink_to(self.root / "outside_target")
        with self.assertRaisesRegex(install.InstallError, "Symlink"):
            migration_check.rehearse(self.home, self.package, self.root / "SIMULATION_rehearsal", True)

    def test_migration_rejects_existing_table_constraint_change(self):
        table = {"columns": ["v"], "rows": [[1]]}
        before = {"schema": [["table", "sample", "sample", "CREATE TABLE sample(v INTEGER CHECK(v>0))"]], "tables": {"sample": table}}
        after = {"schema": [["table", "sample", "sample", "CREATE TABLE sample(v INTEGER)"]], "tables": {"sample": table}}
        self.assertFalse(migration_check.additive_comparison(before, after)["additive_data_check_passed"])

    def test_existing_apply_requires_quiescence_acknowledgement(self):
        self.make_home()
        with self.assertRaisesRegex(install.InstallError, "--quiesced"):
            install.apply_install(self.home, self.package)

    def test_claimed_request_refused_even_expired(self):
        self.make_home()
        self.sql("UPDATE requests SET state='claimed'")
        with self.assertRaisesRegex(install.InstallError, "claimed_native_requests"):
            install.apply_install(self.home, self.package, True)

    def test_unresolved_cancellation_refused(self):
        run = self.make_home()
        self.sql("INSERT INTO cancellations VALUES ('SIMULATED-cancel',?,'SIMULATED-native','SIMULATED interruption',NULL,0)", (run,))
        with self.assertRaisesRegex(install.InstallError, "unresolved_cancellations"):
            install.check(self.home, self.package)

    def test_sending_outbox_refused_even_expired(self):
        self.make_home()
        self.sql("UPDATE outbox SET status='sending',claim_until=0")
        with self.assertRaisesRegex(install.InstallError, "sending_outbox"):
            install.apply_install(self.home, self.package, True)

    def test_orphan_native_owner_refused(self):
        self.make_home()
        self.sql("UPDATE runs SET owner='SIMULATED-owner',lease_until=0")
        with self.assertRaisesRegex(install.InstallError, "owned_native_runs"):
            install.apply_install(self.home, self.package, True)

    def test_scheduler_lock_refused(self):
        self.make_home()
        with (self.home / "scheduler.lock").open("a+b") as f:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(install.InstallError, "Active scheduler lock"):
                install.apply_install(self.home, self.package, True)

    def test_concurrent_database_writer_refused(self):
        self.make_home()
        with sqlite3.connect(self.home / "tango.sqlite3", isolation_level=None) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                with self.assertRaisesRegex(install.InstallError, "Database writer is active"):
                    install.apply_install(self.home, self.package, True)
            finally:
                c.execute("ROLLBACK")

    def test_unknown_schema_fails_closed(self):
        self.make_home()
        self.sql("ALTER TABLE cancellations RENAME TO missing_cancellations")
        with self.assertRaisesRegex(install.InstallError, "required table cancellations"):
            install.apply_install(self.home, self.package, True)

    def test_missing_manifest_refused(self):
        (self.package / package.MANIFEST).unlink()
        with self.assertRaises(package.PackageError):
            install.check(self.home, self.package)

    def test_tampered_payload_refused_before_home_creation(self):
        with (self.package / "program" / "tango.py").open("a") as f:
            f.write("\n# SIMULATION tamper\n")
        with self.assertRaisesRegex(package.PackageError, "Checksum mismatch"):
            install.apply_install(self.home, self.package)
        self.assertFalse(self.home.exists())

    def test_unmanifested_payload_refused(self):
        (self.package / "program" / "unexpected.py").write_text("# SIMULATION extra\n")
        with self.assertRaisesRegex(package.PackageError, "inventory differs"):
            install.check(self.home, self.package)

    def test_symlink_installation_path_refused(self):
        self.make_home()
        (self.home / "program" / "unsafe.py").symlink_to(self.package / "program" / "tango.py")
        with self.assertRaisesRegex(install.InstallError, "Symlink"):
            install.apply_install(self.home, self.package, True)

    def test_unknown_nonempty_destination_refused(self):
        self.home.mkdir()
        (self.home / "unrelated.txt").write_text("SIMULATION unrelated data")
        with self.assertRaisesRegex(install.InstallError, "Nonempty destination"):
            install.check(self.home, self.package)

    def test_code_rollback_preserves_subsequent_database_work(self):
        self.make_home()
        code_before = install.digest(install.code_state(self.home))
        report = install.apply_install(self.home, self.package, True)
        self.sql("INSERT INTO events(run_id,kind,payload,created) VALUES(NULL,'SIMULATED-after-install','{}',1)")
        current = install.protected_state(self.home)
        rollback = install.rollback(self.home, self.package, Path(report["backup"]), True, True)
        self.assertEqual(install.protected_state(self.home), current)
        self.assertEqual(install.digest(install.code_state(self.home)), code_before)
        self.assertFalse(rollback["database_restored"])

    def test_rollback_backup_tamper_refused(self):
        self.make_home()
        report = install.apply_install(self.home, self.package, True)
        prior = Path(report["backup"]) / "prior" / "README.md"
        prior.write_text("SIMULATION tamper")
        with self.assertRaisesRegex(install.InstallError, "Backup code checksum mismatch"):
            install.rollback(self.home, self.package, Path(report["backup"]), True, True)

    def test_autonomous_journals_survive_update_rehearsal_and_rollback(self):
        self.make_home()
        jobs = self.home / "autonomous" / "jobs"
        attempts = self.home / "autonomous" / "attempts" / "SIMULATED-attempt"
        jobs.mkdir(parents=True)
        attempts.mkdir(parents=True)
        (jobs / "SIMULATED-job.json").write_text('{"goal":"Retain this synthetic task"}\n')
        journal = attempts / "attempt.json"
        journal.write_text('{"state":"Synthetic saved checkpoint"}\n')
        before = install.protected_state(self.home)
        self.assertIn("autonomous", install.PROTECTED_TREES)
        update = install.apply_install(self.home, self.package, True)
        self.assertEqual(install.protected_state(self.home), before)
        copied = self.root / "SIMULATION_autonomous_rehearsal"
        rehearsal = migration_check.rehearse(self.home, self.package, copied, True)
        self.assertTrue(rehearsal["additive_data_check_passed"], rehearsal["problems"])
        self.assertEqual(install.tree_snapshot(copied / "SIMULATION_HOME" / "autonomous"),
                         install.tree_snapshot(self.home / "autonomous"))
        journal.write_text('{"state":"Synthetic checkpoint recorded after update"}\n')
        after_new_work = install.protected_state(self.home)
        self.assertNotEqual(after_new_work, before)
        install.rollback(self.home, self.package, Path(update["backup"]), True, True)
        self.assertEqual(install.protected_state(self.home), after_new_work)

    def test_partial_switch_failure_restores_prior_code(self):
        self.make_home()
        before = install.protected_state(self.home)
        code = install.digest(install.code_state(self.home))
        original = install.os.replace
        def failing_replace(source, destination):
            if Path(source).name == "docs" and Path(source).parent.name == "new":
                raise OSError("SIMULATED directory-switch failure")
            return original(source, destination)
        with mock.patch.object(install.os, "replace", side_effect=failing_replace):
            with self.assertRaisesRegex(OSError, "SIMULATED"):
                install.apply_install(self.home, self.package, True)
        self.assertEqual(install.protected_state(self.home), before)
        self.assertEqual(install.digest(install.code_state(self.home)), code)

    def test_copy_only_migration_rehearsal(self):
        self.make_home()
        before = install.protected_state(self.home)
        result = migration_check.rehearse(self.home, self.package, self.root / "SIMULATION_rehearsal", True)
        self.assertTrue(result["additive_data_check_passed"], result["problems"])
        self.assertEqual(result["additions"]["tables"], ["meeting_members", "meeting_policy", "meeting_posts", "meeting_summaries", "meetings", "parent_actions", "parent_presence"])
        self.assertEqual(install.protected_state(self.home), before)
        self.assertFalse(result["original_database_migrated"])

    def test_rehearsal_requires_new_output_outside_original_home(self):
        self.make_home()
        with self.assertRaisesRegex(install.InstallError, "outside the original home"):
            migration_check.rehearse(self.home, self.package, self.home / "unsafe-rehearsal", True)

    def test_migration_comparison_rejects_old_row_changes(self):
        before = {"schema": [], "tables": {"runs": {"columns": ["run_id", "checkpoint"], "rows": [["SIMULATED-run", "original"]]}}}
        after = {"schema": [], "tables": {"runs": {"columns": ["run_id", "checkpoint"], "rows": [["SIMULATED-run", "changed"]]}}}
        self.assertFalse(migration_check.additive_comparison(before, after)["additive_data_check_passed"])

if __name__ == "__main__":
    unittest.main(verbosity=2)
