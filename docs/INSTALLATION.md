# Offline source-only installation and migration safety

This release contains source and documentation. Source installation remains offline and does not launch models. The optional autonomous runner requires an installed, authenticated Codex CLI; see [AUTONOMOUS.md](AUTONOMOUS.md). The installer and broker CLI use Python 3.10+ and the standard library on a POSIX host with `fcntl.flock` and SQLite. The retained queue-only scheduler and service require Linux because process identity and PID namespace checks depend on `/proc`; a successful installation on macOS does not establish service compatibility. No network access, model/provider client, account CLI, credentials, keys, package manager, or service registration is needed for installation. The installer does not import the broker, send messages, spawn native sessions, start a scheduler, or execute queued work.

## Package integrity

The release maintainer seals the final source after all edits and tests:

```sh
python3 -B installer/package.py --seal
python3 -B installer/package.py --verify
```

Distribute the resulting `MANIFEST.sha256` with the package. Install/check refuses missing, extra, changed, or symlinked payload files. The manifest covers `program/`, `docs/`, `installer/`, `README.md`, and `install.sh`, excluding bytecode. Test fixtures, historical evidence, and baseline archives are not installed. Hashes detect corruption; they do not authenticate a publisher if an attacker replaces both source and manifest. Obtain the source and release digest from a trusted source. An end user should verify the supplied manifest, not reseal an unexplained mismatch.

## Fresh isolated home

Choose an explicit new directory. There is deliberately no default destination and no implicit live upgrade.

```sh
./install.sh --check --home /absolute/path/to/new-tango-home
./install.sh --apply --home /absolute/path/to/new-tango-home
/absolute/path/to/new-tango-home/bin/tango --version
```

Check is a read-only snapshot and does not create the destination. Apply installs code, docs, launcher, and installation records only. It does not create a database or slot histories. When you are ready to initialize the new isolated home:

```sh
/absolute/path/to/new-tango-home/bin/tango init
```

`init` creates the fresh local database and three slot directories. It does not start native work or the scheduler. A nonempty unrecognized directory without a TANGO database is refused.

## Existing v0.1.3 home: stop at an idle boundary

An upgrade does not require goals to be completed or slots to be cleared. `REPLANNING`, queued requests, pending messages, checkpoints, and retained goals may remain. Do not call `standby`, replace goals, cancel healthy work, or clear slots merely to enable installation.

Before applying:

1. Wait until the active native executor/verifier finishes or is legitimately interrupted and its actual termination is acknowledged
2. Stop the autonomous user service and any foreground autonomous runner; also stop a legacy queue scheduler through its owning retained execution session. Never signal a PID from a different namespace
3. Suspend parent routing, dispatch, delivery, and all other CLI/DB/artifact writers until installation and any separately approved migration are finished
4. Run check, review the result, then pass `--quiesced` only when that operational boundary is real

```sh
./install.sh --check --home /absolute/path/to/existing-home
./install.sh --apply --home /absolute/path/to/existing-home --quiesced
```

Apply rechecks and refuses any claimed request, unresolved cancellation, sending outbox entry, owned native run, active scheduler or autonomous-runner lock, or unavailable SQLite write reservation. Expired claims still block: the installer must not invent cancellation or delivery receipts. `--quiesced` is an acknowledgement, not a bypass. Unknown or incomplete core schemas fail closed. Logical tasks may remain unfinished.

The installer holds the scheduler's shared-filesystem lock and a SQLite `BEGIN IMMEDIATE` reservation throughout backup and replacement. This excludes scheduler launch and database writes while it is held; it does not stop an already running native process, prevent arbitrary file writes, or stop another CLI from waiting and continuing after the lock is released. Therefore external quiescence is essential. Check is a snapshot, not a reservation for a later apply. No hot-upgrade guarantee is made.

## What is backed up and compared

Each apply writes a private transaction directory under `HOME/.tango-upgrades/`. The JSON result returns its exact path.

- `prior/` contains the previous program, docs, launchers, README, and an integrity-checked database snapshot created using SQLite's backup API, including committed WAL data
- `transaction.json` records source-package hashes, replacement progress, prior code hashes, and before/after protected-state digests
- The protected digest covers all SQLite application tables and schema, SQLite AUTOINCREMENT state, all run/checkpoint/goal/slot data, and the complete `slots`, `runs`, `goals`, `checkpoints`, `artifacts`, and `autonomous` trees when present
- Tree digests include paths, modes, file contents, and symlink targets; symlinks are not followed
- The database and protected trees are never restored or rewritten by the installer

Source files are prepared before replacement. Directory/file renames are atomic individually. Replacing all code, docs, and launchers is not a single globally atomic filesystem operation: an initial directory rename has a short unavailable interval, so all users must remain quiesced. The durable progress journal and retained prior copies support recovery. A caught replacement failure restores prior code only. A process/host crash requires inspecting the journal and using the rollback command before resuming writers. Filesystem and power-loss guarantees still depend on the underlying host/storage.

Older unbundled documentation and unrelated launchers are retained. The program directory is replaced with the verified source payload. Installation paths containing symlinks or special files are refused. Backups may contain private task data; keep their containing directory private and do not publish them with a release.

## Separate migration rehearsal, then an explicit migration decision

Code installation intentionally leaves `meta.version` and every original database byte of logical data unchanged. Even seemingly read-only broker commands currently construct `Broker`, whose initialization applies schema setup and records the version. Therefore do not run the updated `status`, `doctor`, or `init` against an existing home until the migration decision has been made.

First rehearse initialization on a new, isolated copy while original writers remain quiesced:

```sh
python3 -B installer/migration_check.py \
  --home /absolute/path/to/existing-home \
  --output /absolute/path/outside-that-home/migration-rehearsal \
  --quiesced
```

The output directory must not exist and must be outside the original home. The helper refuses symlinks inside the protected trees, copies the database with SQLite's backup API and protected trees, then runs only the new broker's `init` against `SIMULATION_HOME`. It never runs migration against the original home. All outputs are explicitly labeled SIMULATION.

The check requires every original row and column value in every table to remain unchanged, except `meta.version`. It reports new tables/columns, requires original schema objects and column/foreign-key definitions to be unchanged, rejects new triggers/views pending review, compares artifact trees, checks SQLite integrity, and verifies the original home's before/after digest. From 0.1.3 the expected additions are `meeting_members`, `meeting_policy`, `meeting_posts`, `meeting_summaries`, `meetings`, `parent_actions` and `parent_presence`. From 0.3.0 the only added table is `meeting_summaries`. The 0.4.0 CLI adapter adds no broker tables beyond 0.3.1; it records execution provenance in existing records and private `autonomous/` files. Changes to existing table definitions, including added columns, require separate review and do not automatically pass this release's strict rehearsal. Legacy prerequisite backfill that changes an existing table is flagged for review rather than silently allowed. A pass is evidence about that copy, not authorization or a guarantee for a later live migration.

For a separately approved live migration, keep the same idle boundary, first take a new code-only installer backup/check, review the successful rehearsal and expected schema additions, then explicitly run the installed broker's `init`. Compare protected original-table and artifact data again before resuming any writers. This package intentionally provides no automatic production-migration or database-restore command. Unexpected changes require investigation on a copy; never replay an old DB over subsequent work.

## Code-only rollback

Use the exact backup path returned by apply:

```sh
./install.sh --check --home /absolute/path/to/existing-home \
  --rollback /absolute/path/to/existing-home/.tango-upgrades/TRANSACTION
./install.sh --apply --home /absolute/path/to/existing-home --quiesced \
  --rollback /absolute/path/to/existing-home/.tango-upgrades/TRANSACTION
```

Rollback performs the same idle checks and validates backup code checksums. It restores only the prior program/docs/launchers/README, and compares the *current* database and protected-tree digest before/after. It never restores the saved database, so work committed after installation is retained. Review compatibility before running old code against a newer schema; additive changes are the intended compatibility boundary. Rollback of a fresh code-only installation removes installed code while retaining any database or task histories created afterward.

## Test scope

Run the focused installer simulation suite:

```sh
python3 -B -m unittest discover -s tests -p test_installer.py -v
```

Every home and test package is synthetic and created under the release test directory. Tests cover fresh code-only install, v0.1.3 update, queued/REPLANNING preservation, SQLite backup, code rollback with subsequent work retained, failed-switch recovery, all specified safety refusals, checksum failures, and copy-only migration rehearsal. These are local installer simulations, not live migration, native-agent execution, transport, or lifecycle acceptance evidence. See `TEST_REPORT.md` and `evidence/TESTS.txt` for this English edition's local verification; the supplied archive's historical full Linux run is retained separately in `evidence/upstream/TESTS.txt`.
