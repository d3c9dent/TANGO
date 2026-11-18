# TANGO 0.4.0 verification report

Verification date: 2026-11-17 (Asia/Shanghai).

## Regression checks

On macOS with Python 3.13.9, **217 of 217 selected tests passed** in 36.578 seconds. The suite discovered 223 tests. Six retained Linux `/proc` lifecycle tests were explicitly excluded; none is claimed to have passed in this run.

The [actual log](evidence/TESTS.txt) and [machine-readable summary](evidence/LOCAL_TEST_SUMMARY.json) record the result and exact exclusions. The original 159 tests remain, with current version and README expectations updated; 64 additional cases cover CLI receipts, subprocess execution, autonomous lifecycle, submission recovery, service configuration, and runtime-preserving installation.

The new lifecycle checks exercise failed verification followed by repair and a fresh verifier, no-progress continuation, permission/quota waiting and rechecking, pause/resume, recovery after the supervisor process is killed, submission races and interruption, and the full source-inventory/five-participant/ten-round/next-execution path. These use **synthetic model outputs in actual local subprocesses**. They verify protocol behavior and process handling; they are not evidence of real model discussions.

Run `python3 -B scripts/run_tests.py --portable` to repeat the local subset. Use Linux and `python3 -B -m unittest discover -s tests -v` for all 223 tests. GitHub CI is configured for Python 3.10 and 3.13, but has not been run on GitHub during this preparation.

## Real model acceptance

A bounded test used the actual, already authenticated Codex CLI (0.169.0-alpha.11.1) and the public `tango-auto submit` command. Without a follow-up user message, an execution session wrote the requested text file, a distinct fresh session independently verified it, and the task reached `COMPLETED`. The file's exact bytes matched the criterion and no cancellation remained unresolved. See [REAL_CLI_ACCEPTANCE.json](evidence/REAL_CLI_ACCEPTANCE.json).

A preceding smaller real-backend check also passed. The public package includes only sanitized acceptance metadata, not account credentials, session transcripts, live databases, or private runtime homes. An initial restricted-context attempt could not create Codex's normal session records; the successful real checks were run with approved access to those records. No account configuration was changed.

These checks establish a small actual execute/verify/complete path. They do **not** establish an actual real-model five-participant ten-round meeting, indefinite resource availability, universal task solvability, or OS-service restart behavior.

## Installation and source checks

The code-only installer, installed launchers, generated service configuration, source syntax, documentation links, and checksum manifests are checked separately. Service tests use mocked manager commands, and generated macOS plist syntax is checked. No persistent system service was installed or enabled during preparation. See [INSTALL_SMOKE.json](evidence/INSTALL_SMOKE.json) for the fresh-install smoke results.

The runtime and installer changed in 0.4.0. Their predecessor's byte-identity claims do not apply to this release. [SOURCE_PROVENANCE.json](evidence/SOURCE_PROVENANCE.json) identifies changed and retained source files. `spec/user_spec.txt` and the 0.1.3 baseline remain unchanged compatibility inputs.

## Historical evidence

The original archive's reports remain under `evidence/upstream/`. The earlier English-only 0.3.1 packaging checks remain under `evidence/english-0.3.1/`. These are historical records, not new 0.4.0 validation.

`MANIFEST.sha256` covers the installation payload. `RELEASE_SHA256SUMS` covers the source distribution except itself. Hashes detect changes; they are not publisher signatures.
