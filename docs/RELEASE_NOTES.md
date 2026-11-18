# TANGO 0.4.0: autonomous execution

This release adds actual execution to the English source distribution. The runtime identifies itself as `0.4.0-autonomous`. The original collaboration-tool adapter and its protocol remain supported.

## Execution and continuation

- A genuine local Codex CLI backend launches real sessions, records the returned thread IDs, requires completed turns and successful process exits, and resumes discussion sessions when needed.
- The autonomous runner executes queued tasks, preserves checkpoints, retries recoverable failures without a fixed attempt cap, and uses fresh independent verifier sessions. Goal satisfaction, explicit pause/stop, and genuine prerequisites retain their distinct outcomes.
- Submission is serialized per home. A durable intent and stable message ID allow interrupted submission to be replayed without mixing another task's goal and criteria. Running services also recover pending submissions.
- Permission, material, quota, and contrary-result goal decisions remain waiting until authenticated information and an actual recheck justify continuing.
- The original ten-calculation trigger, source inventory, five distinct discussion participants, ten synchronized rounds, and next-calculation handoff remain enforced.

## Recovery and deployment

- CLI launch registration, actual process birth identity, private execution journals, leases, and cancellation reconciliation distinguish completed, interrupted, never-launched, failed-start, and ambiguous attempts. Uncertain old work is not blindly duplicated.
- Linux systemd user services and macOS LaunchAgents can supervise the runner. Service generation does not activate it; installation is a separate explicit deployment action.
- The installer adds `tango-auto` and `tango-supervisor`, uses an absolute Python interpreter, refuses upgrade while the runner is active, and preserves the `autonomous/` runtime tree through upgrade, rehearsal, and rollback.

## Compatibility and evidence

The public contract in `spec/user_spec.txt` and the 0.1.3 migration fixture remain byte-identical compatibility inputs. [spec/autonomous_contract.txt](../spec/autonomous_contract.txt) defines the new backend. Existing regression assertions are retained except for current version and English README expectations. Additional subprocess and recovery tests are clearly labeled synthetic.

Unlike the prior English-only edition, this release changes runtime and installer code. Its predecessor's byte-identity and test reports are historical and are stored separately. See [TEST_REPORT.md](../TEST_REPORT.md) for this release's exact local checks and bounded real-model acceptance. A real full five-agent ten-round meeting and an installed OS-service restart have not been certified. No live runtime, credentials, private model transcripts, or task outputs are included in the public source package.

The supplied original archive contained no license; this release does not choose one on behalf of the rights holder.
