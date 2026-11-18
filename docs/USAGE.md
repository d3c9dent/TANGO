# Using the retained native-parent adapter

This guide describes the original collaboration-tool adapter. For the new self-running local Codex backend, use [AUTONOMOUS.md](AUTONOMOUS.md). Its execution and service setup are separate from the adapter below.

## What this adapter does

TANGO is a task state machine for a parent agent with access to real native agent tools. It is not a standalone model client. Copying this package to a computer does not provide models, agents, or permission to wake a parent in the background.

The workflow follows a fixed order:

1. Complete ten substantive calculation attempts, retaining results, unsuccessful approaches, and checkpoints.
2. **Inventory all accessible existing project results first**, not just the latest ten attempts. Read the original material, classify it, remove duplicates, and distinguish accepted results, unsuccessful approaches, additional assumptions, unresolved obligations, and unverified findings. Existing indexes help locate material; they do not establish that each proof has been checked.
3. Record a hash for every source and state what was accessible, unavailable, or excluded. Missing material permits an explicitly partial inventory, not a claim of complete coverage. An empty source inventory cannot start a meeting.
4. Seal a single shared summary. In each round, five distinct real agents receive that summary and all contributions from earlier completed rounds. All five contributions must be present before the next round begins, for ten rounds in total. Five role-played responses are not a substitute.
5. Save an evidence-based direction for the next calculation, dissenting views, unfinished proof obligations, and stopping conditions. The parent claims the automatically queued calculation request and invokes the actual native tools to continue.

Meeting consensus is not a mathematical proof. Partial conclusions cannot replace the original goal. Changes to sources, additional user requirements, or revised acceptance criteria invalidate the affected summary and discussion and require a new inventory. Pause and stop commands take priority over automatic continuation.

## Requirements

- Linux is required for the full service lifecycle, with Python 3.10 or later. No additional Python packages are required.
- A native parent must actually be able to call `collaboration.spawn_agent`, `followup_task`, `list_agents`, and, when needed, `interrupt_agent`. Tool names must match the integration platform's real capabilities. Success receipts must never be fabricated.
- The meeting requires capacity for one coordinator and five discussion participants in addition to the parent. If the platform lacks enough slots, wait for capacity; do not silently reduce the group or simulate five participants sequentially.
- Continued execution requires an active parent. Unattended recovery after the conversation ends also requires persistent wake support that the platform actually provides and that has been configured and verified. This package neither supplies nor assumes that capability.
- A real five-agent, ten-round meeting **has not yet passed end-to-end acceptance**. The original 159 isolated tests use simulated receipts to check the protocol; the autonomous edition adds further tests. These do not establish that real discussions took place. See the [test report](../TEST_REPORT.md) for the results applicable to this distribution.

## Installation or local evaluation

Start in the extracted repository root. The full test suite requires Linux. Restore the shell entry point's executable permission if your download method did not retain it:

```sh
chmod +x install.sh
python3 -B installer/package.py --verify
python3 -B -m unittest discover -s tests -v
```

Choose a new directory that does not already exist. Replace the absolute path below with your own destination:

```sh
sh install.sh --check --home /absolute/path/to/new-tango-home
sh install.sh --apply --home /absolute/path/to/new-tango-home
/absolute/path/to/new-tango-home/bin/tango init
/absolute/path/to/new-tango-home/bin/tango doctor
```

`doctor` does not declare native integration ready merely because unit tests pass. Do not fabricate receipts to clear its diagnostics.

Integrate the parent using the [step-by-step native tool procedure](NATIVE_PARENT_LOOP.md). Without a native parent, local state management and tests are available, but automated research execution has not been established.

## Upgrading an existing instance

Finish any active calculation, confirm that no old-version writers or unacknowledged native cancellations remain, then back up and rehearse at an idle boundary before replacing the code. Relative to 0.3.0, version 0.3.1 adds only the `meeting_summaries` table and updates version metadata; it retains calculation counts, task goals, and queued calculations. Each actual instance still needs its own checks. Do not directly copy another instance's database.

The source inventory must exclude the active database, scheduling files, meeting contribution and receipt directories, and the summary being generated. Otherwise, every write would invalidate the summary. Historical research directories and explicitly listed earlier research files can be monitored; changes to the actual mathematical material still require a new inventory. State the scope and reasons for exclusions explicitly.

See [installation and migration](INSTALLATION.md) for the full procedure and rollback boundaries.

## File guide

- [`program/tango.py`](../program/tango.py): the current broker and JSON API.
- [`program/service.py`](../program/service.py): the local Linux queue service within the current execution session; it does not execute models.
- [`docs/NATIVE_PARENT_LOOP.md`](NATIVE_PARENT_LOOP.md): step-by-step integration of the native parent.
- [`docs/PREMEETING_INVENTORY.md`](PREMEETING_INVENTORY.md): summary fields, sealing, and invalidation recovery.
- [`docs/INSTALLATION.md`](INSTALLATION.md): installation, migration, and rollback boundaries.
- [`tests/`](../tests/): the complete isolated regression suite.
- [`spec/user_spec.txt`](../spec/user_spec.txt): the public release specification, not a private original conversation.
