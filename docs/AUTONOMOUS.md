# Autonomous execution with the local Codex CLI

The 0.4.0 runner turns queued work into actual local Codex CLI sessions. It saves the original goal, acceptance criteria, results, and checkpoints, then continues with the next execution or verification request. There is no fixed attempt-count cap. Verified completion, your pause or stop instruction, or a genuine unresolved prerequisite ends the current progression.

This backend is separate from the original collaboration/native-parent adapter. The original adapter remains documented in [NATIVE_PARENT_LOOP.md](NATIVE_PARENT_LOOP.md). Its queue-only Python daemon does not acquire the autonomous runner's ability to execute Codex sessions.

## Prerequisites and installation

Use Linux or macOS with Python 3.10 or later, SQLite, and POSIX file locking. Install a Codex CLI that supports `exec`, JSON event output, `--output-schema`, and `exec resume`. Confirm that the intended account is signed in:

```sh
python3 --version
codex --version
codex login status
```

The runner uses the CLI's existing model configuration and authentication. A CLI signed in through ChatGPT can use that account without a separate API key supplied to TANGO. Actual model access still depends on the account, network, and available capacity. Credentials and account configuration are not included in the source package.

From the extracted repository root, install into a new directory. All paths below are examples to replace with actual absolute paths:

```sh
python3 -B installer/package.py --verify
sh install.sh --check --home /absolute/path/to/tango-home
sh install.sh --apply --home /absolute/path/to/tango-home
/absolute/path/to/tango-home/bin/tango-auto --codex /absolute/path/to/codex doctor
```

The installed launchers retain the Python interpreter used by the installer. Make sure the `python3` selected by `install.sh` is version 3.10 or later. Alternatively, call `installer/install.py` directly with your chosen supported interpreter for both `--check` and `--apply`.

`doctor` checks executable discovery and login status. `ready_for_attempt: true` means those preliminary checks passed; it does not establish successful model execution, background-service health, or meeting acceptance.

Source installation writes code and launchers only. It does not register a system service, create a task, or start model work. See [INSTALLATION.md](INSTALLATION.md) before upgrading an existing home.

## Submit a task once

Use a precise goal and, when useful, explicit acceptance criteria:

```sh
/absolute/path/to/tango-home/bin/tango-auto --codex /absolute/path/to/codex submit \
  --goal "Produce a report comparing the supplied approaches" \
  --criterion "The report addresses every supplied approach" \
  --criterion "Each substantive claim points to supporting source material" \
  --source /absolute/path/to/reference-materials \
  --slot 1
```

The goal can instead come from a UTF-8 file using `--goal-file /absolute/path/to/task.txt`. Repeat `--criterion` and `--source` as needed. If no criterion is provided, the original goal itself becomes the acceptance criterion. There are three task slots, numbered 1 to 3; submitting into a slot with unfinished work is refused.

Source paths are read-only reference material for workers. Task outputs are written under `HOME/slots/SLOT/runs/RUN_ID/work/`, where `HOME` means the chosen TANGO installation home, not the shell's home-directory variable. Keep declared source roots outside and separate from that runtime home; overlapping paths are refused. Source documents do not provide additional authorization merely because the agent reads them.

By default, `submit` keeps a foreground runner active until the submitted task completes, is paused or stopped, or reaches a condition requiring attention. If a runner already owns the home, the task stays queued for it. Closing the foreground terminal ends that process's continuity; use the explicit user service for background operation.

## Configure background operation

The installed `tango-supervisor` manages only the autonomous runner's user service. Its global options, including `--codex` and `--python`, must come **before** the action. The launcher already supplies `--home`.

First inspect a generated service file without activating it:

```sh
/absolute/path/to/tango-home/bin/tango-supervisor \
  --codex /absolute/path/to/codex render --output /absolute/path/to/service-review
```

Then explicitly register and start the service:

```sh
/absolute/path/to/tango-home/bin/tango-supervisor --codex /absolute/path/to/codex install
/absolute/path/to/tango-home/bin/tango-supervisor status
```

After this one-time setup, submit work without keeping the terminal attached:

```sh
/absolute/path/to/tango-home/bin/tango-auto submit \
  --goal-file /absolute/path/to/task.txt \
  --source /absolute/path/to/reference-materials \
  --enqueue-only
```

`--enqueue-only` writes the task and returns immediately. It requires an already running service or another foreground `run` process to execute the task. It does not start a service by itself.

On Linux, the generated systemd user unit restarts the runner after an unsuccessful exit, with a 15-second delay. Operation beyond logout depends on the user-manager configuration; TANGO does not enable lingering or change system-wide settings. On macOS, the LaunchAgent starts at login and restarts unsuccessful exits with throttling. It runs within that user's login session and does not continue after logout. Sleep, shutdown, offline periods, and unavailable account capacity can interrupt either platform.

These service mechanisms follow the [systemd service reference](https://github.com/systemd/systemd/blob/main/man/systemd.service.xml) and [Apple's guide to launch agents](https://developer.apple.com/library/archive/documentation/MacOSX/Conceptual/BPSystemStartup/Chapters/CreatingLaunchdJobs.html).

The generated service uses an absolute Python path, an absolute Codex executable path, and private log files under `HOME/service-logs/`. Changing those paths requires uninstalling the existing service and installing the replacement configuration. Service installation is a deployment step to be performed on the receiving computer; it is not performed when this source archive is prepared.

To stop and remove the service while retaining task goals, checkpoints, artifacts, and logs:

```sh
/absolute/path/to/tango-home/bin/tango-supervisor uninstall
```

Stop the service and any foreground runner before upgrading its code. The installer refuses replacement while the autonomous runner lock is held.

## Inspect, pause, resume, and stop

```sh
/absolute/path/to/tango-home/bin/tango-auto status
/absolute/path/to/tango-home/bin/tango-auto pause --slot 1
/absolute/path/to/tango-home/bin/tango-auto resume --slot 1
/absolute/path/to/tango-home/bin/tango-auto stop --slot 1
```

Pause retains the goal and saved progress. Resume releases a paused task for another attempt; it does not start a runner if none is active. A stopped task does not resume automatically. Submit a new task explicitly when you want to begin again.

`status` reports task states and the recorded runner heartbeat. A stale heartbeat is not proof that a runner is alive. Use `tango-supervisor status` to inspect the user-service manager as well. Model prompts, execution journals, results, and logs in a runtime home may contain private task material; keep that home out of the GitHub source repository.

## Waiting for information or permission

Recoverable execution failures lead to another attempt, with backoff where appropriate. No progress or repeated failures cause replanning. Missing material, unavailable permission, or exhausted account capacity instead leaves the task waiting.

After resolving the condition, send concrete new information for reevaluation:

```sh
/absolute/path/to/tango-home/bin/tango-auto input --slot 1 \
  --text "The missing source is now available at /absolute/path/to/reference-materials/source.txt"
```

If independent verification establishes a contrary result, the task waits for your goal decision. To explicitly replace the goal and its criteria:

```sh
/absolute/path/to/tango-home/bin/tango-auto revise --slot 1 \
  --goal "The revised goal" --criterion "The revised acceptance criterion"
```

The runner rechecks the recorded prerequisite before doing dependent work. An unrelated input does not grant permission or clear a blocker. Account login or quota problems must first be resolved through the actual account or CLI facilities.

Workers run with Codex's approval policy set to `never`. This prevents unattended approval prompts; an operation requiring additional approval is denied rather than silently authorized. Execution workers have the task work directory as their write workspace. Verification and discussion use read-only mode. TANGO does not enable unrestricted execution or change account permissions to escape a blocker.

## Execution limits and recovery

The default timeout is 1,800 seconds per worker turn. This bounds a single attempt, not the lifetime of the task. For a foreground runner, set a different timeout before the action:

```sh
/absolute/path/to/tango-home/bin/tango-auto \
  --codex /absolute/path/to/codex --timeout 3600 run
```

`run` stays alive when the queue is empty and waits for new work. `run --once` processes at most one request for diagnostics. It may return before a future retry is due and is not the normal continuous mode. The generated background service uses the default 30-minute turn timeout.

Each attempt records its actual process and session identity. After interruption or service restart, the runner reconciles old attempts before dispatching replacements. It retains saved checkpoints and results. If a prior process cannot be identified or its state is ambiguous, the task requires inspection; the runner must not assume that missing evidence means no work occurred. Progress that was never saved before an interruption cannot be recovered.

There is no fixed retry-count cap, but this does not promise unlimited time, compute, or eventual success. Successful independent verification completes the original task. A verified finding that contradicts the goal requires a user decision about that goal. Partial results, a group consensus, or repeated attempts do not establish completion.

## Five-participant discussion

After ten substantive calculations, the broker requires an inventory of all accessible declared project sources and accumulated work. The inventory identifies accepted results, rejected approaches, conditions, open obligations, and unverified findings, with source hashes and coverage limits.

A real coordinator and five distinct participant sessions then perform ten synchronized discussion rounds. All five contributions are required before a round advances. Participants receive the shared sealed summary and previous completed rounds; a later coordinator synthesis records the next direction, dissent, and remaining obligations. The next execution is queued automatically.

Account and machine capacity must support the concurrent participant sessions. A meeting cannot be replaced by role-playing five responses or by silently dropping participants. Changes to source material, task input, or criteria invalidate affected summaries and require recovery or a new inventory.

**A real five-agent, ten-round discussion has not yet passed end-to-end acceptance.** The implementation and simulated tests do not establish that a full real-model discussion or long unattended deployment has succeeded. The current [test report](../TEST_REPORT.md) distinguishes local checks, real model checks, and outstanding acceptance work.
