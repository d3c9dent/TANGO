# Getting started with TANGO 0.4.0

TANGO's autonomous runner uses your authenticated local Codex CLI to keep working on a task. It saves progress, retries recoverable failures, and asks a separate session to verify the result against the original goal.

1. Install Python 3.10 or later and a compatible Codex CLI; confirm that `codex login status` succeeds.
2. Install this source package into a fresh home using the commands in [README.md](README.md).
3. Run `tango-auto --codex /absolute/path/to/codex doctor` from the installed `bin/` directory.
4. Submit a task with `tango-auto submit --goal "Your goal"`. It runs in the current terminal unless a runner already owns the home.
5. For background operation, explicitly install the user service with `tango-supervisor --codex /absolute/path/to/codex install`, then submit using `--enqueue-only`.

Use the [autonomous runner guide](docs/AUTONOMOUS.md) for complete commands, service limits, stop controls, and recovery. A real five-agent, ten-round discussion has not yet passed end-to-end acceptance; consult [TEST_REPORT.md](TEST_REPORT.md) for current validation.

There is no fixed retry-count cap. Tasks stop after verified completion or your stop instruction, and wait when permissions, material, account capacity, or uncertain recovery require attention. Running forever and solving every task are not guaranteed.
