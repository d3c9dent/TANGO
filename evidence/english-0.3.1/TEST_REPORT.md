# English GitHub edition: verification report

Verification date: 2026-11-17 (Asia/Shanghai).

## Checks run for this edition

- On macOS with Python 3.13.9, **153 of 153 selected tests passed** in 5.633 seconds. All 159 retained tests were discovered; six Linux `/proc` lifecycle tests were explicitly excluded. None of those six is claimed as passed here.
- The complete local output is in [evidence/TESTS.txt](TESTS.txt), with counts and exact exclusions in [evidence/LOCAL_TEST_SUMMARY.json](LOCAL_TEST_SUMMARY.json).
- A fresh temporary-home smoke check passed: installer `--check`, installer `--apply`, installed CLI `--version`, `init`, and `doctor`. No scheduler was started. `doctor` correctly reported `ready: false`. See [evidence/INSTALL_SMOKE.json](INSTALL_SMOKE.json).
- Runtime, service, all installer modules, shell entry point contents, migration baseline, and public specification were compared byte-for-byte with the supplied archive. All eight files are unchanged. See [evidence/SOURCE_PROVENANCE.json](SOURCE_PROVENANCE.json).
- Python syntax and relative Markdown links were checked. Both checksum manifests were regenerated for the English source distribution.

Reproduce the local subset with Python 3.10 or later on a POSIX host:

```sh
chmod +x install.sh
python3 -B scripts/run_tests.py --portable
```

Use Linux for the full suite:

```sh
chmod +x install.sh
python3 -B installer/package.py --verify
python3 -B -m unittest discover -s tests -v
```

The GitHub Actions workflow is configured to run the full suite on Linux with Python 3.10 and 3.13. It has not been run on GitHub during this preparation, and this edition is not claimed to have a new full Linux pass.

## Historical evidence

The supplied archive reports 159 isolated Linux tests passed in 17.607 seconds. Its log, validation summary, report, and release notes are preserved without modification under [evidence/upstream/](../upstream/). Those records were supplied with the original archive and were not independently reproduced here.

The README documentation test was updated for the English epigraph, entry link, and acceptance limitations. All runtime assertions, Chinese command compatibility cases, and the byte-exact public specification check remain intact.

## Acceptance limits

Local tests and installation do not certify real native agents, an actual five-agent ten-round discussion, message delivery, persistent wake behavior, live migration, or deployment in another environment. No live runtime, private project data, or credentials are included.

`MANIFEST.sha256` covers the installation payload; `RELEASE_SHA256SUMS` covers the source distribution except itself. Hashes detect changes and are not publisher signatures.
