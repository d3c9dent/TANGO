# Portable 0.3.1 verification

- This independent release copy passed all **159 isolated Linux tests**, in 17.607 seconds. See `evidence/TESTS.txt`.
- Fresh isolated installation: `install.sh --check`, `--apply`, installed `tango init` and `tango doctor` all exited successfully. Doctor correctly remains not ready for native acceptance.
- Runtime program, service, three installer modules and install.sh are byte-identical to reviewed 0.3.1 source. Hashes and validation summary: `evidence/VALIDATION.json`.
- Two documentation-fixture test cases changed for the public distribution; runtime protocol assertions were not relaxed. See `docs/RELEASE_NOTES.md`.
- No real project inventory, true five-agent ten-round meeting, platform wake continuity, or receiving-environment deployment is certified by these tests. No live runtime or project data is included.

Full-source integrity is covered by RELEASE_SHA256SUMS; installed payload integrity is covered by MANIFEST.sha256. An independently obtained ZIP SHA256 must be checked before extracting untrusted archives.
