# Contributing to TANGO

Use Python 3.10 or later. The Python code uses the standard library. Real autonomous execution additionally requires an installed, authenticated Codex CLI. Tests use clearly labeled synthetic CLI subprocesses and do not require a model account. Use Linux for the full test suite.

Keep runtime homes outside the source tree. Test only with fresh synthetic homes and retain the distinction between simulation, protocol validation, and real native-agent acceptance. Never include credentials, private conversations, project results, or a live database in a contribution.

## Validate a change

From the repository root on Linux:

```sh
chmod +x install.sh
python3 -B -m unittest discover -s tests -v
python3 -B program/tango.py --version
```

Preserve task goals, independent verification, pause/stop priority, cancellation reconciliation, and source provenance. The public specification in `spec/user_spec.txt` is checked byte-for-byte. Changes to that contract need explicit review; changing a test expectation does not establish the new behavior.

On macOS, run the limited local subset with `python3 -B scripts/run_tests.py --portable`. The runner lists the six excluded Linux process-identity tests and runs the remaining tests, including the new autonomous regression cases. This is not a full-suite pass or evidence of macOS service support.

Documentation and output are in English. Chinese command aliases and multilingual fixtures intentionally test backward compatibility and exact preservation of user input.

## Update release checksums

Checksums describe a particular source revision. After reviewing your changes and running the tests, regenerate the installation manifest. Stage the files intended for the next release, then regenerate the full-source checksums from Git's tracked file list:

```sh
python3 -B installer/package.py --seal
git add .
python3 -B - <<'PY'
import hashlib
import pathlib
import subprocess

names = subprocess.check_output(['git', 'ls-files', '-z']).decode('utf-8').split('\0')
paths = sorted(name for name in names if name and name != 'RELEASE_SHA256SUMS')
lines = [f'{hashlib.sha256(pathlib.Path(name).read_bytes()).hexdigest()}  {name}\n'
         for name in paths]
pathlib.Path('RELEASE_SHA256SUMS').write_text(''.join(lines), encoding='utf-8')
PY
git add MANIFEST.sha256 RELEASE_SHA256SUMS
python3 -B installer/package.py --verify
sha256sum --check RELEASE_SHA256SUMS
```

Review the staged files before committing. On macOS, `shasum -a 256 -c RELEASE_SHA256SUMS` can check the full-source hashes, but it cannot replace the Linux lifecycle tests. Do not reseal a downloaded release just to hide an unexplained mismatch.

Describe what changed, why, and the tests actually run in a pull request. Retain historical test evidence as historical; do not present a previous release's results as a fresh run. The supplied source archive did not contain a license, and this edition does not choose one on behalf of the rights holder.
