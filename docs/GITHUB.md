# Uploading this project to GitHub

The extracted `TANGO/` directory is the repository root. It contains the English README, source, tests, documentation, checksum manifests, and a GitHub Actions workflow. This package has not been uploaded to a GitHub account.

## Upload the source files

Create an empty repository in your account, then upload the **contents** of `TANGO/`, preserving the folder structure. `README.md`, `program/`, `tests/`, and `.github/` must appear directly at the repository root. Uploading only the ZIP stores a downloadable archive; it does not expose its contents as a source repository.

Include the hidden `.github/`, `.gitignore`, and `.gitattributes` files. If your file picker hides them, enable hidden files or use Git. On macOS, Command–Shift–Period toggles hidden files in Finder.

Do not add an automatically generated README or `.gitignore` when creating the empty repository; these files are already provided. The source archive did not contain a license. The rights holder can choose and add an appropriate license separately.

## Upload with Git

Run these commands from the extracted `TANGO/` directory. Replace the example repository URL with the actual URL of your empty repository:

```sh
git init
git branch -M main
chmod +x install.sh
git add .
git update-index --chmod=+x install.sh
git commit -m "Add TANGO 0.4.0 autonomous source distribution"
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPOSITORY.git
git push -u origin main
```

Git will use the identity and GitHub authentication configured on your computer. No credentials belong in this repository. If you change any source or documentation before uploading, refresh the manifests as described in [CONTRIBUTING.md](../CONTRIBUTING.md).

## Verify the uploaded project

The [Linux verification workflow](../.github/workflows/ci.yml) is configured to check both checksum manifests, run the full retained and new test suite, and inspect the CLI version with Python 3.10 and 3.13. It uses GitHub's official [checkout](https://github.com/actions/checkout) and [setup-python](https://github.com/actions/setup-python) actions. Inspect the actual workflow results after uploading; a workflow file is not evidence of a completed CI run.

The workflow restores `install.sh`'s executable permission, which browser uploads may not retain. A downloaded checkout can also use `sh install.sh` for installation; direct execution and installer tests need `chmod +x install.sh`.

For a downloadable release, attach the English ZIP and its external `.sha256` file as release assets. The [test report](../TEST_REPORT.md) records what was verified locally and what remains outside the acceptance scope.
