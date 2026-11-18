#!/usr/bin/env python3
"""Offline source-package checksums. Integrity checking is not publisher authentication."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = "MANIFEST.sha256"

class PackageError(RuntimeError):
    pass

def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()

def inventory(root):
    root = Path(root)
    result = []
    for relative in ("install.sh", "README.md"):
        p = root / relative
        if p.is_symlink() or not p.is_file():
            raise PackageError(f"Required regular package file missing: {relative}")
        result.append(relative)
    for directory in ("program", "docs", "installer"):
        base = root / directory
        if not base.is_dir() or base.is_symlink():
            raise PackageError(f"Required regular package directory missing: {directory}")
        for p in sorted(base.rglob("*")):
            if "__pycache__" in p.parts or p.suffix == ".pyc":
                continue
            if p.is_symlink():
                raise PackageError(f"Package symlink refused: {p.relative_to(root)}")
            if p.is_file():
                result.append(p.relative_to(root).as_posix())
            elif not p.is_dir():
                raise PackageError(f"Special package file refused: {p.relative_to(root)}")
    for needed in ("program/tango.py", "program/service.py", "installer/install.py", "installer/migration_check.py"):
        if needed not in result:
            raise PackageError(f"Required source missing: {needed}")
    return sorted(result)

def seal(root):
    root = Path(root)
    content = "".join(f"{sha(root / name)}  {name}\n" for name in inventory(root))
    (root / MANIFEST).write_text(content, encoding="utf-8")
    return verify(root)

def verify(root):
    root = Path(root)
    path = root / MANIFEST
    if not path.is_file() or path.is_symlink():
        raise PackageError("Package is not sealed: MANIFEST.sha256 is missing")
    expected = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split("  ", 1)
        if len(parts) != 2 or len(parts[0]) != 64 or any(c not in "0123456789abcdef" for c in parts[0]):
            raise PackageError("Malformed checksum manifest")
        digest, name = parts
        if name in expected or name.startswith("/") or ".." in Path(name).parts:
            raise PackageError("Duplicate or unsafe checksum manifest path")
        expected[name] = digest
    actual = inventory(root)
    if set(actual) != set(expected):
        raise PackageError("Package file inventory differs from checksum manifest")
    for name in actual:
        if sha(root / name) != expected[name]:
            raise PackageError(f"Checksum mismatch: {name}")
    return {"files_verified": len(actual), "manifest_sha256": sha(path)}

def main():
    p = argparse.ArgumentParser(description=__doc__)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--seal", action="store_true", help="Release packaging step; run only after reviewing all source edits")
    g.add_argument("--verify", action="store_true")
    p.add_argument("--package", type=Path, default=ROOT)
    a = p.parse_args()
    try:
        print(json.dumps(seal(a.package) if a.seal else verify(a.package), sort_keys=True))
    except (OSError, PackageError) as e:
        p.exit(2, f"Package check failed: {e}\n")

if __name__ == "__main__":
    main()
