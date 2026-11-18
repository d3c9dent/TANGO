#!/bin/sh
# Source-only, offline installer; Python standard library is the only dependency.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec python3 -B "$HERE/installer/install.py" "$@"
