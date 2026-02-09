#!/bin/sh
set -eu

OUT=${1:-job_runner_dummy.tar.gz}
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

tar -C "$SCRIPT_DIR" -czf "$OUT" Dockerfile run.sh
echo "wrote $OUT"
