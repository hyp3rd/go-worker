#!/bin/sh
set -eu

echo "dummy job starting"
echo "env DUMMY_SHOULD_FAIL=${DUMMY_SHOULD_FAIL:-1}"

if [ "${DUMMY_SHOULD_FAIL:-1}" = "1" ]; then
  echo "dummy job failing intentionally" >&2
  exit 42
fi

echo "dummy job succeeded"
