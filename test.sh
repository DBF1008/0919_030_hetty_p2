#!/usr/bin/env bash
#
# test.sh - Manual unit test runner.
#
# Usage:
#   ./test.sh                 # vet + all unit tests
#   ./test.sh pkg/db/bolt     # vet + tests for one package (relative path)
#   ./test.sh -v pkg/reqlog   # extra flags/pkgs are passed to `go test`
#   RACE=1 ./test.sh          # run with the race detector (needs CGO)
#
# Intended to be run manually; not wired into CI.

set -euo pipefail

cd "$(dirname "$0")"

PKGS=("$@")
if [ ${#PKGS[@]} -eq 0 ]; then
	PKGS=("./...")
fi

echo "==> go vet ${PKGS[*]}"
go vet "${PKGS[@]}"

RACE_FLAG=()
if [ "${RACE:-0}" = "1" ]; then
	echo "==> race detector enabled"
	RACE_FLAG=("-race")
fi

echo "==> go test ${PKGS[*]}"
go test -count=1 "${RACE_FLAG[@]}" "${PKGS[@]}"

echo "==> all checks passed"
