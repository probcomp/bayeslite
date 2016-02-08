#!/bin/sh

set -Ceu

: ${PYTHON:=python}

root=`cd -- "$(dirname -- "$0")" && pwd`

function ensure_pytest() {
    ok='This is pytest'
    if ! ./pythenv.sh "$PYTHON" -m pytest --version 2>&1 | grep -q "$ok"; then
        printf >&2 'ERROR: Unable to find pytest. Will not run tests.\n'
        exit 1
    fi
}

(
    set -Ceu
    cd -- "${root}"
    rm -rf build
    "$PYTHON" setup.py build
    export BAYESDB_WIZARD_MODE=1
    export BAYESDB_DISABLE_VERSION_CHECK=1
    ensure_pytest
    if [ $# -eq 0 ]; then
        # By default, when running all tests, skip tests that have
        # been marked for continuous integration by using __ci_ in
        # their names.  (git grep __ci_ to find these.)
        ./pythenv.sh "$PYTHON" -m pytest -k "not __ci_" \
            tests shell/tests
    else
        # If args are specified, run all tests, including continuous
        # integration tests, for the selected components.
        ./pythenv.sh "$PYTHON" -m pytest "$@"
    fi
)
