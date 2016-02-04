#!/bin/sh

set -Ceu

: ${PYTHON:=python}

lib_avail=`./pythenv.sh python -m pytest --version 2>&1 | grep 'This is pytest'`
if [ -z "$lib_avail" ]; then
    printf >&2 'Unable to find pytest.\n'
    exit 1
fi

root=`cd -- "$(dirname -- "$0")" && pwd`

(
    set -Ceu
    cd -- "${root}"
    rm -rf build
    "$PYTHON" setup.py build
    export BAYESDB_WIZARD_MODE=1
    export BAYESDB_DISABLE_VERSION_CHECK=1
    export MPLBACKEND=pdf
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
