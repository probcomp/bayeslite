#!/bin/sh

set -Ceu

: ${PYTHON:=python}
: ${PY_TEST:=`which py.test`}

if [ ! -x "${PY_TEST}" ]; then
    printf >&2 'unable to find pytest\n'
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
    if [ $# -eq 0 ]; then
        # By default, when running all tests, skip tests that have
        # been marked for continuous integration by using __ci_ in
        # their names.  (git grep __ci_ to find these.)
        ./pythenv.sh "$PYTHON" "$PY_TEST" -k "not __ci_" \
            tests shell/tests
    else
        # If args are specified, run all tests, including continuous
        # integration tests, for the selected components.
        ./pythenv.sh "$PYTHON" "$PY_TEST" "$@"
    fi
)
