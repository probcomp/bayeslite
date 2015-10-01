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
        ./pythenv.sh "$PYTHON" "$PY_TEST" -k "not _slow" tests shell/tests
    else
        ./pythenv.sh "$PYTHON" "$PY_TEST" "$@"
    fi
)
