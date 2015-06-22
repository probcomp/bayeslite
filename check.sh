#!/bin/sh

set -Ceu

: ${PYTHON:=python}
: ${PY_TEST:=`which py.test`}

root=`cd -- "$(dirname -- "$0")" && pwd`

(
    set -Ceu
    cd -- "${root}"
    rm -rf build
    ./pythenv.sh "$PYTHON" setup.py build
    if [ $# -eq 0 ]; then
        ./pythenv.sh "$PYTHON" "$PY_TEST" tests
    else
        ./pythenv.sh "$PYTHON" "$PY_TEST" "$@"
    fi
)
