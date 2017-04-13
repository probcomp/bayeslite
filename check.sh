#!/bin/sh

set -Ceu

: ${PYTHON:=python}

root=`cd -- "$(dirname -- "$0")" && pwd`

(
    set -Ceu
    cd -- "${root}"
    rm -rf build
    "$PYTHON" setup.py build
    if [ $# -eq 0 ]; then
        # By default, when running all tests, skip tests that have
        # been marked for continuous integration by using __ci_ in
        # their names.  (git grep __ci_ to find these.)
        ./pythenv.sh "$PYTHON" -m pytest --pyargs bayeslite -k "not __ci_"
    else
        # If args are specified, run all tests, including continuous
        # integration tests, for the selected components.
        ./pythenv.sh "$PYTHON" -m pytest "$@"
    fi
)
