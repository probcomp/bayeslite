#!/bin/sh

set -Ceu

: ${PYTHON:=python}

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
        ./pythenv.sh "$PYTHON" -m pytest -k "not __ci_" \
                     tests shell/tests
    elif [ "docker" = "$1" ]; then
        shift
        echo "WARNING: Possible version skew, because using crosscat from pypi."
        # XXX: Leave all docker-based testing to packaging, where we have info
        #      about corresponding versions that we don't have here.
        python setup.py sdist
        docker build -f tests/Dockerfile -t bayeslite-test-15-10 "$@" .
        docker build -f tests/LTS-Dockerfile -t bayeslite-test-14-04 "$@" .
    else
        # If args are specified, run all tests, including continuous
        # integration tests, for the selected components.
        ./pythenv.sh "$PYTHON" -m pytest "$@"
    fi
)
