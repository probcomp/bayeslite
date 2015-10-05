#!/bin/sh

set -Ceu

: ${PYTHON:=python}
: ${PY_TEST:=`which py.test`}
: ${PY_LINT:=`which pylint`}

if [ ! -x "${PY_TEST}" ]; then
    printf >&2 'unable to find pytest\n'
    exit 1
fi

root=`cd -- "$(dirname -- "$0")" && pwd`

(
    set -Ceu
    cd -- "${root}"
    rm -rf build
    "$PYTHON" setup.py build > /dev/null
    export BAYESDB_WIZARD_MODE=1
    export BAYESDB_DISABLE_VERSION_CHECK=1
    if [ $# -eq 0 ]; then
        ./pythenv.sh "$PYTHON" "$PY_TEST" -k "not _slow" tests shell/tests
    elif [ "$1" == "cov" ]; then
        ./pythenv.sh "$PYTHON" "$PY_TEST" --cov=bayeslite tests shell/tests
    elif [ "$1" == "lint" ]; then
        if [ $# -eq 1 ]; then
            pyfiles=`find . -name "*.py" -and -not -path "./external/*" -and -not -path "./build/*"`
            ./pythenv.sh "$PYTHON" "$PY_LINT" --rcfile=tests/pylintrc -E $pyfiles
        else
            shift
            pythenv.sh "$PYTHON" "$PY_LINT" --rcfile=tests/pylintrc "$@"
        fi
    else
        ./pythenv.sh "$PYTHON" "$PY_TEST" "$@"
    fi
)
