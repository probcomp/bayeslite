#!/bin/sh

set -Ceu

: ${PYTHON:=python}
: ${PY_TEST:=py.test}

root=`cd -- "$(dirname -- "$0")" && pwd`
platform=`"${PYTHON}" -c 'import distutils.util as u; print u.get_platform()'`
version=`"${PYTHON}" -c 'import sys; print sys.version[0:3]'`

bayeslite="${root}/build/lib.${platform}-${version}"
export PYTHONPATH="${bayeslite}${PYTHONPATH:+:${PYTHONPATH}}"

(
    set -Ceu
    cd -- "${root}"
    rm -rf build
    "$PYTHON" setup.py build
    if [ $# -eq 0 ]; then
        "$PY_TEST" tests shell/tests
    else
        "$PY_TEST" "$@"
    fi
)
