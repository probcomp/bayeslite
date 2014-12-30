#!/bin/sh

set -Ceu

: ${PYTHON:=python}
: ${PY_TEST:=py.test}

root=`cd -- "$(dirname -- "$0")" && pwd`
platform=`"${PYTHON}" -c 'import distutils.util as u; print u.get_platform()'`
version=`"${PYTHON}" -c 'import sys; print sys.version[0:3]'`

export PYTHONPATH="${root}/build/lib.${platform}-${version}"

(
    set -Ceu
    cd -- "${root}"
    "$PYTHON" setup.py build
    cd tests
    "$PY_TEST"
)
