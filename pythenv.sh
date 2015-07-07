#!/bin/sh

set -Ceu

: ${PYTHON:=python}
root=`cd -- "$(dirname -- "$0")" && pwd`
platform=`"${PYTHON}" -c 'import distutils.util as u; print u.get_platform()'`
version=`"${PYTHON}" -c 'import sys; print sys.version[0:3]'`

libdir="${root}/build/lib.${platform}-${version}"
export PYTHONPATH="${libdir}${PYTHONPATH:+:${PYTHONPATH}}"

bindir="${root}/build/scripts-${version}"
export PATH="${bindir}${PATH:+:${PATH}}"

exec "$@"
