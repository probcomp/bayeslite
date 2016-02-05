#!/bin/sh

# Copyright (c) 2010-2016, MIT Probabilistic Computing Project.
#
# This file is part of Venture.
#
# Venture is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Venture is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Venture.  If not, see <http://www.gnu.org/licenses/>.

# Prepare Plex 1.1.5 for import:
# - Delete extraneous files.
# - Translate carriage return to line feed.
# - Make sure the tests still run.

set -Ceu

: ${PYTHON:=python}

if [ ! -d Plex -o ! -f "$(printf 'Icon\r')" ]; then
    printf >&2 'Usage: %s\n' "$0"
    printf >&2 '  Run within the Plex distribution directory.\n'
    exit 1
fi

# Remove Mac OS X junk.
find . -type f \( -name .DS_Store -o -name '._*' \) -exec rm -f '{}' ';'

# Remove empty file with CR in its name.
rm -f -- "$(printf 'Icon\r')"

# Remove Mac OS Classic(???) junk.
rm -f -- tests/PythonInterpreter

# Convert CR to LF.  All remaining files should be plain text.
find . -type f -exec sh -c '
    tr "\\r" "\\n" < "$1" > "$1".tmp && mv -f "$1".tmp "$1"
' -- '{}' ';'

# Make sure the tests still run.  Avoid generating .pyc and .pyo files
# by passing -B to Python.
PYTHONPATH="`pwd`" \
    sh -c 'cd tests && exec "$1" -B runtests.py' -- "$PYTHON"
