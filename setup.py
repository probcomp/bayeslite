# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2014, MIT Probabilistic Computing Project
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

# XXX This is a mega-kludge.  Since distutils/setuptools has no way to
# order dependencies (what kind of brain-dead build system can't do
# this?), we just always regenerate the grammar.  Could hack
# distutils.command.build to include a dependency mechanism, but this
# is more expedient for now.
grammars = [
    'src/grammar.y',
]

import distutils.spawn
import errno
import os
import os.path
root = os.path.dirname(os.path.abspath(__file__))
lemonade = root + '/external/lemonade/dist'
for grammar in grammars:
    parser = os.path.splitext(grammar)[0] + '.py'
    parser_mtime = None
    try:
        parser_mtime = os.path.getmtime(parser)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
    if parser_mtime is not None:
        if os.path.getmtime(grammar) < parser_mtime:
            continue
    print 'generating %s -> %s' % (grammar, parser)
    distutils.spawn.spawn([
        '/usr/bin/env', 'PYTHONPATH=' + lemonade,
        lemonade + '/bin/lemonade',
        '-s',                   # Write statistics to stdout.
        grammar,
    ])

setup(
    name='bayeslite',
    version='0.0.dev',
    packages=[
        'bayeslite',
        'bayeslite.plex',
        'bayeslite.shell',
    ],
    package_dir={
        'bayeslite': 'src',
        'bayeslite.plex': 'external/plex/dist/Plex',
        'bayeslite.shell': 'shell',
    },
    scripts=['scripts/bayeslite'],
    license='Apache License, Version 2.0',
)
