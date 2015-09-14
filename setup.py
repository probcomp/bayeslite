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

with open('VERSION', 'rU') as f:
    version = f.readline().strip()

# Append the Git commit id if this is a development version.
if version.endswith('+'):
    tag = 'v' + version[:-1]
    try:
        import subprocess
        desc = subprocess.check_output([
            'git', 'describe', '--dirty', '--match', tag,
        ])
    except Exception:
        version += 'unknown'
    else:
        assert desc.startswith(tag)
        version = desc[1:].strip()

# XXX Mega-kludge.  See below about grammars for details.
try:
    with open('src/version.py', 'rU') as f:
        version_old = f.readlines()
except IOError:
    version_old = None
version_new = ['__version__ = %s\n' % (repr(version),)]
if version_old != version_new:
    with open('src/version.py', 'w') as f:
        f.writelines(version_new)

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
    version=version,
    install_requires=[
        'crosscat>=0.1.24',
    ],
    packages=[
        'bayeslite',
        'bayeslite.metamodels',
        'bayeslite.plex',
        'bayeslite.shell',
    ],
    package_dir={
        'bayeslite': 'src',
        'bayeslite.plex': 'external/plex/dist/Plex',
        'bayeslite.shell': 'shell/src',
    },
    # Not in this release, perhaps later.
    #scripts=['shell/scripts/bayeslite'],
    license='Apache License, Version 2.0',
)
