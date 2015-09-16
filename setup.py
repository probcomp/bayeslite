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
import hashlib
import errno
import os
import os.path

root = os.path.dirname(os.path.abspath(__file__))
lemonade = root + '/external/lemonade/dist'

def sha256_file(pathname):
    sha256 = hashlib.sha256()
    with open(pathname, 'r') as f:
        for block in iter(lambda: f.read(65536), ''):
            sha256.update(block)
    return sha256

def uptodate(path_in, path_out, path_sha256):
    try:
        with open(path_sha256, 'r') as file_sha256:
            if file_sha256.next() != sha256_file(path_in).hexdigest():
                return False
            if file_sha256.next() != sha256_file(path_out).hexdigest():
                return False
    except IOError as e:
        if e.errno != errno.ENOENT:
            raise
        return False
    return True

def commit(path_in, path_out, path_sha256):
    with open(path_sha256 + '.tmp', 'w') as file_sha256:
        file_sha256.write('%s\n' % (sha256_file(path_in).hexdigest(),))
        file_sha256.write('%s\n' % (sha256_file(path_out).hexdigest(),))
    os.rename(path_sha256 + '.tmp', path_sha256)

for path_y in grammars:
    path = os.path.splitext(path_y)[0]
    path_py = path + '.py'
    path_sha256 = path + '.sha256'
    if uptodate(path_y, path_py, path_sha256):
        continue
    print 'generating %s -> %s' % (path_y, path_py)
    distutils.spawn.spawn([
        '/usr/bin/env', 'PYTHONPATH=' + lemonade,
        lemonade + '/bin/lemonade',
        '-s',                   # Write statistics to stdout.
        path_y,
    ])
    commit(path_y, path_py, path_sha256)

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
