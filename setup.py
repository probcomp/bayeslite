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

# If some modules are not found, we use others, so no need to warn:
# pylint: disable=import-error
try:
    from setuptools import setup
    from setuptools.command.test import test as TestCommand
except ImportError:
    from distutils.core import setup
    from distutils.cmd import Command
    class TestCommand(Command):
        def __init__(self, *args, **kwargs):
            Command.__init__(self, *args, **kwargs)
        def initialize_options(self): pass
        def finalize_options(self): pass
        def run(self): self.run_tests()
        def run_tests(self): Command.run_tests(self)
        def set_undefined_options(self, opt, val):
            Command.set_undefined_options(self, opt, val)


with open('VERSION', 'rU') as version_file:
    version = version_file.readline().strip()

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
        import re
        match = re.match(r'v([^-]*)-([0-9]+)-(.*)$', desc)
        if match is None:       # paranoia
            version += 'unknown'
        else:
            ver, rev, local = match.groups()
            version = '%s.post%s+%s' % (ver, rev, local.replace('-', '.'))
            assert '-' not in version

# XXX Mega-kludge.  See below about grammars for details.
try:
    with open('src/version.py', 'rU') as version_pyfile:
        version_old = version_pyfile.readlines()
except IOError:
    version_old = None
version_new = ['__version__ = %s\n' % (repr(version),)]
if version_old != version_new:
    with open('src/version.py', 'w') as version_pyfile:
        version_pyfile.writelines(version_new)

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
    with open(pathname, 'r') as source_file:
        for block in iter(lambda: source_file.read(65536), ''):
            sha256.update(block)
    return sha256

def uptodate(path_in, path_out, path_sha256):
    try:
        with open(path_sha256, 'r') as file_sha256:
            # Strip newlines and compare.
            if file_sha256.next()[:-1] != sha256_file(path_in).hexdigest():
                return False
            if file_sha256.next()[:-1] != sha256_file(path_out).hexdigest():
                return False
    except (IOError, OSError) as e:
        if e.errno != errno.ENOENT:
            raise
        return False
    return True

def commit(path_in, path_out, path_sha256):
    with open(path_sha256 + '.tmp', 'w') as file_sha256:
        file_sha256.write('%s\n' % (sha256_file(path_in).hexdigest(),))
        file_sha256.write('%s\n' % (sha256_file(path_out).hexdigest(),))
    os.rename(path_sha256 + '.tmp', path_sha256)

def run_lemonade_on_grammars(grammar_paths):
    for path_y in grammar_paths:
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

run_lemonade_on_grammars(grammars)

# XXX Several horrible kludges here to make `python setup.py test' work:
#
# - Standard setputools test command searches for unittest, not
#   pytest.
#
# - pytest suggested copypasta assumes . works in sys.path; we
#   deliberately make . not work in sys.path and require ./build/lib
#   instead, in order to force a clean build.
#
# - Must set PYTHONPATH too for shell tests, which fork and exec a
#   subprocess which inherits PYTHONPATH but not sys.path.
#
# - build command's build_lib variable is relative to source
#   directory, so we must assume os.getcwd() gives that.
class cmd_pytest(TestCommand):
    def __init__(self, *args, **kwargs):
        TestCommand.__init__(self, *args, **kwargs)
        self.test_suite = 'tests shell/tests'
        self.build_lib = None
    def finalize_options(self):
        TestCommand.finalize_options(self)
        # self.build_lib = ...
        self.set_undefined_options('build', ('build_lib', 'build_lib'))
    def run_tests(self):
        import pytest
        import sys
        sys.path = [os.path.join(os.getcwd(), self.build_lib)] + sys.path
        os.environ['BAYESDB_WIZARD_MODE'] = '1'
        os.environ['BAYESDB_DISABLE_VERSION_CHECK'] = '1'
        os.environ['PYTHONPATH'] = ':'.join(sys.path)
        sys.exit(pytest.main(['tests', 'shell/tests']))

setup(
    name='bayeslite',
    version=version,
    description='BQL database built on SQLite3',
    url='http://probcomp.csail.mit.edu/bayesdb',
    author='MIT Probabilistic Computing Project',
    author_email='bayesdb@mit.edu',
    license='Apache License, Version 2.0',
    install_requires=[
        'crosscat>=0.1.29',
        'requests',
    ],
    tests_require=[
        'numpy',
        'pytest',
    ],
    packages=[
        'bayeslite',
        'bayeslite.metamodels',
        'bayeslite.plex',
        'bayeslite.shell',
        'bayeslite.weakprng',
    ],
    package_dir={
        'bayeslite': 'src',
        'bayeslite.plex': 'external/plex/dist/Plex',
        'bayeslite.shell': 'shell/src',
        'bayeslite.weakprng': 'external/weakprng/dist',
    },
    # Not in this release, perhaps later.
    #scripts=['shell/scripts/bayeslite'],
    cmdclass={
        'test': cmd_pytest,
    },
)
