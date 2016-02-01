# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2015, MIT Probabilistic Computing Project
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
    from setuptools.command.build_py import build_py
    from setuptools.command.sdist import sdist
    from setuptools.command.test import test
except ImportError:
    from distutils.core import setup
    from distutils.cmd import Command
    from distutils.command.build_py import build_py
    from distutils.command.sdist import sdist

    class test(Command):
        def __init__(self, *args, **kwargs):
            Command.__init__(self, *args, **kwargs)
        def initialize_options(self): pass
        def finalize_options(self): pass
        def run(self): self.run_tests()
        def run_tests(self): Command.run_tests(self)
        def set_undefined_options(self, opt, val):
            Command.set_undefined_options(self, opt, val)

def get_version():
    with open('VERSION', 'rb') as f:
        version = f.read().strip()

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

    return version

version = get_version()

def write_version_py(path):
    try:
        with open(path, 'rb') as f:
            version_old = f.read()
    except IOError:
        version_old = None
    version_new = '__version__ = %r\n' % (version,)
    if version_old != version_new:
        print 'writing %s' % (path,)
        with open(path, 'wb') as f:
            f.write(version_new)

def sha256_file(pathname):
    import hashlib
    sha256 = hashlib.sha256()
    with open(pathname, 'rb') as source_file:
        for block in iter(lambda: source_file.read(65536), ''):
            sha256.update(block)
    return sha256

def uptodate(path_in, path_out, path_sha256):
    import errno
    try:
        with open(path_sha256, 'rb') as file_sha256:
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
    import os
    with open(path_sha256 + '.tmp', 'wb') as file_sha256:
        file_sha256.write('%s\n' % (sha256_file(path_in).hexdigest(),))
        file_sha256.write('%s\n' % (sha256_file(path_out).hexdigest(),))
    os.rename(path_sha256 + '.tmp', path_sha256)

def generate_parser(lemonade, path_y):
    import distutils.spawn
    import os.path
    root = os.path.dirname(os.path.abspath(__file__))
    lemonade = os.path.join(root, *lemonade.split('/'))
    base, ext = os.path.splitext(path_y)
    assert ext == '.y'
    path_py = base + '.py'
    path_sha256 = base + '.sha256'
    if uptodate(path_y, path_py, path_sha256):
        return
    print 'generating %s -> %s' % (path_y, path_py)
    distutils.spawn.spawn([
        '/usr/bin/env', 'PYTHONPATH=' + lemonade,
        lemonade + '/bin/lemonade',
        '-s',                   # Write statistics to stdout.
        path_y,
    ])
    commit(path_y, path_py, path_sha256)

class local_build_py(build_py):
    def run(self):
        write_version_py(version_py)
        for grammar in grammars:
            generate_parser(lemonade, grammar)
        build_py.run(self)

# XXX For inexplicable reasons, during sdist.run, setuptools quietly
# modifies self.distribution.metadata.version to replace plus signs by
# hyphens -- even where they are explicitly allowed by PEP 440.
# distutils does not do this -- only setuptools.
class local_sdist(sdist):
    # This is not really a subcommand -- it's actually a predicate to
    # determine whether to run a subcommand.  So modifying anything in
    # it is a little evil.  But it'll do.
    def fixidioticegginfomess(self):
        self.distribution.metadata.version = version
        return False
    sub_commands = [('sdist_fixidioticegginfomess', fixidioticegginfomess)]

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
class local_test(test):
    def __init__(self, *args, **kwargs):
        test.__init__(self, *args, **kwargs)
        self.test_suite = ' '.join(test_directories)
        self.build_lib = None
    def finalize_options(self):
        test.finalize_options(self)
        # self.build_lib = ...
        self.set_undefined_options('build', ('build_lib', 'build_lib'))
    def run_tests(self):
        import pytest
        import sys
        sys.path = [os.path.join(os.getcwd(), self.build_lib)] + sys.path
        os.environ['BAYESDB_WIZARD_MODE'] = '1'
        os.environ['BAYESDB_DISABLE_VERSION_CHECK'] = '1'
        os.environ['PYTHONPATH'] = ':'.join(sys.path)
        sys.exit(pytest.main(test_directories))

# XXX These should be attributes of `setup', but helpful distutils
# doesn't pass them through when it doesn't know about them a priori.
version_py = 'src/version.py'
lemonade = 'external/lemonade/dist'
grammars = [
    'src/grammar.y',
]
test_directories = [
    'shell/tests',
    'tests',
]

setup(
    name='bayeslite',
    version=version,
    description='BQL database built on SQLite3',
    url='http://probcomp.csail.mit.edu/bayesdb',
    author='MIT Probabilistic Computing Project',
    author_email='bayesdb@mit.edu',
    license='Apache License, Version 2.0',
    install_requires=[
        'bayeslite-apsw>=3.8.0',
        'crosscat>=0.1.48',
        'jsonschema',
        'numpy',
        'requests',
    ],
    tests_require=[
        'pandas',
        'pexpect',
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
        'build_py': local_build_py,
        'sdist': local_sdist,
        'test': local_test,
    },
    package_data={
        'bayeslite.metamodels': ['*.schema.json'],
    },
)
