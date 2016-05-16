# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2016, MIT Probabilistic Computing Project
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
        import re
        import subprocess
        version = version[:-1]
        tag = 'v' + version
        desc = subprocess.check_output([
            'git', 'describe', '--dirty', '--long', '--match', tag,
        ])
        match = re.match(r'^v([^-]*)-([0-9]+)-(.*)$', desc)
        assert match is not None
        verpart, revpart, localpart = match.groups()
        assert verpart == version
        # Local part may be g0123abcd or g0123abcd-dirty.  Hyphens are
        # not kosher here, so replace by dots.
        localpart = localpart.replace('-', '.')
        full_version = '%s.post%s+%s' % (verpart, revpart, localpart)
    else:
        full_version = version

    # Strip the local part if there is one, to appease pkg_resources,
    # which handles only PEP 386, not PEP 440.
    if '+' in full_version:
        pkg_version = full_version[:full_version.find('+')]
    else:
        pkg_version = full_version

    # Sanity-check the result.  XXX Consider checking the full PEP 386
    # and PEP 440 regular expressions here?
    assert '-' not in full_version, '%r' % (full_version,)
    assert '-' not in pkg_version, '%r' % (pkg_version,)
    assert '+' not in pkg_version, '%r' % (pkg_version,)

    return pkg_version, full_version

pkg_version, full_version = get_version()

def write_version_py(path):
    try:
        with open(path, 'rb') as f:
            version_old = f.read()
    except IOError:
        version_old = None
    version_new = '__version__ = %r\n' % (full_version,)
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
        sha256_in = sha256_file(path_in).hexdigest()
        sha256_out = sha256_file(path_out).hexdigest()
        expected = bytes('%s\n%s\n' % (sha256_in, sha256_out))
        with open(path_sha256, 'rb') as file_sha256:
            actual = file_sha256.read(len(expected))
            if actual != expected or file_sha256.read(1) != '':
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
    import sys
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
        sys.executable,
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

# Make sure the VERSION file in the sdist is exactly specified, even
# if it is a development version, so that we do not need to run git to
# discover it -- which won't work because there's no .git directory in
# the sdist.
class local_sdist(sdist):
    def make_release_tree(self, base_dir, files):
        import os
        sdist.make_release_tree(self, base_dir, files)
        version_file = os.path.join(base_dir, 'VERSION')
        print('updating %s' % (version_file,))
        # Write to temporary file first and rename over permanent not
        # just to avoid atomicity issues (not likely an issue since if
        # interrupted the whole sdist directory is only partially
        # written) but because the upstream sdist may have made a hard
        # link, so overwriting in place will edit the source tree.
        with open(version_file + '.tmp', 'wb') as f:
            f.write('%s\n' % (pkg_version,))
        os.rename(version_file + '.tmp', version_file)

class local_test(test):
    description = "Run check.sh"
    user_options = [('fail=', None, 'Use check.sh.')] # for distutils
    def __init__(self, *args, **kwargs):
        test.__init__(self, *args, **kwargs)
        self.test_suite = "not None"
    def run_tests(self):
        import subprocess
        subprocess.check_call(["./check.sh"])
        print "Using ./check.sh directly gives you more options for testing."

# XXX These should be attributes of `setup', but helpful distutils
# doesn't pass them through when it doesn't know about them a priori.
version_py = 'src/version.py'
lemonade = 'external/lemonade/dist'
grammars = [
    'src/grammar.y',
]

setup(
    name='bayeslite',
    version=pkg_version,
    description='BQL database built on SQLite3',
    url='http://probcomp.csail.mit.edu/bayesdb',
    author='MIT Probabilistic Computing Project',
    author_email='bayesdb@mit.edu',
    license='Apache License, Version 2.0',
    install_requires=[
        'bayeslite-apsw>=3.8.0',
        'crosscat==0.1.55',
        'jsonschema',
        'numpy',
        'requests',
        'setuptools', # For parse_version in src/remote.py
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
    test_suite = "not None",  # Without it, run_tests is not called.
    cmdclass={
        'build_py': local_build_py,
        'sdist': local_sdist,
        'test': local_test,
    },
    package_data={
        'bayeslite.metamodels': ['*.schema.json'],
    },
)
