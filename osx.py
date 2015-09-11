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

# Requires boost pre-installed.
# This is slightly special if you want to use Venture. The GNU c++
# compilers use the standard library libstdc++, while Mac's c++
# compiler on Mavericks uses libc++. In order for Venture to build,
# you must build Boost using libstdc++, and then build Venture using
# the same. This can be accomplished by building both Boost and
# Venture using GNU gcc (installed via Homebrew) instead of Mac's
# compiler. The correct version of gcc is set for Venture installation
# in the setup.py file. To install Boost with the correct library,
# call:
#
#    brew install boost --cc=gcc-4.9
#    brew install boost-python --cc=gcc-4.9

# Requires locate to be working, so we can find a pre-installed boost.
# Requires virtualenv pre-installed.
# Requires read access to the listed git repos.
GIT_REPOS = ['crosscat', 'bdbcontrib', 'bayeslite']
PEG = {  # None means head.
  'crosscat': None,
  'bdbcontrib': None,
  'bayeslite': None
  }

import distutils.spawn
import errno
import os
import os.path
root = os.path.dirname(os.path.abspath(__file__))
here = os.getcwd()
import subprocess
import time
import tempfile

START_TIME = time.time()

try:
  from setuptools import setup
except ImportError:
  from distutils.core import setup

BUILD_DIR = tempfile.mkdtemp(prefix='BayesLite-app-')
os.chdir(BUILD_DIR)
print "Building in", BUILD_DIR

def run(cmd):
  print cmd
  assert not os.system(cmd)

def outputof(cmd, **kwargs):
  print cmd
  output = subprocess.check_output(cmd, **kwargs)
  print "OUTPUT:", output
  return output

# Do not specify --python to virtualenv, bc eg python27 is a 32-bit version that
# will produce a .app that osx will not open.
# You get errors like:
#  You can’t open the application “...” because PowerPC applications are no longer supported.
# Or less helpfully:
#  The application “...” can’t be opened.
# and when investigating:
#  lipo: can't figure out the architecture type of: ...
# Instead, use the built-in python by not specifying anything, and get fine results.
# Rather than merely praying that the built-in python is language-compatible, let's check.
PYVER = outputof('python --version 2>&1', shell=True)
assert "Python 2.7" == PYVER[:10]


def get_version(project_dir):
  here = os.getcwd()
  with open(os.path.join(project_dir, 'VERSION'), 'rU') as f:
    version = f.readline().strip()

  # Append the Git commit id if this is a development version.
  if version.endswith('+'):
    tag = 'v' + version[:-1]
    try:
      os.chdir(project_dir)
      desc = outputof(['git', 'describe', '--dirty', '--match', tag])
      os.chdir(here)
    except Exception:
      version += 'unknown'
    else:
      assert desc.startswith(tag)
      version = desc[1:].strip()
  return version

VERSION = ''
for project in GIT_REPOS:
  print "Checking out", project
  run("git clone git@github.com:mit-probabilistic-computing-project/%s.git %s"
      % (project, os.path.join(BUILD_DIR, project)))
  if PEG[project]:
    run('cd %s; git checkout %s' % (os.path.join(BUILD_DIR, project), PEG[project]))
  if os.path.exists(os.path.join(project, 'VERSION')):
    project_version = get_version(project)
    if project_version:
      VERSION += "-" + project[:5] + project_version
      print "Project", project, "version is", project_version

VENV_DIR=os.path.join(BUILD_DIR, "venv")

# Do not specify --python version here. See explanation where we fail fast above.
run('virtualenv %s' % VENV_DIR)

os.chdir(VENV_DIR)
def venv_run(cmd):
  print cmd
  assert not os.system('source bin/activate; %s' % cmd)

# DEPENDENCIES
print "Deps for BdbContrib"
LIBBOOST_DIR = os.path.dirname(outputof("locate -l 1 libboost_atomic-mt.dylib", shell=True))
assert os.path.exists(LIBBOOST_DIR), ("We need libboost-dev already installed: %s" %
                                      LIBBOOST_DIR)
venv_run("cp %s/libboost_* lib/" % LIBBOOST_DIR)
os.environ['DYLD_LIBRARY_PATH']=os.path.join(VENV_DIR, "lib")
venv_run("pip install cython")  # If we don't, crosscat's setup tries and fails.
venv_run("pip install numpy")
venv_run("cp -R %s/bdbcontrib/bdbcontrib lib/python2.7/site-packages/bdbcontrib" % BUILD_DIR)
print "Deps for BdbContrib"
venv_run("pip install matplotlib seaborn==0.5.1 pandas markdown2 sphinx numpydoc")
print "Deps for BayesLite"
# Assume that osx has sqlite3 already.
# http://computechtips.com/619/upgrade-sqlite-os-x-mavericks-yosemite
# which suggests that OS 10.10 and above have a sufficiently new sqlite.
venv_run("pip install pytest sphinx cov-core dill ")

BUILD_EXAMPLES = os.path.join(BUILD_DIR, "examples")
run("mkdir -p '%s'" % BUILD_EXAMPLES)

# Main installs:
for project in GIT_REPOS:
  reqfile = os.path.join(BUILD_DIR, project, "requirements.txt")
  if os.path.exists(reqfile):
    print "Installing dependencies for", project
    venv_run("pip install -r %s" % reqfile)
  setupfile = os.path.join(BUILD_DIR, project, "setup.py")
  if os.path.exists(setupfile):
    print "Installing", project, "into", BUILD_DIR
    venv_run("cd %s; python setup.py install" % os.path.join(BUILD_DIR, project))
  examplesdir = os.path.join(BUILD_DIR, project, "examples")
  if os.path.exists(examplesdir):
    print "Copying examples from", examplesdir
    run("/bin/cp -r '%s'/* '%s'/" % (examplesdir, BUILD_EXAMPLES))

# This app's only other dependency:
venv_run("pip install 'ipython[notebook]' runipy")

print "Ready to start packaging the app!"
venv_run('virtualenv --relocatable %s' % VENV_DIR)
# Sadly, that doesn't actually fix the most critical file, the activate script.
relocable = '''VIRTUAL_ENV=$(dirname $( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd ))\n'''
new_activate = tempfile.NamedTemporaryFile(delete=False)
old_activate_path = os.path.join(VENV_DIR, "bin", "activate")
with open(old_activate_path, "r") as old_activate:
  for line in old_activate:
    if line[:len("VIRTUAL_ENV=")] == "VIRTUAL_ENV=":
      new_activate.write(relocable)
    else:
      new_activate.write(line)
new_activate.close()
run("mv '%s' '%s'" % (new_activate.name, old_activate_path))
# Also, we are hard linking to the python that we were built with, which may be different
# than the python that we want the client to execute. Let's just assume that on the client,
# we want to use the generic /System/Library py2.7, rather than a special one.
PYTHON_DYLIB_LINK = os.path.join(VENV_DIR, ".Python")
run("ln -fs /System/Library/Frameworks/Python.framework/Versions/2.7/Python %s" %
    PYTHON_DYLIB_LINK)

NAME="BayesDB%s" % VERSION
DIST_DIR = os.path.join(BUILD_DIR, "BayesDB")
MACOS_PATH = os.path.join(DIST_DIR, NAME + ".app", "Contents", "MacOS")
os.makedirs(MACOS_PATH)
run("/bin/cp -r '%s' '%s/'" % (BUILD_EXAMPLES, MACOS_PATH))
run("/bin/ln -s /Applications '%s'" % DIST_DIR)

STARTER = '''#!/bin/bash

set -e
wd=`dirname $0`
cd $wd
wd=`pwd -P`
NAME=`basename $(dirname $(dirname $wd)) .app`

activate="$wd/venv/bin/activate"
sitepkgs="$wd/venv/lib/python2.7/site-packages"
pypath="$sitepkgs:$sitepkgs/bdbcontrib"
ldpath="$wd/lib"

source $activate
export PYTHONPATH="$pypath"
export DYLD_LIBRARY_PATH="$ldpath"

# Copy the examples to someplace writeable:
rsync -r --ignore-existing "$wd/examples"/* "$HOME/Documents/$NAME"
ipython notebook "$HOME/Documents/$NAME"
'''

startsh_path = os.path.join(MACOS_PATH, "start.sh")
with open(startsh_path, "w") as startsh:
  startsh.write(STARTER)
run("chmod +x '%s'" % startsh_path)

LAUNCHER = '''#!/bin/bash

wd=`dirname $0`
cd $wd
wd=`pwd -P`

osacmd="tell application \\"Terminal\\" to do script"
script="/bin/bash $wd/start.sh"
osascript -e "$osacmd \\"$script\\""
'''

launchsh_path = os.path.join(MACOS_PATH, NAME)  # Must be the same as NAME in MACOS_PATH
with open(launchsh_path, "w") as launchsh:
  launchsh.write(LAUNCHER)
run("chmod +x '%s'" % launchsh_path)
run("mv -f '%s' '%s'" % (VENV_DIR, MACOS_PATH))

# Basic sanity check.
venv_run("runipy '%s'" % os.path.join(MACOS_PATH, "examples", "Satellites.ipynb"))

DMG_PATH = os.path.join(os.environ['HOME'], 'Desktop', '%s.dmg' % NAME)
naming_attempt = 0
while os.path.exists(DMG_PATH):
  naming_attempt += 1
  DMG_PATH = os.path.join(os.environ['HOME'], 'Desktop',
                          "%s (%d).dmg" % (NAME, naming_attempt))
run("hdiutil create -format UDBZ -size 1g -srcfolder '%s' '%s'" % (DIST_DIR, DMG_PATH))
run("/bin/rm -fr '%s'" % BUILD_DIR)

print "Done. %d seconds elapsed" % (time.time() - START_TIME)
