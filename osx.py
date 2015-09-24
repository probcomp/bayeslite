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
GIT_REPOS = ['crosscat', 'bayeslite', 'bdbcontrib']
PEG = {  # None means head.
  'crosscat': None,
  'bdbcontrib': None,
  'bayeslite': None
  }
PAUSE_TO_MODIFY = False

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

def shellquote(s):
  """Return `s`, quoted appropriately for use in a shell script."""
  return "'" + s.replace("'", "'\\''") + "'"

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
  run("git clone https://github.com/probcomp/%s.git %s"
      % (project, os.path.join(BUILD_DIR, project)))
  if PEG[project]:
    repodir = os.path.join(BUILD_DIR, project)
    branch = PEG[project]
    run('cd -- %s && git checkout %s' %
        (shellquote(repodir), shellquote(branch)))
  if os.path.exists(os.path.join(project, 'VERSION')):
    project_version = get_version(project)
    if project_version:
      VERSION += "-" + project[:5] + project_version
      print "Project", project, "version is", project_version

VENV_DIR=os.path.join(BUILD_DIR, "venv")

# Do not specify --python version here. See explanation where we fail fast above.
run('virtualenv %s' % (shellquote(VENV_DIR),))

os.chdir(VENV_DIR)
def venv_run(cmd):
  print cmd
  assert not os.system('source bin/activate; %s' % (cmd,))

# Preprocessing
# =============
print "Deps for CrossCat"
LIBBOOST_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
  outputof("locate -l 1 boost/random/mersenne_twister.hpp", shell=True)))))
assert os.path.exists(LIBBOOST_DIR), \
  ("We need boost headers already installed for CrossCat: %s" % (LIBBOOST_DIR,))
os.environ["BOOST_ROOT"] = LIBBOOST_DIR
venv_run("pip install cython")  # If we don't, crosscat's setup tries and fails.
venv_run("pip install numpy")

BUILD_EXAMPLES = os.path.join(BUILD_DIR, "examples")
run("mkdir -p %s" % (shellquote(BUILD_EXAMPLES),))

# Main installs
# =============
for project in GIT_REPOS:
  reqfile = os.path.join(BUILD_DIR, project, "requirements.txt")
  if os.path.exists(reqfile):
    print "Installing dependencies for", project
    venv_run("pip install -r %s" % (shellquote(reqfile),))
  setupfile = os.path.join(BUILD_DIR, project, "setup.py")
  if os.path.exists(setupfile):
    print "Installing", project, "into", BUILD_DIR
    repodir = os.path.join(BUILD_DIR, project)
    venv_run("cd -- %s && pip install ." % (shellquote(repodir),))

# Postprocessing
# ==============
# This app's only other dependency:
venv_run("pip install 'ipython[notebook]' runipy")

print "Ready to start packaging the app!"
venv_run('virtualenv --relocatable %s' % (shellquote(VENV_DIR),))
# Sadly, that doesn't actually fix the most critical file, the activate script.
relocable = '''VIRTUAL_ENV=$(dirname -- "$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" && pwd )")\n'''
new_activate = tempfile.NamedTemporaryFile(delete=False)
old_activate_path = os.path.join(VENV_DIR, "bin", "activate")
with open(old_activate_path, "r") as old_activate:
  for line in old_activate:
    if line[:len("VIRTUAL_ENV=")] == "VIRTUAL_ENV=":
      new_activate.write(relocable)
    else:
      new_activate.write(line)
new_activate.close()
run("mv %s %s" %
    (shellquote(new_activate.name), shellquote(old_activate_path)))
# Also, we are hard linking to the python that we were built with, which may be different
# than the python that we want the client to execute. Let's just assume that on the client,
# we want to use the generic /System/Library py2.7, rather than a special one.
PYTHON_DYLIB_LINK = os.path.join(VENV_DIR, ".Python")
run("ln -fs /System/Library/Frameworks/Python.framework/Versions/2.7/Python %s" %
    (shellquote(PYTHON_DYLIB_LINK),))
# And we still have a copy of python that my otherwise reference its
# own dependencies, rather than relying on the built-in python. So
# remove that.
#
# XXX Actually, this doesn't work at all -- it altogether defeats the
# mechanism by which Python discovers what should be in sys.path for a
# virtualenv.
#
# If it really turns out to be necessary to do this, we can replace
# the $VENV_DIR/bin/python by the following two-line shell script:
#
#       #!/bin/bash
#       exec -a "$0" /usr/bin/python2.7 ${1+"$@"}
#
#run("rm -f %s" % (shellquote(os.path.join(VENV_DIR, "bin", "python")),))
#run("ln -s /usr/bin/python2.7 %s" %
#    (shellquote(os.path.join(VENV_DIR, "bin", "python")),))

NAME="Bayeslite%s" % (VERSION,)
DIST_DIR = os.path.join(BUILD_DIR, "dmgroot")
MACOS_PATH = os.path.join(DIST_DIR, NAME + ".app", "Contents", "MacOS")
os.makedirs(MACOS_PATH)
run("/bin/cp -r %s %s/" % (shellquote(BUILD_EXAMPLES), shellquote(MACOS_PATH)))
run("/bin/ln -s /Applications %s" % (shellquote(DIST_DIR),))

STARTER = '''#!/bin/bash

set -e
wd=`dirname -- "$0"`
cd -- "$wd"
wd=`pwd -P`
NAME=`basename -- "$(dirname -- "$(dirname -- "$wd")")" .app`

activate="$wd/venv/bin/activate"
ldpath="$wd/lib"

# Clear any user's PYTHONPATH setting, which may interfere with what
# we need.
unset PYTHONPATH

source "$activate"
export DYLD_LIBRARY_PATH="$ldpath"

# Copy the examples to someplace writeable:
if [ -d "$HOME/Documents/$NAME" ]; then
    cd -- "$HOME/Documents/$NAME" && "$wd/venv/bin/bayesdb-demo" launch
else
    mkdir -- "$HOME/Documents/$NAME"
    cd -- "$HOME/Documents/$NAME" && "$wd/venv/bin/bayesdb-demo"
fi
'''

startsh_path = os.path.join(MACOS_PATH, "start.sh")
with open(startsh_path, "w") as startsh:
  startsh.write(STARTER)
run("chmod +x %s" % (shellquote(startsh_path),))

LAUNCHER = '''#!/bin/bash

wd=`dirname -- "$0"`
cd -- "$wd"
wd=`pwd -P`

osascript -e '
    on run argv
        set wd to item 1 of argv
        set cmd to "/bin/bash -- " & quoted form of wd & "/start.sh"
        tell application "Terminal" to do script cmd
    end run
' -- "$wd"
'''

launchsh_path = os.path.join(MACOS_PATH, NAME)  # Must be the same as NAME in MACOS_PATH
with open(launchsh_path, "w") as launchsh:
  launchsh.write(LAUNCHER)
run("chmod +x %s" % (shellquote(launchsh_path),))
run("mv -f %s %s" % (shellquote(VENV_DIR), shellquote(MACOS_PATH)))

# Basic sanity check.
test_dir = tempfile.mkdtemp('bayeslite-test')
try:
  venv_run("cd -- %s && bayesdb-demo fetch" % (shellquote(test_dir),))
  venv_run("cd -- %s && runipy Satellites.ipynb" % (shellquote(test_dir),))
finally:
  run("rm -rf -- %s" % (shellquote(test_dir),))

if PAUSE_TO_MODIFY:
  print "Pausing to let you modify %s before packaging it up." % (MACOS_PATH,)
  os.system('read -s -n 1 -p "Press any key to continue..."')

DMG_PATH = os.path.join(os.environ['HOME'], 'Desktop', '%s.dmg' % (NAME,))
naming_attempt = 0
while os.path.exists(DMG_PATH):
  naming_attempt += 1
  DMG_PATH = os.path.join(os.environ['HOME'], 'Desktop',
                          "%s (%d).dmg" % (NAME, naming_attempt))
run("hdiutil create -volname Bayeslite -format UDBZ -size 1g -srcfolder %s %s"
    % (shellquote(DIST_DIR), shellquote(DMG_PATH)))
run("/bin/rm -fr %s" % (shellquote(BUILD_DIR),))

print "Done. %d seconds elapsed" % (time.time() - START_TIME,)
