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

PEG = {  # None means head.
  'crosscat': None,
  'bdbcontrib': None,
  'venturecxx': 'release-0.4.1',
  'bayeslite': None
  }

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
for project in ['crosscat', 'bdbcontrib', 'venturecxx', 'bayeslite']:
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
# Venture has a requirements.txt, so will get done below.
print "Deps for BayesLite"
# Assume that osx has sqlite3 already.
# http://computechtips.com/619/upgrade-sqlite-os-x-mavericks-yosemite
# which suggests that OS 10.10 and above have a sufficiently new sqlite.
venv_run("pip install pytest sphinx cov-core dill ")

# Main installs:
for project in ['crosscat', 'venturecxx', 'bayeslite']:
  reqfile = os.path.join(BUILD_DIR, project, "requirements.txt")
  if os.path.exists(reqfile):
    print "Installing dependencies for", project
    venv_run("pip install -r %s" % reqfile)
  print "Installing", project, "into", BUILD_DIR
  venv_run("cd %s; python setup.py install" % os.path.join(BUILD_DIR, project))

# This app's only other dependency:
venv_run("pip install 'ipython[notebook]'")

print "Ready to start packaging the app!"
venv_run('virtualenv --relocatable %s' % VENV_DIR)

NAME="BayesDB%s" % VERSION
DIST_DIR = os.path.join(BUILD_DIR, "BayesDB")
MACOS_PATH = os.path.join(DIST_DIR, NAME + ".app", "Contents", "MacOS")
os.makedirs(MACOS_PATH)

STARTER = '''#!/bin/bash

set -e
wd=`dirname $0`
activate="$wd/venv/bin/activate"
bdbcontrib="$wd/venv/lib/python2.7/site-packages/bdbcontrib"
pypath="$bdbcontrib"
ldpath="$wd/lib"
blite="$wd/venv/bin/bayeslite"
rcfile="$wd/bayesliterc"

cat > $rcfile <<EOF
.hook $bdbcontrib/facade.py
.hook $bdbcontrib/contrib_math.py
.hook $bdbcontrib/contrib_plot.py
.hook $bdbcontrib/contrib_util.py
EOF

source $activate
export PYTHONPATH="$pypath"
export DYLD_LIBRARY_PATH="$ldpath"
$blite -m -f $rcfile
# Use .open to open a bdb, rather than using a command line arg.
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

DMG_PATH = os.path.join(os.environ['HOME'], 'Desktop', '%s.dmg' % NAME)
naming_attempt = 0
while os.path.exists(DMG_PATH):
  naming_attempt += 1
  DMG_PATH = os.path.join(os.environ['HOME'], 'Desktop',
                          "%s (%d).dmg" % (NAME, naming_attempt))
run("hdiutil create -format UDBZ -size 1g -srcfolder '%s' '%s'" % (DIST_DIR, DMG_PATH))
run("/bin/rm -fr '%s'" % BUILD_DIR)

print "Done. %d seconds elapsed" % (time.time() - START_TIME)
