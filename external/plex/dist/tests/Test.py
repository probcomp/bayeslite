#
#   Run a Plex test
#

import sys

# Mac slow console stderr hack
if sys.platform == 'mac':
  if sys.stderr is sys.__stderr__:
    sys.stderr = sys.__stdout__

import Plex

force_debug = 0

if force_debug or sys.argv[1:2] == ["-d"]:
  debug = sys.stderr
else:
  debug = None

def run(lexicon, test_name, 
        debug = 0, trace = 0, scanner_class = Plex.Scanner):
  if debug:
    debug_file = sys.stdout
    lexicon.machine.dump(debug_file)
    print "=" * 70
  else:
    debug_file = None
  in_name = test_name + ".in"
  f = open(in_name, "rU")
  s = scanner_class(lexicon, f, in_name)
  if trace:
    s.trace = 1
  while 1:
    value, text = s.read()
    name, line, pos = s.position()
    print "%s %3d %3d %-10s %s" % (name, line, pos, value, repr(text))
    if value is None:
      break

  


