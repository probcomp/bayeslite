import os
import sys

import Plex
import pascal

if sys.platform == 'mac':
  import MacOS
  def time():
    return MacOS.GetTicks() / 60.0
  timekind = "real"
else:
  def time():
    t = os.times()
    return t[0] + t[1]
  timekind = "cpu"

time1 = time()
lexicon = pascal.make_lexicon()
time2 = time()
print "Constructing scanner took %s %s seconds" % (time2 - time1, timekind)

f = open("speedtest.in", "r")
scanner = Plex.Scanner(lexicon, f)
time1 = time()
while 1:
  value, text = scanner.read()
  if value is None:
    break
time2 = time()
_, lines, _ = scanner.position()
time = time2 - time1
lps = float(lines) / float(time)
print "Scanning %d lines took %s %s seconds (%s lines/sec)" % (
  lines, time, timekind, lps)



