# Test traditional regular expression syntax.

import Test

from Plex.Traditional import re
from Plex.Errors import PlexError
from Plex import Seq, AnyBut

def test_err(s):
  try:
    print re(s)
  except PlexError, e:
    print e

print re("")
print re("a")
print re("[a]")
print re("[ab]")
print re("[abc]")
print re("[a-c]")
print re("[a-cd]")
print re("[a-cg-i]")
print re("[^a]")
print re("[^a-cg-i]")
print re("[-]")
print re("[-abc]")
print re("[abc-]")
print re("[]]")
print re("[]-]")
print re("[^-]")
print re("[^-abc]")
print re("[^abc-]")
print re("[^]]")
print re("[^]-]")
print re("a*")
print re("a+")
print re("a?")
print re("a*+?")
print re("ab")
print re("a|b")
print re("abcde")
print re("a|b|c|d|e")
print re("abc|def|ghi")
print re("abc(def|ghi)")
print re("ab\(c\[de")
print re("^abc$")
print str(re(".")) == str(Seq(AnyBut('\n')))
test_err("abc(de")
test_err("abc[de")
test_err("abc)de")


