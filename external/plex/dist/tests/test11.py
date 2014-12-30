import Test
from Plex import *
import sys

lex = Lexicon([
  (Str("Python"), 'upper-python'),
  (Str("python"), 'lower-python'),
  (NoCase(Str("COBOL", "perl", "Serbo-Croatian")), 'other-language'),
  (NoCase(Str("real") + Case(Str("basic"))), 'real-1'),
  (NoCase(Str("real") + Case(Str("Basic"))), 'real-2'),
  (Any(" \t\n"), IGNORE)
],
debug = Test.debug,
timings = sys.stderr
)

Test.run(lex, "test11", debug = 0, trace = 0)
