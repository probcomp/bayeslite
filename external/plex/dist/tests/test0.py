import Test
from Plex import *
import sys

lex = Lexicon([
  (Str("a"), 'thing'),
  (Any("\n"), IGNORE)
],
debug = Test.debug,
timings = sys.stderr
)

Test.run(lex, "test0", debug = 0, trace = 0)


