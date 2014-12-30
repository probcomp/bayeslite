import Test
from Plex import *
import sys

lex = Lexicon([
  (Any("ab") + Rep(Any("ab01")), 'ident'),
  (Any(" \n"), IGNORE)
],
debug = Test.debug,
timings = sys.stderr
)

Test.run(lex, "test1", debug = 0, trace = 0)


