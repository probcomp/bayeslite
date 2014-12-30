import Test
import sys
from Plex import *

spaces = Rep1(Any(" \t\n"))

lex = Lexicon([
  (Bol + Rep1(Str("a")),       'begin'),
  (      Rep1(Str("b")),       'middle'),
  (      Rep1(Str("c")) + Eol, 'end'),
  (Bol + Rep1(Str("d")) + Eol, 'everything'),
  (spaces, IGNORE)
],
debug = Test.debug,
timings = sys.stderr
)

Test.run(lex, "test7", trace = 0)


