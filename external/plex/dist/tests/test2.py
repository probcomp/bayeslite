import Test
from Plex import *
import sys

lex = Lexicon([
  (Seq(Any("ab"), Rep(Any("ab01"))), 'ident'),
  (Seq(Any("01"), Rep(Any("01"))), 'num'),
  (Any(' \n'), IGNORE),
  (Str("abba"), 'abba'),
  (Any('([{!"#') + Rep(AnyBut('!"#}])')) + Any('!"#}])'), IGNORE)
],
debug = Test.debug,
timings = sys.stderr
)

Test.run(lex, "test2", debug = 0, trace = 0)


