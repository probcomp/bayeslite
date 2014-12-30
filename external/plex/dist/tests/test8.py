#
#   This one tests the backing-up mechanism.
#

import Test
import sys
from Plex import *

spaces = Rep1(Any(" \t\n"))

lex = Lexicon([
  (Str("ftangftang"), 'two_ftangs'),
  (Str("ftang"),      'one_ftang'),
  (Str("fta"),        'one_fta'),
  (spaces, IGNORE)
],
debug = Test.debug,
timings = sys.stderr
)

Test.run(lex, "test8", trace = 0)


