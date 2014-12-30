import sys
from cStringIO import StringIO

import Test
from Plex import *

lex = Lexicon([
	(Str("'") + Rep(AnyBut("'")) + Str("'"), TEXT)
	],
	debug = Test.debug,
	timings = sys.stderr
)

Test.run(lex, "test12", debug = 0, trace = 0)
