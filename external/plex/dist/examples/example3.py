#
#   Example 3
#

from Plex import *

letter = Range("AZaz")
digit = Range("09")
name = letter + Rep(letter | digit)
number = Rep1(digit)
space = Any(" \t\n")
comment = Str("{") + Rep(AnyBut("}")) + Str("}")

resword = Str("if", "then", "else", "end")

lexicon = Lexicon([
  (name,            'ident'),
  (number,          'int'),
  (resword,         TEXT),
  (Any("+-*/=<>"),  TEXT),
  (space | comment, IGNORE)
])

filename = "example3.in"
f = open(filename, "r")
scanner = Scanner(lexicon, f, filename)
while 1:
  token = scanner.read()
  print token
  if token[0] is None:
    break


