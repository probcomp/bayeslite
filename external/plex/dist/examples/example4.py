#
#   Example 4
#

from Plex import *

def begin_comment(scanner, text):
  scanner.nesting_level = scanner.nesting_level + 1

def end_comment(scanner, text):
  scanner.nesting_level = scanner.nesting_level - 1

def maybe_a_name(scanner, text):
  if scanner.nesting_level == 0:
    return 'ident'

letter = Range("AZaz")
digit = Range("09")
name = letter + Rep(letter | digit)
space = Any(" \t\n")

lexicon = Lexicon([
  (Str("(*"), begin_comment),
  (Str("*)"), end_comment),
  (name,      maybe_a_name),
  (space,     IGNORE)
])

filename = "example4.in"
f = open(filename, "r")
scanner = Scanner(lexicon, f, filename)
scanner.nesting_level = 0
while 1:
  token = scanner.read()
  print token
  if token[0] is None:
    break


