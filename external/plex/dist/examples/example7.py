#
#   Example 7
#

from Plex import *

letter = Range("AZaz")
digit = Range("09")
name = letter + Rep(letter | digit)
number = Rep1(digit)
space = Any(" \t\n")

class MyScanner(Scanner):

  def begin_comment(self, text):
    if self.nesting_level == 0:
      self.begin('comment')
    self.nesting_level = self.nesting_level + 1

  def end_comment(self, text):
    self.nesting_level = self.nesting_level - 1
    if self.nesting_level == 0:
      self.begin('')

  lexicon = Lexicon([
    (name,          'ident'),
    (number,        'int'),
    (space,         IGNORE),
    (Str("(*"),     begin_comment),
    State('comment', [
      (Str("(*"),   begin_comment),
      (Str("*)"),   end_comment),
      (AnyChar,     IGNORE)
    ])
  ])

  def __init__(self, file, name):
    Scanner.__init__(self, self.lexicon, file, name)
    self.nesting_level = 0

filename = "example7.in"
f = open(filename, "r")
scanner = MyScanner(f, filename)
while 1:
  token = scanner.read()
  print token
  if token[0] is None:
    break


