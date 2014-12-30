import Test

import exceptions
import sys
from Plex import *

if 1:
  debug = sys.stdout
else:
  debug = None

#########################################################################

class NaughtyNaughty(exceptions.Exception):
  pass

class MyScanner(Scanner):
  bracket_nesting_level = 0
  indentation_stack = None
  indentation_char = None

  def current_level(self):
    return self.indentation_stack[-1]

  def open_bracket_action(self, text):
    self.bracket_nesting_level = self.bracket_nesting_level + 1
    return text

  def close_bracket_action(self, text):
    self.bracket_nesting_level = self.bracket_nesting_level - 1
    return text

  def newline_action(self, text):
    if self.bracket_nesting_level == 0:
      self.begin('indent')
      self.produce('newline', '')

  def indentation_action(self, text):
    self.begin('')
    # Check that tabs and spaces are being used consistently.
    if text:
      c = text[0]
      if self.indentation_char is None:
        self.indentation_char = c
      else:
        if self.indentation_char <> c:
          raise NaughtyNaughty("Mixed up tabs and spaces!")
    # Figure out how many indents/dedents to do
    current_level = self.current_level()
    new_level = len(text)
    if new_level == current_level:
      return
    elif new_level > current_level:
      self.indentation_stack.append(new_level)
      self.produce('INDENT', '')
    else:
      while new_level < self.current_level():
        del self.indentation_stack[-1]
        self.produce('DEDENT', '')
      if new_level <> self.current_level():
        raise NaughtyNaughty("Indentation booboo!")

  def eof(self):
    while len(self.indentation_stack) > 1:
      self.produce('DEDENT', '')
      self.indentation_stack.pop()

  letter = Range("AZaz") | Any("_")
  digit = Range("09")
  hexdigit = Range("09AFaf")
  indentation = Rep(Str(" ")) | Rep(Str("\t"))

  name = letter + Rep(letter | digit)
  number = Rep1(digit) | (Str("0x") + Rep1(hexdigit))
  sq_string = (
    Str("'") + 
    Rep(AnyBut("\\\n'") | (Str("\\") + AnyChar)) + 
    Str("'"))
  dq_string = (
    Str('"') + 
    Rep(AnyBut('\\\n"') | (Str("\\") + AnyChar)) + 
    Str('"'))
  non_dq = AnyBut('"') | (Str('\\') + AnyChar)
  tq_string = (
    Str('"""') +
    Rep(
      non_dq |
      (Str('"') + non_dq) |
      (Str('""') + non_dq)) + Str('"""'))
  stringlit = sq_string | dq_string | tq_string
  bra = Any("([{")
  ket = Any(")]}")
  punct = Any(":,;+-*/|&<>=.%`~^")
  diphthong = Str("==", "<>", "!=", "<=", "<<", ">>", "**")
  spaces = Rep1(Any(" \t"))
  comment = Str("#") + Rep(AnyBut("\n"))
  escaped_newline = Str("\\\n")
  lineterm = Str("\n") | Eof

  lexicon = Lexicon([
    (name, 'name'),
    (number, 'number'),
    (stringlit, 'string'),
    (punct | diphthong, TEXT),
    (bra, open_bracket_action),
    (ket, close_bracket_action),
    (lineterm, newline_action),
    (comment, IGNORE),
    (spaces, IGNORE),
    (escaped_newline, IGNORE),
    State('indent', [
      (indentation + Opt(comment) + lineterm, IGNORE),
      (indentation, indentation_action),
    ]),
  ], 
  debug = Test.debug,
  debug_flags = 7,
  timings = sys.stderr)

  def __init__(self, file):
    Scanner.__init__(self, self.lexicon, file)
    self.indentation_stack = [0]
    self.begin('indent')

#########################################################################

#s.machine.dump(sys.stdout)
#print "=" * 70

f = open("test9.in", "rU")
ts = MyScanner(f)
ts.trace = 0
while 1:
  value, text = ts.read()
  level = len(ts.indentation_stack) - 1
  if level:
    print (4 * level - 1) * ' ',
  if text and text <> value:
    print "%s(%s)" % (value, repr(text))
  else:
    print repr(value)
  if value is None:
    break




