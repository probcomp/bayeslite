#
#   Example - Python scanner
#

import exceptions
from Plex import *

class NaughtyNaughty(exceptions.Exception):
  pass

class PythonScanner(Scanner):	 
  
  def open_bracket_action(self, text):
    self.bracket_nesting_level = self.bracket_nesting_level + 1
    return text

  def close_bracket_action(self, text):
    self.bracket_nesting_level = self.bracket_nesting_level - 1
    return text

  def current_level(self):
    return self.indentation_stack[-1]

  def newline_action(self, text):
    if self.bracket_nesting_level == 0:
      self.begin('indent')
      return 'newline'

  def indentation_action(self, text):
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
    if new_level > current_level:
      self.indent_to(new_level)
    elif new_level < current_level:
      self.dedent_to(new_level)
    # Change back to default state
    self.begin('')

  def indent_to(self, new_level):
    self.indentation_stack.append(new_level)
    self.produce('INDENT', '')

  def dedent_to(self, new_level):
    while new_level < self.current_level():
      del self.indentation_stack[-1]
      self.produce('DEDENT', '')
    if new_level <> self.current_level():
      raise NaughtyNaughty("Indentation booboo!")

  def eof(self):
    self.dedent_to(0)

  letter = Range("AZaz") | Any("_")
  digit = Range("09")
  hexdigit = Range("09AFaf")

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
  opening_bracket = Any("([{")
  closing_bracket = Any(")]}")
  punct1 = Any(":,;+-*/|&<>=.%`~^")
  punct2 = Str("==", "<>", "!=", "<=", "<<", ">>", "**")
  punctuation = punct1 | punct2

  spaces = Rep1(Any(" \t"))
  indentation = Rep(Str(" ")) | Rep(Str("\t"))
  lineterm = Str("\n") | Eof
  escaped_newline = Str("\\\n")
  comment = Str("#") + Rep(AnyBut("\n"))
  blank_line = indentation + Opt(comment) + lineterm
  
  lexicon = Lexicon([
    (name,            'name'),
    (number,          'number'),
    (stringlit,       'string'),
    (punctuation,     TEXT),
    (opening_bracket, open_bracket_action),
    (closing_bracket, close_bracket_action),
    (lineterm,        newline_action),
    (comment,         IGNORE),
    (spaces,          IGNORE),
    (escaped_newline, IGNORE),
      State('indent', [
      (blank_line,    IGNORE),
      (indentation,   indentation_action),
    ]),
  ])

  def __init__(self, file):
    Scanner.__init__(self, self.lexicon, file)
    self.indentation_stack = [0]
    self.bracket_nesting_level = 0
    self.indentation_char = None
    self.begin('indent')

f = open("python.in", "r")
scanner = PythonScanner(f)
level = 0
while 1:
    token, text = scanner.read()
    if token is None:
        break
    if token == 'INDENT':
      level = level + 1
    elif token == 'DEDENT':
      level = level - 1
    indent = ' ' * (level * 4)
    if not text or token == text:
      value = token
    else:
      value = "%s(%s)" % (token, repr(text))
    print indent + value


