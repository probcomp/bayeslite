#
#   Example - Apple Object Pascal scanner
#

from Plex import *

def make_lexicon():

  letter = Range("AZaz") | Any("_")
  digit = Range("09")
  space = Any(" \t\n")

  ident = letter + Rep(letter | digit)
  resword = NoCase(Str("program", "unit", "uses", "const", "type", "var",
                "if", "then", "else", "while", "do", "repeat", "until",
                "for", "to", "downto", "and", "or", "not",
                "array", "of", "record", "object"))
  number = Rep1(digit)
  string = Str("'") + (Rep(AnyBut("'")) | Str("''")) + Str("'")
  diphthong = Str(":=", "<=", ">=", "<>", "..")
  punct = Any("^&*()-+=[]|;:<>,./")
  spaces = Rep1(space)
  comment_begin = Str("{")
  comment_char = AnyBut("}")
  comment_end = Str("}")

  lexicon = Lexicon([
    (resword, TEXT),
    (ident, 'ident'),
    (number, 'num'),
    (string, 'str'),
    (punct | diphthong, TEXT),
    (spaces, IGNORE),
    (comment_begin, Begin('comment')),
    State('comment', [
      (comment_char, IGNORE),
      (comment_end, Begin(''))
    ])
  ])

  return lexicon

if __name__ == "__main__":
  lexicon = make_lexicon()
  filename = "pascal.in"
  f = open(filename, "r")
  scanner = Scanner(lexicon, f, filename)
  while 1:
      token = scanner.read()
      print token
      if token[0] is None:
          break


