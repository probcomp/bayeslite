#
#   Example 1
#

from Plex import *

lexicon = Lexicon([
  (Str("Python"),      "my_favourite_language"),
  (Str("Perl"),        "the_other_language"),
  (Str("rocks"),       "is_excellent"),
  (Str("sucks"),       "is_differently_good"),
  (Rep1(Any(" \t\n")), IGNORE)
])

#
#   Example 2
#

filename = "example1and2.in"
f = open(filename, "r")
scanner = Scanner(lexicon, f, filename)
while 1:
  token = scanner.read()
  print token
  if token[0] is None:
    break


