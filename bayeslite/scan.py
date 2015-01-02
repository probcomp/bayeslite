# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2014, MIT Probabilistic Computing Project
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import StringIO

import bayeslite.grammar as grammar
import bayeslite.plex as Plex

'''
grep -o 'K_[A-Z0-9_]*' < grammar.y | sort -u | awk '
{
    sub("^K_", "", $1)
    printf("    \"%s\": grammar.K_%s,\n", tolower($1), $1)
}'
'''
keywords = {
    "all": grammar.K_ALL,
    "and": grammar.K_AND,
    "as": grammar.K_AS,
    "asc": grammar.K_ASC,
    "between": grammar.K_BETWEEN,
    "by": grammar.K_BY,
    "collate": grammar.K_COLLATE,
    "correlation": grammar.K_CORRELATION,
    "dependence": grammar.K_DEPENDENCE,
    "desc": grammar.K_DESC,
    "distinct": grammar.K_DISTINCT,
    "escape": grammar.K_ESCAPE,
    "freq": grammar.K_FREQ,
    "from": grammar.K_FROM,
    "group": grammar.K_GROUP,
    "hist": grammar.K_HIST,
    "in": grammar.K_IN,
    "information": grammar.K_INFORMATION,
    "is": grammar.K_IS,
    "isnull": grammar.K_ISNULL,
    "like": grammar.K_LIKE,
    "limit": grammar.K_LIMIT,
    "match": grammar.K_MATCH,
    "mutual": grammar.K_MUTUAL,
    "not": grammar.K_NOT,
    "notnull": grammar.K_NOTNULL,
    "null": grammar.K_NULL,
    "of": grammar.K_OF,
    "offset": grammar.K_OFFSET,
    "or": grammar.K_OR,
    "order": grammar.K_ORDER,
    "plot": grammar.K_PLOT,
    "predictive": grammar.K_PREDICTIVE,
    "probability": grammar.K_PROBABILITY,
    "respect": grammar.K_RESPECT,
    "select": grammar.K_SELECT,
    "similarity": grammar.K_SIMILARITY,
    "summarize": grammar.K_SUMMARIZE,
    "to": grammar.K_TO,
    "typicality": grammar.K_TYPICALITY,
    "where": grammar.K_WHERE,
    "with": grammar.K_WITH,
}
def scan_name(_scanner, text):
    return keywords.get(text) or keywords.get(text.lower()) or grammar.L_NAME;

def scan_integer(scanner, text):
    scanner.produce(grammar.L_INTEGER, int(text, 10))

def scan_float(scanner, text):
    # XXX Consider a system-independent representation of floats which
    # we can pass through to the SQL engine.  (E.g., for the benefit
    # of SQLite4 which will use decimal floating-point arithmetic
    # instead of binary floating-point arithmetic.)
    scanner.produce(grammar.L_FLOAT, float(text))

def scan_bad(scanner, text):
    # XXX Syntax error!
    print "Ignoring bad input: %s" % (text,)

def scan_qname_start(scanner, text):
    assert text == '"'
    scan_quoted_start(scanner, text, "QNAME")

def scan_qname_end(scanner, text):
    scan_quoted_end(scanner, text, grammar.L_NAME)

def scan_string_start(scanner, text):
    assert text == "'"
    scan_quoted_start(scanner, text, "STRING")

def scan_string_end(scanner, text):
    scan_quoted_end(scanner, text, grammar.L_STRING)

def scan_quoted_start(scanner, text, state):
    assert scanner.stringio is None
    assert scanner.stringquote is None
    scanner.stringio = StringIO.StringIO()
    scanner.begin(state)

def scan_quoted_text(scanner, text):
    assert scanner.stringio is not None
    scanner.stringio.write(text)

def scan_quoted_quote(scanner, text):
    assert scanner.stringio is not None
    assert text[0] == text[1]
    scanner.stringio.write(text[0])

def scan_quoted_end(scanner, text, token):
    assert scanner.stringio is not None
    string = scanner.stringio.getvalue()
    scanner.stringio.close()
    scanner.stringio = None
    scanner.produce(token, string)
    scanner.begin("")

def scan_blob(scanner, text):
    assert text[0] == "x" or text[0] == "X"
    assert text[1] == text[-1] == "'"
    nhexits = len(text) - len("x''")
    assert (nhexits % 2) == 0
    blob = bytearray.fromhex(buffer(hexbuf, 1, nhexits))
    scanner.produce(grammar.L_BLOB, blob)

def scan_badblob(scanner, text):
    assert text[0] == "x" or text[0] == "X"
    assert text[1] == text[-1] == "'"
    # XXX Syntax error!
    print "Ignoring bad input: %s" % (text,)

class BQLScanner(Plex.Scanner):
    line_comment = Plex.Str("--") + Plex.Rep(Plex.AnyBut("\n"))
    whitespace = Plex.Any("\f\n\r\t ")
    # XXX Support non-US-ASCII Unicode text.
    letter = Plex.Range("azAZ")
    digit = Plex.Range("09")
    digits = Plex.Rep(digit)
    digits1 = Plex.Rep1(digit)
    hexit = digit | Plex.Range("afAF")
    hexits1 = Plex.Rep1(hexit)
    integer_dec = digits1
    integer_hex = Plex.Str("0x", "0X") + hexits1
    dot = Plex.Str('.')
    intfrac = digits1 + dot + digits
    fraconly = dot + digits1
    optsign = Plex.Opt(Plex.Any('+-'))
    expmark = Plex.Any('eE')
    exponent = expmark + optsign + digits1
    optexp = Plex.Opt(exponent)
    float_dec = ((intfrac | fraconly) + optexp) | (digits1 + exponent)
    blob = Plex.Str("x'", "X'") + Plex.Rep(hexit + hexit) + Plex.Str("'")
    badblob = Plex.Str("x'", "X'") + Plex.AnyBut("'") + Plex.Str("'")
    name_special = Plex.Any("_$") 
    name = (letter | name_special) + Plex.Rep(letter | digit | name_special)

    lexicon = Plex.Lexicon([
        (whitespace,            Plex.IGNORE),
        (line_comment,          Plex.IGNORE),
        (Plex.Str(";"),         grammar.T_SEMI),
        (Plex.Str("("),         grammar.T_LROUND),
        (Plex.Str(")"),         grammar.T_RROUND),
        (Plex.Str("+"),         grammar.T_PLUS),
        (Plex.Str("-"),         grammar.T_MINUS),
        (Plex.Str("*"),         grammar.T_STAR),
        (Plex.Str("/"),         grammar.T_SLASH),
        (Plex.Str("%"),         grammar.T_PERCENT),
        (Plex.Str("="),         grammar.T_EQ),
        (Plex.Str("=="),        grammar.T_EQ),
        (Plex.Str("<"),         grammar.T_LT),
        (Plex.Str("<>"),        grammar.T_NEQ),
        (Plex.Str("<="),        grammar.T_LEQ),
        (Plex.Str(">"),         grammar.T_GT),
        (Plex.Str(">="),        grammar.T_GEQ),
        (Plex.Str("<<"),        grammar.T_LSHIFT),
        (Plex.Str(">>"),        grammar.T_RSHIFT),
        (Plex.Str("!="),        grammar.T_NEQ),
        (Plex.Str("|"),         grammar.T_BITIOR),
        (Plex.Str("||"),        grammar.T_CONCAT),
        (Plex.Str(","),         grammar.T_COMMA),
        (Plex.Str("&"),         grammar.T_BITAND),
        (Plex.Str("~"),         grammar.T_BITNOT),
        (Plex.Str("."),         grammar.T_DOT),
        # (Plex.Str("?"),         grammar.L_NUMVAR),
        # (Plex.Str("?") + dec,   grammar.L_NUMVAR),
        # (Plex.Str(":") + name,  grammar.L_NAMVAR),
        # (Plex.Str("@") + name,  grammar.L_NAMVAR),
        # (Plex.Str("$") + name,  grammar.L_NAMVAR),
        (Plex.Str("'"),         scan_string_start),
        (Plex.Str('"'),         scan_qname_start),
        (blob,                  scan_blob),
        (badblob,               scan_badblob),
        (name,                  scan_name),
        (integer_dec,           scan_integer),
        (integer_hex,           scan_integer),
        (float_dec,             scan_float),
        (Plex.AnyChar,          scan_bad),
        Plex.State("STRING", [
            (Plex.Str("'"),                     scan_string_end),
            (Plex.Str("''"),                    scan_quoted_quote),
            (Plex.Rep1(Plex.AnyBut("'")),       scan_quoted_text),
        ]),
        Plex.State("QNAME", [
            (Plex.Str('"'),                     scan_qname_end),
            (Plex.Str('""'),                    scan_quoted_quote),
            (Plex.Rep1(Plex.AnyBut('"')),       scan_quoted_text),
        ]),
    ])

    def __init__(self, f, context):
        Plex.Scanner.__init__(self, self.lexicon, f, context)
        self.stringio = None
        self.stringquote = None

    def produce(self, token, value=None):
        if token is None:       # EOF
            token = 0
        Plex.Scanner.produce(self, token, value)
