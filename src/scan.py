# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2016, MIT Probabilistic Computing Project
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

from bayeslite.util import casefold

'''
grep -o 'K_[A-Z0-9_]*' < grammar.y | sort -u | awk '
{
    sub("^K_", "", $1)
    # All keywords are US-ASCII, so tolower is the same as casefold.
    printf("    \"%s\": grammar.K_%s,\n", tolower($1), $1)
}'
'''
keywords = {
    "all": grammar.K_ALL,
    "alter": grammar.K_ALTER,
    "analyze": grammar.K_ANALYZE,
    "and": grammar.K_AND,
    "as": grammar.K_AS,
    "asc": grammar.K_ASC,
    "begin": grammar.K_BEGIN,
    "between": grammar.K_BETWEEN,
    "btable": grammar.K_BTABLE,
    "by": grammar.K_BY,
    "case": grammar.K_CASE,
    "cast": grammar.K_CAST,
    "checkpoint": grammar.K_CHECKPOINT,
    "collate": grammar.K_COLLATE,
    "column": grammar.K_COLUMN,
    "columns": grammar.K_COLUMNS,
    "commit": grammar.K_COMMIT,
    "conf": grammar.K_CONF,
    "confidence": grammar.K_CONFIDENCE,
    "correlation": grammar.K_CORRELATION,
    "create": grammar.K_CREATE,
    "default": grammar.K_DEFAULT,
    "dependence": grammar.K_DEPENDENCE,
    "desc": grammar.K_DESC,
    "distinct": grammar.K_DISTINCT,
    "drop": grammar.K_DROP,
    "else": grammar.K_ELSE,
    "end": grammar.K_END,
    "escape": grammar.K_ESCAPE,
    "estimate": grammar.K_ESTIMATE,
    "exists": grammar.K_EXISTS,
    "explicit": grammar.K_EXPLICIT,
    "for": grammar.K_FOR,
    "from": grammar.K_FROM,
    "generator": grammar.K_GENERATOR,
    "given": grammar.K_GIVEN,
    "glob": grammar.K_GLOB,
    "group": grammar.K_GROUP,
    "having": grammar.K_HAVING,
    "if": grammar.K_IF,
    "in": grammar.K_IN,
    "infer": grammar.K_INFER,
    "information": grammar.K_INFORMATION,
    "initialize": grammar.K_INITIALIZE,
    "is": grammar.K_IS,
    "isnull": grammar.K_ISNULL,
    "iteration": grammar.K_ITERATION,
    "iterations": grammar.K_ITERATIONS,
    "like": grammar.K_LIKE,
    "limit": grammar.K_LIMIT,
    "match": grammar.K_MATCH,
    "minute": grammar.K_MINUTE,
    "minutes": grammar.K_MINUTES,
    "model": grammar.K_MODEL,
    "models": grammar.K_MODELS,
    "mutual": grammar.K_MUTUAL,
    "not": grammar.K_NOT,
    "notnull": grammar.K_NOTNULL,
    "null": grammar.K_NULL,
    "of": grammar.K_OF,
    "offset": grammar.K_OFFSET,
    "or": grammar.K_OR,
    "order": grammar.K_ORDER,
    "pairwise": grammar.K_PAIRWISE,
    "predict": grammar.K_PREDICT,
    "predictive": grammar.K_PREDICTIVE,
    "probability": grammar.K_PROBABILITY,
    "pvalue": grammar.K_PVALUE,
    "regexp": grammar.K_REGEXP,
    "rename": grammar.K_RENAME,
    "respect": grammar.K_RESPECT,
    "rollback": grammar.K_ROLLBACK,
    "row": grammar.K_ROW,
    "samples": grammar.K_SAMPLES,
    "second": grammar.K_SECOND,
    "seconds": grammar.K_SECONDS,
    "select": grammar.K_SELECT,
    "set": grammar.K_SET,
    "similarity": grammar.K_SIMILARITY,
    "simulate": grammar.K_SIMULATE,
    "table": grammar.K_TABLE,
    "temp": grammar.K_TEMP,
    "temporary": grammar.K_TEMPORARY,
    "then": grammar.K_THEN,
    "to": grammar.K_TO,
    "unset": grammar.K_UNSET,
    "using": grammar.K_USING,
    "value": grammar.K_VALUE,
    "wait": grammar.K_WAIT,
    "when": grammar.K_WHEN,
    "where": grammar.K_WHERE,
    "with": grammar.K_WITH,
}
def scan_name(_scanner, text):
    return keywords.get(text) or keywords.get(casefold(text)) or \
        grammar.L_NAME;

def scan_integer(scanner, text):
    scanner.produce(grammar.L_INTEGER, int(text, 10))

def scan_float(scanner, text):
    # XXX Consider a system-independent representation of floats which
    # we can pass through to the SQL engine.  (E.g., for the benefit
    # of SQLite4 which will use decimal floating-point arithmetic
    # instead of binary floating-point arithmetic.)
    scanner.produce(grammar.L_FLOAT, float(text))

def scan_numpar_next(scanner, text):
    # Numbered parameters are 1-indexed.
    scanner.n_numpar += 1
    scanner.produce(grammar.L_NUMPAR, scanner.n_numpar)

def scan_numpar(scanner, text):
    assert text[0] == '?'
    if 20 < len(text):          # 2^64 < 10^20
        scan_bad(scanner, text)
    else:
        n = int(text[1:])
        if n == 0:
            # Numbered parameters are 1-indexed.
            scanner.produce(-1, text)
        else:
            scanner.n_numpar = max(n, scanner.n_numpar)
            scanner.produce(grammar.L_NUMPAR, n)

def scan_nampar(scanner, text):
    text = casefold(text)
    n = None
    if text in scanner.nampar_map:
        n = scanner.nampar_map[text]
    else:
        # Numbered parameters are 1-indexed.
        scanner.n_numpar += 1
        n = scanner.n_numpar
        scanner.nampar_map[text] = n
    scanner.produce(grammar.L_NAMPAR, (n, text))

def scan_bad(scanner, text):
    scanner.produce(-1, text)   # error

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
        (Plex.Str("?"),         scan_numpar_next),
        (Plex.Str("?") + integer_dec,
                                scan_numpar),
        (Plex.Str(":") + name,  scan_nampar),
        (Plex.Str("@") + name,  scan_nampar),
        (Plex.Str("$") + name,  scan_nampar),
        (Plex.Str("'"),         scan_string_start),
        (Plex.Str('"'),         scan_qname_start),
        (name,                  scan_name),
        (integer_dec,           scan_integer),
        (integer_hex,           scan_integer),
        (float_dec,             scan_float),
        (integer_dec + name,    scan_bad),
        (integer_hex + name,    scan_bad),
        (float_dec + name,      scan_bad),
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
        self.n_numpar = 0
        self.nampar_map = {}

    def produce(self, token, value=None):
        if token is None:       # EOF
            token = 0
        Plex.Scanner.produce(self, token, value)
