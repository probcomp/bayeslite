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

from collections import namedtuple

from bayeslite.exception import BQLParseError
from bayeslite.util import casefold

import grammar

'''
grep -o 'K_[A-Z][A-Z0-9_]*' < grammar.y | sort -u | awk '
{
    sub("^K_", "", $1);
    printf("    '\''%s'\'': grammar.K_%s,\n", tolower($1), $1);
}'
'''

KEYWORDS = {
    'clustering': grammar.K_CLUSTERING,
    'concentration': grammar.K_CONCENTRATION,
    'hyperparameters': grammar.K_HYPERPARAMETERS,
    'loom': grammar.K_LOOM,
    'optimized': grammar.K_OPTIMIZED,
    'quiet': grammar.K_QUIET,
    'row': grammar.K_ROW,
    'rows': grammar.K_ROWS,
    'skip': grammar.K_SKIP,
    'subproblem': grammar.K_SUBPROBLEM,
    'subproblems': grammar.K_SUBPROBLEMS,
    'variable': grammar.K_VARIABLE,
    'variables': grammar.K_VARIABLES,
}

PUNCTUATION = {
    ',': grammar.T_COMMA,
    '(': grammar.T_LROUND,
    ')': grammar.T_RROUND,
    ';': grammar.T_SEMI,
}

def parse(tokens):
    semantics = CGpmAnalyzeSemantics()
    parser = grammar.Parser(semantics)
    for token in tokenize(tokens):
        semantics.context.append(token)
        if len(semantics.context) > 10:
            semantics.context.pop(0)
        parser.feed(token)
    if semantics.errors:
        raise BQLParseError(semantics.errors)
    if semantics.failed:
        raise BQLParseError(['parse failed mysteriously'])
    assert semantics.phrases is not None
    return semantics.phrases


def tokenize(tokens):
    for token in tokens:
        if isinstance(token, str):
            if casefold(token) in KEYWORDS:
                yield KEYWORDS[casefold(token)], token
            elif token in PUNCTUATION:
                yield PUNCTUATION[token], token
            else:               # XXX check for alphanumeric/_
                yield grammar.L_NAME, token
        elif isinstance(token, (int, float)):
            yield grammar.L_NUMBER, token
        else:
            raise IOError('Invalid token: %r' % (token,))
    yield 0, ''                 # EOF


class CGpmAnalyzeSemantics(object):
    def __init__(self):
        self.context = []
        self.errors = []
        self.failed = False
        self.phrases = None

    def accept(self):
        pass
    def parse_failed(self):
        self.failed = True

    def syntax_error(self, (token, text)):
        if token == -1:         # error
            self.errors.append('Bad token: %r' % (text,))
        else:
            self.errors.append("Syntax error near [%s] after [%s]" % (
                text, ' '.join([str(t) for (_t, t) in self.context[:-1]])))

    def p_analysis_start(self, ps):             self.phrases = ps

    def p_phrases_one(self, p):                 return [p] if p else []
    def p_phrases_many(self, ps, p):
        if p: ps.append(p)
        return ps

    def p_phrase_none(self,):                   return None

    def p_phrase_variables(self, cols):         return Variables(cols)
    def p_phrase_skip(self, cols):              return Skip(cols)

    def p_phrase_rows(self, rows):               return Rows(rows)

    def p_phrase_loom(self):                    return Optimized('loom')
    def p_phrase_optimized(self):               return Optimized('lovecat')

    def p_phrase_quiet(self):                   return Quiet(True)

    def p_phrase_subproblems(self, s):          return Subproblem(s)

    def p_subproblems_list_one(self, s):        return [s]
    def p_subproblems_list_many(self, s):       return s

    def p_subproblems_one(self, s):             return [s]
    def p_subproblems_many(self, ss, s):        ss.append(s); return ss

    def p_subproblem_variable_hyperparameters(self):
        return 'variable_hyperparameters'

    def p_subproblem_variable_clustering(self):
        return 'variable_clustering'
    def p_subproblem_variable_clustering_concentration(self):
        return 'variable_clustering_concentration'

    def p_subproblem_row_clustering(self):
        return 'row_clustering'
    def p_subproblem_row_clustering_concentration(self):
        return 'row_clustering_concentration'

    def p_column_list_one(self, col):           return [col]
    def p_column_list_many(self, cols, col):    cols.append(col); return cols
    def p_column_name_n(self, name):            return name

    def p_row_list_one(self, row):           return [row]
    def p_row_list_many(self, rows, row):    rows.append(row); return rows
    def p_row_index_n(self, n):              return n


Optimized = namedtuple('Optimized', ['backend'])
Quiet = namedtuple('Quiet', ['flag'])
Rows = namedtuple('Rows', ['rows'])
Skip = namedtuple('Skip', ['vars'])
Subproblem = namedtuple('Subproblem', ['subproblems'])
Variables = namedtuple('Variables', ['vars'])
