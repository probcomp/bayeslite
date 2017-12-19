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
    'and': grammar.K_AND,
    'category': grammar.K_CATEGORY,
    'expose': grammar.K_EXPOSE,
    'for': grammar.K_FOR,
    'generative': grammar.K_GENERATIVE,
    'given': grammar.K_GIVEN,
    'latent': grammar.K_LATENT,
    'model': grammar.K_MODEL,
    'override': grammar.K_OVERRIDE,
    'set': grammar.K_SET,
    'subsample': grammar.K_SUBSAMPLE,
    'to': grammar.K_TO,
    'using': grammar.K_USING,
}

PUNCTUATION = {
    '(': grammar.T_LROUND,
    ')': grammar.T_RROUND,
    ',': grammar.T_COMMA,
    '=': grammar.T_EQ,
    ';': grammar.T_SEMI,
}

def parse(tokenses):
    semantics = CGPM_Semantics()
    parser = grammar.Parser(semantics)
    for token in tokenize(tokenses):
        semantics.context.append(token)
        if len(semantics.context) > 10:
            semantics.context.pop(0)
        parser.feed(token)
    if semantics.errors:
        raise BQLParseError(semantics.errors)
    if semantics.failed:
        raise BQLParseError(['parse failed mysteriously'])
    assert semantics.schema is not None
    return semantics.schema

def tokenize(tokenses):
    for token in intersperse(',', [flatten(tokens) for tokens in tokenses]):
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

def intersperse(comma, l):
    if len(l) == 0:
        return []
    it = iter(l)
    result = list(it.next())
    for l_ in it:
        result.append(comma)
        result += l_
    return result

def flatten(l):
    def flatten1(l, f):
        for x in l:
            if isinstance(x, list):
                f.append('(')
                flatten1(x, f)
                f.append(')')
            else:
                f.append(x)
    f = []
    flatten1(l, f)
    return f

class CGPM_Semantics(object):
    def __init__(self):
        self.context = []
        self.errors = []
        self.failed = False
        self.schema = None

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

    def p_cgpm_empty(self):                     self.schema = []
    def p_cgpm_schema(self, s):                 self.schema = s

    def p_schema_one(self, c):                  return [c]
    def p_schema_some(self, s, c):
        if c:
            s.append(c)
        return s
    def p_clause_opt_none(self):                return None
    def p_clause_opt_some(self, c):             return c

    def p_clause_basic(self, var, dist, params):
        return Basic(var, dist, params)
    def p_clause_foreign(self, outputs, inputs, exposed, name, params):
        return Foreign(outputs, inputs, exposed, name, params)
    def p_clause_subsamp(self, n):
        return Subsample(n)
    def p_clause_latent(self, var, st):
        return Latent(var, st)

    def p_dist_name(self, dist):                return casefold(dist)
    def p_foreign_name(self, foreign):          return casefold(foreign)

    def p_given_opt_none(self):                 return []
    def p_given_opt_some(self, vars):           return vars

    def p_exposing_opt_none(self):              return []
    def p_exposing_opt_one(self, exp):          return exp

    def p_and_opt_none(self):                   return None
    def p_and_opt_one(self):                    return None

    def p_exposed_one(self, v, s):              return [(v,s)]
    def p_exposed_many(self, exp, v, s):        exp.append((v,s)); return exp

    def p_vars_one(self, var):                  return [var]
    def p_vars_many(self, vars, var):           vars.append(var); return vars
    def p_var_name(self, var):                  return casefold(var)

    def p_stattype_s(self, st):                 return st

    def p_param_opt_none(self):                 return []
    def p_param_opt_some(self, ps):             return ps
    def p_params_one(self, param):              return [param]
    def p_params_many(self, params, param):
        params.append(param); return params
    def p_param_num(self, p, num):              return (p, num)
    def p_param_nam(self, p, nam):              return (p, nam)

Basic = namedtuple('Basic', [
    'var',
    'dist',
    'params',
])

Foreign = namedtuple('Foreign', [
    'outputs',
    'inputs',
    'exposed',
    'name',
    'params',
])

Subsample = namedtuple('Subsample', [
    'n',
])

Latent = namedtuple('Latent', [
    'name',
    'stattype',
])
