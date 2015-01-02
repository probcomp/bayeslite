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

import bayeslite.ast as ast
import bayeslite.grammar as grammar
import bayeslite.scan as scan

def parse_bql(f, context):
    scanner = scan.BQLScanner(f, context)
    semantics = BQLSemantics()
    parser = grammar.Parser(semantics)
    while True:
        token = scanner.read()
        if token[0] == -1:      # error
            semantics.syntax_error(token)
        else:
            parser.feed(token)
        if token[0] == 0:       # EOF
            break
    return semantics.phrases

def parse_bql_string(string):
    return parse_bql(StringIO.StringIO(string), '(string)')

class BQLSemantics(object):
    def __init__(self):
        self.phrases = None

    def accept(self):
        assert self.phrases is not None
    def parse_failed(self):
        assert self.phrases is None
        # XXX Raise a principled exception here.
        raise Exception('Parse failed')
    def syntax_error(self, (_number, token)):
        # XXX Accumulate errors and report principled error at end.
        raise Exception('Syntax error near %s' % (token,))

    def p_bql_start(self, phrases):
        self.phrases = phrases
    def p_phrases_none(self):
        return []
    def p_phrases_some(self, phrases, phrase):
        if phrase is not None:
            phrases.append(phrase)
        return phrases

    def p_phrasesemi_empty(self):               return None
    def p_phrasesemi_nonempty(self, phrase):    return phrase
    def p_phrase_query(self, action, q):
        return QueryAction(action, q) if action else q
    def p_phrase_command(self, c):              return c

    def p_query_action_none(self):              return None
    def p_query_action_freq(self):              return ast.QACT_FREQ
    def p_query_action_hist(self):              return ast.QACT_HIST
    def p_query_action_summarize(self):         return ast.QACT_SUMMARIZE
    def p_query_action_plot(self):              return ast.QACT_PLOT

    def p_query_select(self, q):                return q
    def p_query_infer(self, q):                 return q
    def p_query_simulate(self, q):              return q
    def p_query_estimate_pairwise_row(self, q): return q
    def p_query_create_column_list(self, q):    return q

    def p_select_s(self, quant, cols, tabs, cond, group, ord, lim):
        return ast.Select(quant, cols, tabs, cond, group, ord, lim)

    def p_select_quant_distinct(self):          return ast.SELQUANT_DISTINCT
    def p_select_quant_all(self):               return ast.SELQUANT_ALL
    def p_select_quant_default(self):           return ast.SELQUANT_ALL

    def p_select_columns_one(self, c):          return [c]
    def p_select_columns_many(self, cs, c):     cs.append(c); return cs

    def p_select_column_star(self):             return ast.SelColAll(None)
    def p_select_column_qstar(self, table):     return ast.SelColAll(table)
    def p_select_column_exp(self, e, name):     return ast.SelColExp(e, name)
    def p_select_column_bql(self, bql):		return bql

    def p_select_bql_predprob(self, col):       return ast.SelBQLPredProb(col)
    def p_select_bql_prob(self, col, e):        return ast.SelBQLProb(col, e)
    def p_select_bql_typ_row(self):             return ast.SelBQLTypRow()
    def p_select_bql_typ_col(self, col):        return ast.SelBQLTypCol(col)
    def p_select_bql_sim(self, row, cols):      return ast.SelBQLSim(row, cols)
    def p_select_bql_depprob(self, cols):       return ast.SelBQLDepProb(*cols)
    def p_select_bql_mutinf(self, cols):        return ast.SelBQLMutInf(*cols)
    def p_select_bql_correl(self, cols):        return ast.SelBQLCorrel(*cols)

    def p_wrt_none(self):                       return [] # XXX None?
    def p_wrt_one(self, collist):               return [collist]
    def p_wrt_some(self, collists):             return collists

    def p_ofwith_with(self, col):               return (col, None)
    def p_ofwith_ofwith(self, col1, col2):      return (col1, col2)

    def p_column_lists_one(self, collist):
        return [collist]
    def p_column_lists_many(self, collists, collist):
        collists.append(collist)
        return collists

    def p_column_list_all(self):                return ast.ColListAll()
    def p_column_list_column(self, col):        return ast.ColListLit([col])

    def p_as_none(self):                        return None
    def p_as_some(self, name):                  return name

    def p_from_empty(self):                     return None
    def p_from_nonempty(self, tables):          return tables

    def p_select_tables_one(self, t):           return [t]
    def p_select_tables_many(self, ts, t):      ts.append(t); return ts
    def p_select_table_named(self, table, name): return ast.SelTab(table, name)
    def p_select_table_subquery(self, q, name):  return ast.SelTab(q, name)

    def p_where_unconditional(self):            return None
    def p_where_conditional(self, condition):   return condition

    def p_table_name_unqualified(self, name):   return name

    def p_group_by_none(self):                  return None
    def p_group_by_some(self, keys):            return keys

    def p_order_by_none(self):                  return None
    def p_order_by_some(self, keys):            return keys
    def p_order_keys_one(self, key):            return [key]
    def p_order_keys_many(self, keys, key):     keys.append(key); return keys
    def p_order_key_k(self, e, s):              return ast.Ord(e, s)
    def p_order_sense_none(self):               return ast.ORD_ASC
    def p_order_sense_asc(self):                return ast.ORD_ASC
    def p_order_sense_desc(self):               return ast.ORD_DESC

    def p_limit_none(self):                     return None
    def p_limit_some(self, limit):              return ast.Lim(limit, None)
    def p_limit_offset(self, limit, offset):    return ast.Lim(limit, offset)
    def p_limit_comma(self, offset, limit):     return ast.Lim(limit, offset)

    def p_opt_expressions_none(self):           return []
    def p_opt_expressions_some(self, es):       return es

    def p_expressions_one(self, e):             return [e]
    def p_expressions_many(self, es, e):        es.append(e); return es

    def p_expression_or(self, e):       return e
    def p_boolean_or_or(self, l, r):    return ast.op(ast.OP_BOOLOR, l, r)
    def p_boolean_or_and(self, a):      return a
    def p_boolean_and_and(self, l, r):  return ast.op(ast.OP_BOOLAND, l, r)
    def p_boolean_and_not(self, n):     return n
    def p_boolean_not_not(self, n):     return ast.op(ast.OP_BOOLNOT, n)
    def p_boolean_not_equality(self, c):
                                        return c
    def p_equality_is(self, l, r):      return ast.op(ast.OP_IS, l, r)
    def p_equality_isnot(self, l, r):   return ast.op(ast.OP_ISNOT, l, r)
    def p_equality_like(self, l, r):    return ast.op(ast.OP_LIKE, l, r)
    def p_equality_notlike(self, l, r): return ast.op(ast.OP_NOTLIKE, l, r)
    def p_equality_like_esc(self, l, r, e):
                                        return ast.op(ast.OP_LIKE_ESC, l, r, e)
    def p_equality_notlike_esc(self, l, r, e):
                                        return ast.op(ast.OP_NOTLIKE_ESC,
                                            l, r, e)
    def p_equality_glob(self, l, r):    return ast.op(ast.OP_GLOB, l, r)
    def p_equality_notglob(self, l, r): return ast.op(ast.OP_NOTGLOB, l, r)
    def p_equality_glob_esc(self, l, r, e):
                                        return ast.op(ast.OP_GLOB_ESC, l, r, e)
    def p_equality_notglob_esc(self, l, r, e):
                                        return ast.op(ast.OP_NOTGLOB_ESC,
                                            l, r, e)
    def p_equality_regexp(self, l, r):  return ast.op(ast.OP_REGEXP, l, r)
    def p_equality_notregexp(self, l, r):
                                        return ast.op(ast.OP_NOTREGEXP, l, r)
    def p_equality_regexp_esc(self, l, r, e):
                                        return ast.op(ast.OP_REGEXP_ESC,
                                            l, r, e)
    def p_equality_notregexp_esc(self, l, r, e):
                                        return ast.op(ast.OP_NOTREGEXP_ESC,
                                            l, r, e)
    def p_equality_match(self, l, r):   return ast.op(ast.OP_MATCH, l, r)
    def p_equality_notmatch(self, l, r):
                                        return ast.op(ast.OP_NOTMATCH, l, r)
    def p_equality_match_esc(self, l, r, e):
                                        return ast.op(ast.OP_MATCH_ESC, l, r, e)
    def p_equality_notmatch_esc(self, l, r, e):
                                        return ast.op(ast.OP_NOTMATCH_ESC,
                                            l, r, e)
    def p_equality_between(self, m, l, r):
                                        return ast.op(ast.OP_BETWEEN, m, l, r)
    def p_equality_notbetween(self, m, l, r):
                                        return ast.op(ast.OP_NOTBETWEEN, m,l,r)
    def p_equality_in(self, e, q):      return ast.ExpIn(e, True, q)
    def p_equality_notin(self, e, q):   return ast.ExpIn(e, False, q)
    def p_equality_isnull(self, e):     return ast.op(ast.OP_ISNULL, e)
    def p_equality_notnull(self, e):    return ast.op(ast.OP_NOTNULL, e)
    def p_equality_neq(self, l, r):     return ast.op(ast.OP_NEQ, l, r)
    def p_equality_eq(self, l, r):      return ast.op(ast.OP_EQ, l, r)
    def p_equality_ordering(self, o):   return o
    def p_ordering_lt(self, l, r):      return ast.op(ast.OP_LT, l, r)
    def p_ordering_leq(self, l, r):     return ast.op(ast.OP_LEQ, l, r)
    def p_ordering_geq(self, l, r):     return ast.op(ast.OP_GEQ, l, r)
    def p_ordering_gt(self, l, r):      return ast.op(ast.OP_GT, l, r)
    def p_ordering_bitwise(self, b):    return b
    def p_bitwise_and(self, l, r):      return ast.op(ast.OP_BITAND, l, r)
    def p_bitwise_ior(self, l, r):      return ast.op(ast.OP_BITIOR, l, r)
    def p_bitwise_lshift(self, l, r):   return ast.op(ast.OP_LSHIFT, l, r)
    def p_bitwise_rshift(self, l, r):   return ast.op(ast.OP_RSHIFT, l, r)
    def p_bitwise_additive(self, a):    return a
    def p_additive_add(self, l, r):     return ast.op(ast.OP_ADD, l, r)
    def p_additive_sub(self, l, r):     return ast.op(ast.OP_SUB, l, r)
    def p_additive_mult(self, m):       return m
    def p_multiplicative_mul(self, l, r):
                                        return ast.op(ast.OP_MUL, l, r)
    def p_multiplicative_div(self, l, r):
                                        return ast.op(ast.OP_DIV, l, r)
    def p_multiplicative_rem(self, l, r):
                                        return ast.op(ast.OP_REM, l, r)
    def p_multiplicative_conc(self, c): return c
    def p_concatenative_concat(self, l, r):
                                        return ast.op(ast.OP_CONCAT, l, r)
    def p_concatenative_collate(self, c):
                                        return c
    def p_collating_collate(self, e, c):
                                        return ast.ExpCollate(e, c)
    def p_collating_bitwise_not(self, n):
                                        return n
    def p_bitwise_not_not(self, n):     return ast.op(ast.OP_BITNOT, n)
    def p_bitwise_not_primary(self, p): return p

    def p_primary_literal(self, v):             return ast.ExpLit(v)
    def p_primary_apply(self, fn, es):          return ast.ExpApp(fn, es)
    def p_primary_paren(self, e):               return e
    def p_primary_subquery(self, q):            return ast.ExpSub(q)
    def p_primary_cast(self, e, t):             return ast.ExpCast(e, t)
    def p_primary_column(self, col):            return ast.ExpCol(None, col)
    def p_primary_tabcol(self, tab, col):       return ast.ExpCol(tab, col)

    def p_literal_null(self):                   return ast.LitNull(None)
    def p_literal_integer(self, i):             return ast.LitInt(i)
    def p_literal_float(self, f):               return ast.LitFloat(f)
    def p_literal_string(self, s):              return ast.LitString(s)

    def p_type_name(self, n):                   return ast.Type(n, [])
    def p_type_onearg(self, n, a):              return ast.Type(n, [a])
    def p_type_twoarg(self, n, a, b):           return ast.Type(n, [a, b])
    def p_typename_one(self, n):                return [n]
    def p_typename_many(self, tn, n):           tn.append(n); return tn
    def p_typearg_unsigned(self, i):            return i
    def p_typearg_positive(self, i):            return i
    def p_typearg_negative(self, i):            return -i
