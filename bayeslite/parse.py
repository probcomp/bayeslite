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
    def p_group_keys_one(self, key):            return [key]
    def p_group_keys_many(self, keys, key):     keys.append(key); return keys

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

    def p_expression_literal(self, v):          return ast.ExpLit(v)
    def p_expression_paren(self, e):            return e
    def p_expression_subquery(self, q):         return ast.ExpSub(q)
    def p_expression_column(self, col):         return ast.ExpCol(None, col)
    def p_expression_tabcol(self, tab, col):    return ast.ExpCol(tab, col)

    def p_literal_null(self):                   return ast.LitNull(None)
    def p_literal_integer(self, i):             return ast.LitInt(i)
    def p_literal_float(self, f):               return ast.LitFloat(f)
    def p_literal_string(self, s):              return ast.LitString(s)
