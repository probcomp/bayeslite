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

import bayeslite.ast as ast
from bayeslite.parse import parse_bql_string

def test_empty():
    assert [] == parse_bql_string('')
    assert [] == parse_bql_string(';')
    assert [] == parse_bql_string(';;')
    assert [] == parse_bql_string(' ;')
    assert [] == parse_bql_string('; ')
    assert [] == parse_bql_string(' ; ')
    assert [] == parse_bql_string(' ; ; ')

def test_select_trivial():
    assert parse_bql_string('select null;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitNull(None)), None)],
            None, None, None, None, None)]
    assert parse_bql_string("select 'x';") == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitString('x')), None)],
            None, None, None, None, None)]
    assert parse_bql_string("select 'x''y';") == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitString("x'y")), None)],
            None, None, None, None, None)]
    assert parse_bql_string('select "x";') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpCol(None, 'x'), None)],
            None, None, None, None, None)]
    assert parse_bql_string('select "x""y";') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpCol(None, 'x"y'), None)],
            None, None, None, None, None)]
    assert parse_bql_string('select 0;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitInt(0)), None)],
            None, None, None, None, None)]
    assert parse_bql_string('select 0 as z;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitInt(0)), 'z')],
            None, None, None, None, None)]
    assert parse_bql_string('select * from t;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select t1.* from t1;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll('t1')],
            [ast.SelTab('t1', None)], None, None, None, None)]
    assert parse_bql_string('select c from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpCol(None, 'c'), None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select c as d from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpCol(None, 'c'), 'd')],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select t.c as d from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpCol('t', 'c'), 'd')],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select t.c as d, p as q, x from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [
                ast.SelColExp(ast.ExpCol('t', 'c'), 'd'),
                ast.SelColExp(ast.ExpCol(None, 'p'), 'q'),
                ast.SelColExp(ast.ExpCol(None, 'x'), None),
            ],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select * from t, u;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
            [ast.SelTab('t', None), ast.SelTab('u', None)],
            None, None, None, None)]
    assert parse_bql_string('select * from t as u;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
            [ast.SelTab('t', 'u')],
            None, None, None, None)]
    assert parse_bql_string('select * where x;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
            None, ast.ExpCol(None, 'x'), None, None, None)]
    assert parse_bql_string('select * from t where x;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
            [ast.SelTab('t', None)],
            ast.ExpCol(None, 'x'), None, None, None)]
    assert parse_bql_string('select * group by x;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
            None, None, [ast.ExpCol(None, 'x')], None, None)]
    assert parse_bql_string('select * from t where x group by y;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
            [ast.SelTab('t', None)],
            ast.ExpCol(None, 'x'),
            [ast.ExpCol(None, 'y')], None, None)]
    assert parse_bql_string('select * from t where x group by y, z;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
            [ast.SelTab('t', None)],
            ast.ExpCol(None, 'x'),
            [ast.ExpCol(None, 'y'), ast.ExpCol(None, 'z')], None, None)]
    assert parse_bql_string('select * order by x;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
            None, None, None, [ast.Ord(ast.ExpCol(None, 'x'), ast.ORD_ASC)],
            None)]
    assert parse_bql_string('select * order by x asc;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
            None, None, None, [ast.Ord(ast.ExpCol(None, 'x'), ast.ORD_ASC)],
            None)]
    assert parse_bql_string('select * order by x desc;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
            None, None, None, [ast.Ord(ast.ExpCol(None, 'x'), ast.ORD_DESC)],
            None)]
    assert parse_bql_string('select * order by x, y;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
            None, None, None,
            [ast.Ord(ast.ExpCol(None, 'x'), ast.ORD_ASC),
             ast.Ord(ast.ExpCol(None, 'y'), ast.ORD_ASC)],
            None)]
    assert parse_bql_string('select * order by x desc, y;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
            None, None, None,
            [ast.Ord(ast.ExpCol(None, 'x'), ast.ORD_DESC),
             ast.Ord(ast.ExpCol(None, 'y'), ast.ORD_ASC)],
            None)]
    assert parse_bql_string('select * order by x, y asc;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
            None, None, None,
            [ast.Ord(ast.ExpCol(None, 'x'), ast.ORD_ASC),
             ast.Ord(ast.ExpCol(None, 'y'), ast.ORD_ASC)],
            None)]
    assert parse_bql_string('select * limit 32;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
            None, None, None, None,
            ast.Lim(ast.ExpLit(ast.LitInt(32)), None))]
    assert parse_bql_string('select * limit 32 offset 16;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
            None, None, None, None,
            ast.Lim(ast.ExpLit(ast.LitInt(32)), ast.ExpLit(ast.LitInt(16))))]
    assert parse_bql_string('select * limit 16, 32;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
            None, None, None, None,
            ast.Lim(ast.ExpLit(ast.LitInt(32)), ast.ExpLit(ast.LitInt(16))))]

def test_select_bql():
    assert parse_bql_string('select predictive probability of c from t;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelBQLPredProb('c')],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select predictive probability of c, * from t;') \
        == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelBQLPredProb('c'), ast.SelColAll(None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select c, predictive probability of d from t;') \
        == \
        [ast.Select(ast.SELQUANT_ALL,
            [
                ast.SelColExp(ast.ExpCol(None, 'c'), None),
                ast.SelBQLPredProb('d'),
            ],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select predictive probability of c, d from t;') \
        == \
        [ast.Select(ast.SELQUANT_ALL,
            [
                ast.SelBQLPredProb('c'),
                ast.SelColExp(ast.ExpCol(None, 'd'), None),
            ],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select probability of c = 42 from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelBQLProb('c', ast.ExpLit(ast.LitInt(42)))],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select typicality from t;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelBQLTypRow()],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select typicality of c from t;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelBQLTypCol('c')],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select similarity to 8 from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelBQLSim(ast.ExpLit(ast.LitInt(8)), [])],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string(
            'select similarity to 8 with respect to c from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelBQLSim(ast.ExpLit(ast.LitInt(8)), [ast.ColListLit(['c'])])],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string(
            'select similarity to 8 with respect to c, d from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [
                ast.SelBQLSim(ast.ExpLit(ast.LitInt(8)),
                    [ast.ColListLit(['c'])]),
                ast.SelColExp(ast.ExpCol(None, 'd'), None),
            ],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string(
            'select similarity to 8 with respect to (c, d) from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelBQLSim(ast.ExpLit(ast.LitInt(8)),
                [ast.ColListLit(['c']), ast.ColListLit(['d'])])],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select dependence probability with c from t;') ==\
        [ast.Select(ast.SELQUANT_ALL, [ast.SelBQLDepProb('c', None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string(
            'select dependence probability of c with d from t;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelBQLDepProb('c', 'd')],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select mutual information with c from t;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelBQLMutInf('c', None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string(
            'select mutual information of c with d from t;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelBQLMutInf('c', 'd')],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select correlation with c from t;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelBQLCorrel('c', None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select correlation of c with d from t;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelBQLCorrel('c', 'd')],
            [ast.SelTab('t', None)], None, None, None, None)]
