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

import contextlib
import itertools
import pytest

import bayeslite
import bayeslite.ast as ast
import bayeslite.parse as parse

def parse_bql_string(string):
    phrases = list(parse.parse_bql_string(string))
    phrase_pos = list(parse.parse_bql_string_pos(string))
    assert len(phrases) == len(phrase_pos)
    start = 0
    for i in range(len(phrase_pos)):
        phrase, pos = phrase_pos[i]
        assert phrases[i] == phrase
        substring = buffer(string, start, len(string) - start)
        phrase0, pos0 = parse.parse_bql_string_pos_1(substring)
        assert phrase0 == phrase
        assert pos0 == pos - start
        start = pos
    return phrases

def test_empty():
    assert [] == parse_bql_string('')
    assert [] == parse_bql_string(';')
    assert [] == parse_bql_string(';;')
    assert [] == parse_bql_string(' ;')
    assert [] == parse_bql_string('; ')
    assert [] == parse_bql_string(' ; ')
    assert [] == parse_bql_string(' ; ; ')

def test_multiquery():
    assert parse_bql_string('select 0; select 1;') == [
        ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitInt(0)), None)],
            None, None, None, None, None),
        ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitInt(1)), None)],
            None, None, None, None, None),
    ]
    assert parse_bql_string('select 0; select 1') == [
        ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitInt(0)), None)],
            None, None, None, None, None),
        ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitInt(1)), None)],
            None, None, None, None, None),
    ]

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
    assert parse_bql_string('select 0.;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitFloat(0)), None)],
            None, None, None, None, None)]
    assert parse_bql_string('select .0;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitFloat(0)), None)],
            None, None, None, None, None)]
    assert parse_bql_string('select 0.0;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitFloat(0)), None)],
            None, None, None, None, None)]
    assert parse_bql_string('select 1e0;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitFloat(1)), None)],
            None, None, None, None, None)]
    assert parse_bql_string('select 1e+1;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitFloat(10)), None)],
            None, None, None, None, None)]
    assert parse_bql_string('select 1e-1;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitFloat(.1)), None)],
            None, None, None, None, None)]
    assert parse_bql_string('select 1.e0;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitFloat(1)), None)],
            None, None, None, None, None)]
    assert parse_bql_string('select .1e0;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitFloat(.1)), None)],
            None, None, None, None, None)]
    assert parse_bql_string('select .1e1;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitFloat(1)), None)],
            None, None, None, None, None)]
    assert parse_bql_string('select 1.e10;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitFloat(1e10)), None)],
            None, None, None, None, None)]
    assert parse_bql_string('select all 0;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpLit(ast.LitInt(0)), None)],
            None, None, None, None, None)]
    assert parse_bql_string('select distinct 0;') == \
        [ast.Select(ast.SELQUANT_DISTINCT,
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
            None, None, ast.Grouping([ast.ExpCol(None, 'x')], None),
            None, None)]
    assert parse_bql_string('select * from t where x group by y;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
            [ast.SelTab('t', None)],
            ast.ExpCol(None, 'x'),
            ast.Grouping([ast.ExpCol(None, 'y')], None), None, None)]
    assert parse_bql_string('select * from t where x group by y, z;') == \
        [ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
            [ast.SelTab('t', None)],
            ast.ExpCol(None, 'x'),
            ast.Grouping([ast.ExpCol(None, 'y'), ast.ExpCol(None, 'z')], None),
            None, None)]
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
    assert parse_bql_string('select (select0);') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpCol(None, 'select0'), None)],
            None, None, None, None, None)]
    assert parse_bql_string('select (select 0);') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpSub(ast.Select(ast.SELQUANT_ALL,
                    [ast.SelColExp(ast.ExpLit(ast.LitInt(0)), None)],
                    None, None, None, None, None)
                ),
                None,
            )],
            None, None, None, None, None)]
    assert parse_bql_string('select f(f(), f(x), f(*), f(distinct x), y);') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpApp(False, 'f', [
                    ast.ExpApp(False, 'f', []),
                    ast.ExpApp(False, 'f', [ast.ExpCol(None, 'x')]),
                    ast.ExpAppStar('f'),
                    ast.ExpApp(True, 'f', [ast.ExpCol(None, 'x')]),
                    ast.ExpCol(None, 'y'),
                ]),
                None,
            )],
            None, None, None, None, None)]

def test_select_bql():
    assert parse_bql_string('select predictive probability of c from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLPredProb(
                [ast.ColListLit(['c'])], []), None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select predictive probability of (c) from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLPredProb(
                [ast.ColListLit(['c'])], []), None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select predictive probability of c, * from t;') \
        == \
        [ast.Select(ast.SELQUANT_ALL,
            [
                ast.SelColExp(ast.ExpBQLPredProb(
                    [ast.ColListLit(['c'])], []), None),
                ast.SelColAll(None),
            ],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select predictive probability of (*), * from t;') \
        == \
        [ast.Select(ast.SELQUANT_ALL,
            [
                ast.SelColExp(
                    ast.ExpBQLPredProb([ast.ColListAll()], []), None),
                ast.SelColAll(None),
            ],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string(
        'select c, predictive probability of d given (c, f) from t;') \
        == \
        [ast.Select(ast.SELQUANT_ALL,
            [
                ast.SelColExp(ast.ExpCol(None, 'c'), None),
                ast.SelColExp(ast.ExpBQLPredProb(
                    [ast.ColListLit(['d'])],
                    [ast.ColListLit(['c']), ast.ColListLit(['f'])]),
                    None
                ),
            ],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string(
        'select c, predictive probability of (d, w, *) given (c, *) from t;') \
        == \
        [ast.Select(ast.SELQUANT_ALL,
            [
                ast.SelColExp(ast.ExpCol(None, 'c'), None),
                ast.SelColExp(ast.ExpBQLPredProb(
                    [ast.ColListLit(['d']), ast.ColListLit(['w']),
                        ast.ColListAll()],
                    [ast.ColListLit(['c']), ast.ColListAll()]),
                    None
                ),
            ],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select predictive probability of c, d from t;') \
        == \
        [ast.Select(ast.SELQUANT_ALL,
            [
                ast.SelColExp(ast.ExpBQLPredProb(
                    [ast.ColListLit(['c'])], []), None),
                ast.SelColExp(ast.ExpCol(None, 'd'), None),
            ],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select probability density of c = 42'
            ' from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpBQLProbDensity([('c', ast.ExpLit(ast.LitInt(42)))],
                    []),
                None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    # Using * for context will later cause a crash in the compiler!
    assert parse_bql_string(
        'select similarity in the context of * from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpBQLSim(None, None, [ast.ColListAll()]),
                None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string(
        'select similarity to (rowid=8) in the context of s from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpBQLSim(
                    None,
                    ast.ExpOp(ast.OP_EQ, (
                        ast.ExpCol(None, 'rowid'),
                        ast.ExpLit(ast.LitInt(8))
                    )),
                    [ast.ColListLit(['s'])]),
                None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    with pytest.raises(parse.BQLParseError):
        # Cannot use similarity of without to.
        parse_bql_string(
            'select similarity of (rowid=8) in the context of r from t')
    assert parse_bql_string(
        'select similarity in the context of c from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLSim(None, None, [ast.ColListLit(['c'])]),
                None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string(
            'select similarity to (rowid=5) in the context of age from t1;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpBQLSim(
                    None,
                    ast.ExpOp(ast.OP_EQ, (
                        ast.ExpCol(None, 'rowid'),
                        ast.ExpLit(ast.LitInt(5)),
                    )),
                    [ast.ColListLit(['age'])]),
                None)],
            [ast.SelTab('t1', None)], None, None, None, None)]
    assert parse_bql_string(
            'select similarity to (rowid=8) in the context of c, d from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [
                ast.SelColExp(
                    ast.ExpBQLSim(
                        None,
                        ast.ExpOp(ast.OP_EQ, (
                            ast.ExpCol(None, 'rowid'),
                            ast.ExpLit(ast.LitInt(8)),
                        )),
                        [ast.ColListLit(['c'])]),
                    None),
                ast.SelColExp(ast.ExpCol(None, 'd'), None),
            ],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select similarity to (rowid=8) in the context of' +
            ' (estimate * from columns of t order by ' +
            '  probability density of value 4 limit 1)' +
            ' from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpBQLSim(
                    None,
                    ast.ExpOp(ast.OP_EQ, (
                        ast.ExpCol(None, 'rowid'),
                        ast.ExpLit(ast.LitInt(8)),
                    )),
                    [ast.ColListSub(
                        ast.EstCols(
                            [ast.SelColAll(None)], 't', None, None, None,
                            [ast.Ord(ast.ExpBQLProbDensityFn(
                                    ast.ExpLit(ast.LitInt(4)),
                                    []),
                                ast.ORD_ASC)],
                            ast.Lim(ast.ExpLit(ast.LitInt(1)), None))
                    )]),
                None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    # Required TO variable.
    with pytest.raises(parse.BQLParseError):
        parse_bql_string(
            'select similarity of ("name" = \'Bar\') from t;')
    # Missing context variable.
    with pytest.raises(bayeslite.BQLParseError):
        parse_bql_string(
            'select similarity of ("name" = \'Bar\') to (rowid=8) '
                'AS "sim_bar_8" from t;')
    assert parse_bql_string(
            'select similarity of ("name" = \'Bar\') to (rowid=8) '
                'in the context of c AS "sim_bar_8" from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpBQLSim(
                    ast.ExpOp(ast.OP_EQ, (
                        ast.ExpCol(None, 'name'),
                        ast.ExpLit(ast.LitString('Bar')),
                    )),
                    ast.ExpOp(ast.OP_EQ, (
                        ast.ExpCol(None, 'rowid'),
                        ast.ExpLit(ast.LitInt(8)),
                    )),
                    [ast.ColListLit(['c'])]),
                'sim_bar_8')],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string(
            'select similarity of ("name" = \'Bar\') to (rowid=8)' +
            'in the context of (estimate * from columns of t ' +
                'order by probability density of value 4 limit 1)' +
            'from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpBQLSim(
                    ast.ExpOp(ast.OP_EQ, (
                        ast.ExpCol(None, 'name'),
                        ast.ExpLit(ast.LitString('Bar')),
                    )),
                    ast.ExpOp(ast.OP_EQ, (
                        ast.ExpCol(None, 'rowid'),
                        ast.ExpLit(ast.LitInt(8)),
                    )),
                    [ast.ColListSub(
                        ast.EstCols(
                            [ast.SelColAll(None)], 't', None, None, None,
                            [ast.Ord(ast.ExpBQLProbDensityFn(
                                    ast.ExpLit(ast.LitInt(4)),
                                    []),
                                ast.ORD_ASC)],
                            ast.Lim(ast.ExpLit(ast.LitInt(1)), None))
                    )]),
                None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select dependence probability with c from t;') ==\
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLDepProb('c', None), None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string(
            'select dependence probability of c with d from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLDepProb('c', 'd'), None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select mutual information with c from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLMutInf(['c'], None, None, None), None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select mutual information with (c) from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLMutInf(['c'], None, None, None), None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string(
            'select mutual information of c with (d) from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLMutInf(['c'], ['d'], None, None),
            None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string(
            'select mutual information of (a, b, q) with (d, r) '
            'given (f, z=2, w) from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLMutInf(
                ['a', 'b', 'q'], ['d', 'r'],
                [('f', ast.ExpLit(ast.LitNull(0))),
                    ('z',ast.ExpLit(ast.LitInt(2))),
                    ('w', ast.ExpLit(ast.LitNull(0)))],
                None),
            None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select mutual information of c with d' +
            ' using (1+2) samples from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpBQLMutInf(
                    ['c'], ['d'], None,
                    ast.op(
                        ast.OP_ADD, ast.ExpLit(ast.LitInt(1)),
                        ast.ExpLit(ast.LitInt(2)))),
                None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('''
            select mutual information with c given (d, a=1) using
            10 samples from t;
            ''') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpBQLMutInf(
                    ['c'], None,
                    [('d', ast.ExpLit(ast.LitNull(0))),
                        ('a',ast.ExpLit(ast.LitInt(1)))],
                    ast.ExpLit(ast.LitInt(10))
                ),
            None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('''
            select mutual information with (c, f) given (d, a=1) using
            10 samples from t;
            ''') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpBQLMutInf(
                    ['c', 'f'],
                    None,
                    [('d', ast.ExpLit(ast.LitNull(0))),
                        ('a',ast.ExpLit(ast.LitInt(1)))],
                    ast.ExpLit(ast.LitInt(10))
                ),
            None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('''
            select mutual information of b with c
            given (d, a=1, e, r=2) from t;''') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpBQLMutInf(
                    ['b'],
                    ['c'],
                    [
                        ('d', ast.ExpLit(ast.LitNull(0))),
                        ('a',ast.ExpLit(ast.LitInt(1))),
                        ('e', ast.ExpLit(ast.LitNull(0))),
                        ('r', ast.ExpLit(ast.LitInt(2))),
                    ],
                    None
                ),
            None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select correlation with c from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLCorrel('c', None), None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select correlation of c with d from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLCorrel('c', 'd'), None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    # XXX This got broken a while ago: parenthesization in PROBABILITY
    # OF X = E is too permissive.  I didn't notice because before I
    # introduced BQLParseError, this simply caught Exception -- which
    # covered the AssertionError that this turned into.
    #
    # with pytest.raises(parse.BQLParseError):
    #     parse_bql_string('select probability density of x = 1 -' +
    #         ' probability density of y = 0 from t;')
    #     # XXX Should really be this test, but getting the grammar to
    #     # admit this unambiguously is too much of a pain at the
    #     # moment.
    #     assert parse_bql_string('select probability density of x = 1 -' +
    #             ' probability density of y = 0 from t;') == \
    #         [ast.Select(ast.SELQUANT_ALL,
    #             [ast.SelColExp(ast.ExpBQLProbDensity([('x',
    #                         ast.ExpOp(ast.OP_SUB, (
    #                             ast.ExpLit(ast.LitInt(1)),
    #                             ast.ExpBQLProbDensity([('y',
    #                                     ast.ExpLit(ast.LitInt(0)))],
    #                                 []),
    #                         )))],
    #                     []),
    #                 None)],
    #             [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select probability density of c1 = f(c2)'
            ' from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLProbDensity([('c1',
                        ast.ExpApp(False, 'f', [ast.ExpCol(None, 'c2')]))],
                    []),
                None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select key, t.(estimate * from columns of t'
            ' order by dependence probability with c desc limit 4)'
            ' from t order by key asc') == \
        [ast.Select(ast.SELQUANT_ALL, [
                ast.SelColExp(ast.ExpCol(None, 'key'), None),
                ast.SelColSub('t',
                    ast.EstCols([ast.SelColAll(None)], 't', None, None, None,
                        [ast.Ord(ast.ExpBQLDepProb('c', None), ast.ORD_DESC)],
                        ast.Lim(ast.ExpLit(ast.LitInt(4)), None)))
            ],
            [ast.SelTab('t', None)],
            None, None,
            [ast.Ord(ast.ExpCol(None, 'key'), ast.ORD_ASC)],
            None)]

def test_predictive_relevance():
    with pytest.raises(parse.BQLParseError):
        # No ofcondition, tocondition, or constraints.
        parse_bql_string(
            'select predictive relevance in the context of f from t;')
    with pytest.raises(parse.BQLParseError):
        # No tocondition, or constraints.
        parse_bql_string(
            'select predictive relevance of (rowid=8) '
            'in the context of q from t')
    assert parse_bql_string(
        'select predictive relevance to existing rows (rowid=8 AND age < 10) '
        'in the context of "s" from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpBQLPredRel(
                    ofcondition=None,
                    tocondition=ast.ExpOp(ast.OP_BOOLAND, (
                        ast.ExpOp(ast.OP_EQ, (
                            ast.ExpCol(None, 'rowid'),
                            ast.ExpLit(ast.LitInt(8))
                        )),
                        ast.ExpOp(ast.OP_LT, (
                            ast.ExpCol(None, 'age'),
                            ast.ExpLit(ast.LitInt(10))
                        )),
                    )),
                    hypotheticals=None,
                    column=[ast.ColListLit(['s'])]),
                None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('''
        select predictive relevance
            of (name = 'Uganda')
            to hypothetical rows with values (
                ("gdp_per_capita" = 82, "mortality" = 14),
                ("gdp_per_capita" = 74, continent = 'Europe', "mortality" = 7)
            )
            in the context of
              "gdp_per_capita"
        from t
        ''') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpBQLPredRel(
                    ofcondition=ast.ExpOp(ast.OP_EQ, (
                        ast.ExpCol(None, 'name'),
                        ast.ExpLit(ast.LitString('Uganda'))
                    )),
                    tocondition=None,
                    hypotheticals=[[
                        ('gdp_per_capita',ast.ExpLit(ast.LitInt(82))),
                        ('mortality',ast.ExpLit(ast.LitInt(14)))
                    ],
                    [
                        ('gdp_per_capita',ast.ExpLit(ast.LitInt(74))),
                        ('continent',ast.ExpLit(ast.LitString('Europe'))),
                        ('mortality',ast.ExpLit(ast.LitInt(7))),
                    ]],
                    column=[ast.ColListLit(['gdp_per_capita'])],
                ),
                None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('''
        select predictive relevance
            of (name = 'Uganda')
            to existing rows (rowid between 1 AND 100)
            and hypothetical rows with values (
                ("gdp_per_capita" = 82, "mortality" = 14),
                ("gdp_per_capita" = 74, continent = 'Europe', "mortality" = 7)
            )
            in the context of
              "gdp_per_capita"
        from t
        ''') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpBQLPredRel(
                    ofcondition=ast.ExpOp(ast.OP_EQ, (
                        ast.ExpCol(None, 'name'),
                        ast.ExpLit(ast.LitString('Uganda'))
                    )),
                    tocondition=ast.ExpOp(ast.OP_BETWEEN,
                        (ast.ExpCol(None, 'rowid'),
                            ast.ExpLit(ast.LitInt(1)),
                            ast.ExpLit(ast.LitInt(100)))),
                    hypotheticals=[[
                        ('gdp_per_capita',ast.ExpLit(ast.LitInt(82))),
                        ('mortality',ast.ExpLit(ast.LitInt(14)))
                    ],
                    [
                        ('gdp_per_capita',ast.ExpLit(ast.LitInt(74))),
                        ('continent',ast.ExpLit(ast.LitString('Europe'))),
                        ('mortality',ast.ExpLit(ast.LitInt(7))),
                    ]],
                    column=[ast.ColListLit(['gdp_per_capita'])],
                ),
                None)],
            [ast.SelTab('t', None)], None, None, None, None)]

def test_trivial_scan_error():
    with pytest.raises(parse.BQLParseError):
        parse_bql_string('select 0c;')
    with pytest.raises(parse.BQLParseError):
        parse_bql_string('select 1.0p1;')

def test_trivial_precedence_error():
    with pytest.raises(parse.BQLParseError):
        parse_bql_string('select similarity to similarity to 0' +
            ' in the context of c from t;')

def test_trivial_commands():
    assert parse_bql_string('''
        create population satellites for satellites_ucs (
            guess(*);
        )
    ''') == \
        [ast.CreatePop(False, 'satellites', 'satellites_ucs', [
            ast.PopGuessVars('*'),
        ])]
    assert parse_bql_string('''
        create population for satellites_ucs (
            guess(*);
        )
    ''') == \
        [ast.CreatePop(False, None, 'satellites_ucs', [
            ast.PopGuessVars('*'),
        ])]
    assert parse_bql_string('''
        create population satellites for satellites_ucs (
            guess stattypes of launch_site, "contracto=r";
            set stattype of country_of_operator, orbit_type to nominal;
            set stattype of launch_mass to numerical;
            set stattype of perigee to numerical;
            set stattype of apogee, period to numerical;
        )
    ''') == \
        [ast.CreatePop(False, 'satellites', 'satellites_ucs', [
            ast.PopGuessVars(['launch_site', 'contracto=r']),
            ast.PopModelVars(
                ['country_of_operator', 'orbit_type'], 'nominal'),
            ast.PopModelVars(['launch_mass'], 'numerical'),
            ast.PopModelVars(['perigee'], 'numerical'),
            ast.PopModelVars(['apogee', 'period'], 'numerical'),
        ])]
    assert parse_bql_string('''
        create population satellites for satellites_ucs (
            set stattype of country_of_operator, orbit_type to nominal;;
            set stattype of apogee, period to numerical;;
        )
    ''') == \
        [ast.CreatePop(False, 'satellites', 'satellites_ucs', [
            ast.PopModelVars(
                ['country_of_operator', 'orbit_type'], 'nominal'),
            ast.PopModelVars(['apogee', 'period'], 'numerical'),
        ])]
    assert parse_bql_string('''
        create population satellites for satellites_ucs (
            country_of_operator nominal;
            orbit_type          nominal;
            launch_mass         numerical;
            perigee             numerical;
            apogee              numerical;
            period              numerical;
        )
    ''') == \
        [ast.CreatePop(False, 'satellites', 'satellites_ucs', [
            ast.PopModelVars(['country_of_operator'], 'nominal'),
            ast.PopModelVars(['orbit_type'], 'nominal'),
            ast.PopModelVars(['launch_mass'], 'numerical'),
            ast.PopModelVars(['perigee'], 'numerical'),
            ast.PopModelVars(['apogee'], 'numerical'),
            ast.PopModelVars(['period'], 'numerical'),
        ])]
    assert parse_bql_string('drop population satellites') == \
        [ast.DropPop(False, 'satellites')]
    assert parse_bql_string('create generator t_cc for t using cgpm'
            '(xyz numerical, pqr nominal, lmn cyclic)') == \
        [ast.CreateGen('t_cc', False, 't', 'cgpm', [
            ['xyz', 'numerical'],
            ['pqr', 'nominal'],
            ['lmn', 'cyclic'],
        ])]
    assert parse_bql_string('create generator for t using cgpm'
            '(xyz numerical, pqr nominal, lmn cyclic)') == \
        [ast.CreateGen(None, False, 't', 'cgpm', [
            ['xyz', 'numerical'],
            ['pqr', 'nominal'],
            ['lmn', 'cyclic'],
        ])]
    assert parse_bql_string('create generator t_cc for t'
            '(xyz numerical, pqr nominal, lmn cyclic)') == \
        [ast.CreateGen(
            't_cc', False, 't',
            None,       # Backend defaults to cgpm.
            [
                ['xyz', 'numerical'],
                ['pqr', 'nominal'],
                ['lmn', 'cyclic'],
        ])]
    assert parse_bql_string('create generator if not exists t_cc'
            ' for t using cgpm'
            '(xyz numerical, pqr nominal, lmn cyclic)') == \
        [ast.CreateGen('t_cc', True, 't', 'cgpm', [
            ['xyz', 'numerical'],
            ['pqr', 'nominal'],
            ['lmn', 'cyclic'],
        ])]
    assert parse_bql_string('create generator if not exists t_cc'
            ' for t using cgpm'
            '(xyz numerical, pqr nominal, lmn cyclic)') == \
        [ast.CreateGen('t_cc', True, 't', 'cgpm', [
            ['xyz', 'numerical'],
            ['pqr', 'nominal'],
            ['lmn', 'cyclic'],
        ])]
    # XXX Schema of [[]] instead of [] is kinda wacky.  Fix?  (But
    # make sure the empty-parens and no-parens cases are equivalent.)
    assert parse_bql_string('create generator t_cc'
            ' for t using cgpm()') == \
        [ast.CreateGen('t_cc', False, 't', 'cgpm', [[]])]
    assert parse_bql_string('create generator t_cc'
            ' for t using cgpm') == \
        [ast.CreateGen('t_cc', False, 't', 'cgpm', [[]])]
    assert parse_bql_string('initialize 1 model for t;') == \
        [ast.InitModels(False, 't', 1)]
    assert parse_bql_string('initialize 1 model if not exists for t;') == \
        [ast.InitModels(True, 't', 1)]
    assert parse_bql_string('initialize 2 models for t;') == \
        [ast.InitModels(False, 't', 2)]
    assert parse_bql_string('initialize 2 models if not exists for t;') == \
        [ast.InitModels(True, 't', 2)]
    assert parse_bql_string('create temporary table tx as'
            ' infer explicit x, predict x as xi confidence xc from t_cc') == \
        [ast.CreateTabAs(True, False, 'tx',
            ast.InferExplicit(
                [
                    ast.SelColExp(ast.ExpCol(None, 'x'), None),
                    ast.PredCol('x', 'xi', 'xc', None),
                ],
                't_cc', None, None, None, None, None, None,
            ))]

def test_analyze():
    assert parse_bql_string('analyze t for 1 iteration;') == \
        [ast.AnalyzeModels('t', None, 1, None, None, None, None)]
    assert parse_bql_string('analyze t for 7 seconds or 1 iteration;') == \
        [ast.AnalyzeModels('t', None, 1, 7, None, None, None)]
    assert parse_bql_string('analyze t for 1 iteration;') == \
        [ast.AnalyzeModels('t', None, 1, None, None, None, None)]
    assert parse_bql_string('analyze t for 1 minute;') == \
        [ast.AnalyzeModels('t', None, None, 60, None, None, None)]
    assert parse_bql_string('analyze t for 1 minute;') == \
        [ast.AnalyzeModels('t', None, None, 60, None, None, None)]
    assert parse_bql_string('analyze t for 2 minutes;') == \
        [ast.AnalyzeModels('t', None, None, 120, None, None, None)]
    assert parse_bql_string('analyze t for 100 iterations or 2 minutes;') == \
        [ast.AnalyzeModels('t', None, 100, 120, None, None, None)]
    assert parse_bql_string('analyze t for 2 minutes;') == \
        [ast.AnalyzeModels('t', None, None, 120, None, None, None)]
    assert parse_bql_string('analyze t for 1 second;') == \
        [ast.AnalyzeModels('t', None, None, 1, None, None, None)]
    assert parse_bql_string('analyze t for 1 second;') == \
        [ast.AnalyzeModels('t', None, None, 1, None, None, None)]
    assert parse_bql_string('analyze t for 2 seconds;') == \
        [ast.AnalyzeModels('t', None, None, 2, None, None, None)]
    assert parse_bql_string('analyze t for 2 seconds;') == \
        [ast.AnalyzeModels('t', None, None, 2, None, None, None)]
    assert parse_bql_string('analyze t model 1 for 1 iteration;') == \
        [ast.AnalyzeModels('t', [1], 1, None, None, None, None)]
    assert parse_bql_string('analyze t models 1,2,3 for 1 iteration;') == \
        [ast.AnalyzeModels('t', [1,2,3], 1, None, None, None, None)]
    assert parse_bql_string('analyze t models 1-3,5 for 1 iteration;') == \
        [ast.AnalyzeModels('t', [1,2,3,5], 1, None, None, None, None)]
    assert parse_bql_string('analyze t for 10 iterations'
            ' checkpoint 3 iterations') == \
        [ast.AnalyzeModels('t', None, 10, None, 3, None, None)]
    assert parse_bql_string('analyze t for 10 iterations'
            ' (mh(default, one, 10))') == \
        [ast.AnalyzeModels('t', None, 10, None, None, None, [
            'mh', '(', 'default', ',', 'one', ',', 10, ')'
        ])]
    assert parse_bql_string('analyze t for 10 seconds'
            ' checkpoint 3 seconds') == \
        [ast.AnalyzeModels('t', None, None, 10, None, 3, None)]
    assert parse_bql_string('analyze t for 1 minute or 10 minutes'
            ' checkpoint 3 seconds') == \
        [ast.AnalyzeModels('t', None, None, 60, None, 3, None)]
    assert parse_bql_string('analyze t for 100 iterations or 10 iterations'
            ' checkpoint 3 seconds') == \
        [ast.AnalyzeModels('t', None, 10, None, None, 3, None)]

def test_altergen():
    assert parse_bql_string('alter generator g '
            'rename to rumba') == \
        [ast.AlterGen(
            generator='g',
            modelnos=None,
            commands=[ast.AlterGenRenameGen('rumba')]
        )]
    assert parse_bql_string('alter generator g models (1, 2, 4) '
            'rename to rumba') == \
        [ast.AlterGen(
            generator='g',
            modelnos=[1,2,4],
            commands=[ast.AlterGenRenameGen('rumba')]
        )]
    assert parse_bql_string('alter generator g '
            'rename to rumba, generic cmd (a,c), generic cmd2') == \
        [ast.AlterGen(
            generator='g',
            modelnos=None,
            commands=[
                ast.AlterGenRenameGen('rumba'),
                ast.AlterGenGeneric(['generic', 'cmd', ['a', ',', 'c']]),
                ast.AlterGenGeneric(['generic', 'cmd2']),
            ],
        )]
    assert parse_bql_string('alter generator g models (1-4) '
            'set variable clustering of (*) to independent, '
            'set variable clustering of (*) to dependent, '
            'set variable cluster of bar to cluster of baz, '
            'set variable cluster of (roger, martin) to singleton, '
            'set variable clustering hyperparameter to 100, '
            'set row cluster of 1 to cluster of 2, '
            'set row clustering hyperparameter of cluster of variable K to 11, '
            'set row clustering hyperparameter of variable K to -1 '
        ) == \
        [ast.AlterGen(
            generator='g',
            modelnos=[1, 2, 3, 4],
            commands=[
                ast.AlterGenGeneric([
                    'set', 'variable', 'clustering', 'of', ['*'],
                    'to', 'independent']),
                ast.AlterGenGeneric([
                    'set', 'variable', 'clustering', 'of', ['*'],
                    'to', 'dependent']),
                ast.AlterGenGeneric([
                    'set', 'variable', 'cluster', 'of', 'bar', 'to',
                    'cluster', 'of', 'baz']),
                ast.AlterGenGeneric([
                    'set', 'variable', 'cluster', 'of',
                    ['roger', ',', 'martin'], 'to', 'singleton']),
                ast.AlterGenGeneric([
                    'set', 'variable', 'clustering', 'hyperparameter',
                    'to', 100]),
                ast.AlterGenGeneric(
                    ['set', 'row', 'cluster', 'of', 1, 'to',
                    'cluster', 'of', 2]),
                ast.AlterGenGeneric([
                    'set', 'row', 'clustering', 'hyperparameter', 'of',
                    'cluster', 'of', 'variable', 'K', 'to', 11]),
                ast.AlterGenGeneric([
                    'set', 'row', 'clustering', 'hyperparameter', 'of',
                    'variable', 'K', 'to', '-', 1])
        ])]

def test_create_tab_csv():
    assert parse_bql_string('create temp table if not exists f '
            'from \'foo.csv\'') == \
        [ast.CreateTabCsv(True, True, 'f', 'foo.csv')]
    assert parse_bql_string('create table if not exists f '
            'from \'foo.csv\'') == \
        [ast.CreateTabCsv(False, True, 'f', 'foo.csv')]
    assert parse_bql_string('create table f '
            'from \'foo.csv\'') == \
        [ast.CreateTabCsv(False, False, 'f', 'foo.csv')]
    # Using double quotes for csv pathname should raise.
    with pytest.raises(parse.BQLParseError):
        parse_bql_string('create temp table if not exists f '
            'from "foo.csv"')

def test_alterpop_stattype():
    assert parse_bql_string('alter population p '
            'set stattype of a to normal') == \
        [ast.AlterPop('p',
            [ast.AlterPopStatType(['a'], 'normal')]
        )]
    assert parse_bql_string('alter population g '
            'set stattypes of a, b to BETA') == \
        [ast.AlterPop('g', [ast.AlterPopStatType(['a', 'b'], 'BETA')])]
    assert parse_bql_string('alter population p '
            'set stattypes of a, b to beta, '
            'set stattype of c to nominal') == \
        [ast.AlterPop('p', [
            ast.AlterPopStatType(['a', 'b'], 'beta'),
            ast.AlterPopStatType(['c'], 'nominal'),]
        )]

def test_alterpop_addvar():
    assert parse_bql_string('alter population p '
            'add variable r') == \
        [ast.AlterPop('p',
            [ast.AlterPopAddVar('r', None)]
        )]
    assert parse_bql_string('alter population g '
            'add variable r nominal') == \
        [ast.AlterPop('g',
            [ast.AlterPopAddVar('r', 'nominal')]
        )]
    assert parse_bql_string('alter population p '
            'add variable a, '
            'set stattype of a to nominal, '
            'add variable b numerical') == \
        [ast.AlterPop('p', [
            ast.AlterPopAddVar('a', None),
            ast.AlterPopStatType(['a'], 'nominal'),
            ast.AlterPopAddVar('b', 'numerical'),]
        )]
    with pytest.raises(bayeslite.BQLParseError):
        parse_bql_string('alter population v add variable;')

def test_alterpop_renamepop():
    assert parse_bql_string('alter population p '
            'rename to z') == \
        [ast.AlterPop('p',
            [ast.AlterPopRenamePop('z')]
        )]

def test_infer_trivial():
    assert parse_bql_string('infer x from p') == \
        [ast.InferAuto([ast.InfColOne('x', None)], ast.ExpLit(ast.LitInt(0)),
            None, 'p', None, None, None, None, None, None)]

def test_infer_conf():
    assert parse_bql_string('infer x with confidence 0.9 from p') == \
        [ast.InferAuto([ast.InfColOne('x', None)],
            ast.ExpLit(ast.LitFloat(0.9)), None, 'p', None, None, None, None,
            None, None)]

def test_infer_samples():
    assert parse_bql_string('infer x using 42 samples from p') == \
        [ast.InferAuto([ast.InfColOne('x', None)],
            ast.ExpLit(ast.LitInt(0)), ast.ExpLit(ast.LitInt(42)), 'p',
            None, None, None, None, None, None)]

def test_infer_conf_samples():
    assert parse_bql_string('infer x with confidence 0.9 using 42 samples'
            ' from p') == \
        [ast.InferAuto([ast.InfColOne('x', None)],
            ast.ExpLit(ast.LitInt(.9)), ast.ExpLit(ast.LitInt(42)), 'p', None,
            None, None, None, None, None)]

def test_infer_explicit():
    assert parse_bql_string('infer explicit x, predict y with confidence 0.9,'
            ' predict p with confidence 0.8 as q, predict u confidence v,'
            ' predict a as b confidence c,'
            ' predict h confidence k using 42 samples'
            ' from p') == \
        [ast.InferExplicit([
                ast.SelColExp(ast.ExpCol(None, 'x'), None),
                ast.SelColExp(
                    ast.ExpBQLPredict('y', ast.ExpLit(ast.LitFloat(.9)), None),
                    None,
                ),
                ast.SelColExp(
                    ast.ExpBQLPredict('p', ast.ExpLit(ast.LitFloat(.8)), None),
                    'q',
                ),
                ast.PredCol('u', None, 'v', None),
                ast.PredCol('a', 'b', 'c', None),
                ast.PredCol('h', None, 'k', ast.ExpLit(ast.LitInt(42))),
            ],
            'p', None, None, None, None, None, None)]

def test_infer_explicit_samples():
    assert parse_bql_string('infer explicit x, predict y with confidence 0.9,'
            ' predict p with confidence 0.8 as q, predict u confidence v,'
            ' predict a as b confidence c,'
            ' predict h confidence k using 42 samples'
            ' from p') == \
        [ast.InferExplicit([
                ast.SelColExp(ast.ExpCol(None, 'x'), None),
                ast.SelColExp(
                    ast.ExpBQLPredict('y', ast.ExpLit(ast.LitFloat(.9)), None),
                    None,
                ),
                ast.SelColExp(
                    ast.ExpBQLPredict('p', ast.ExpLit(ast.LitFloat(.8)), None),
                    'q',
                ),
                ast.PredCol('u', None, 'v', None),
                ast.PredCol('a', 'b', 'c', None),
                ast.PredCol('h', None, 'k', ast.ExpLit(ast.LitInt(42))),
            ],
            'p', None, None, None, None, None, None)]

def test_parametrized():
    assert parse_bql_string('select * from t where id = ?;') == \
        [ast.Parametrized(ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
                [ast.SelTab('t', None)],
                ast.ExpOp(ast.OP_EQ, (
                    ast.ExpCol(None, 'id'),
                    ast.ExpNumpar(1),
                )),
                None, None, None),
            1, {})]
    assert parse_bql_string('select * from t where id = ?123;') == \
        [ast.Parametrized(ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
                [ast.SelTab('t', None)],
                ast.ExpOp(ast.OP_EQ, (
                    ast.ExpCol(None, 'id'),
                    ast.ExpNumpar(123),
                )),
                None, None, None),
            123, {})]
    assert parse_bql_string('select * from t where id = :foo;') == \
        [ast.Parametrized(ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
                [ast.SelTab('t', None)],
                ast.ExpOp(ast.OP_EQ, (
                    ast.ExpCol(None, 'id'),
                    ast.ExpNampar(1, ':foo'),
                )),
                None, None, None),
            1, {':foo': 1})]
    assert parse_bql_string('select * from t where a = :foo and b = @foo;') \
        == \
        [ast.Parametrized(ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
                [ast.SelTab('t', None)],
                ast.ExpOp(ast.OP_BOOLAND, (
                    ast.ExpOp(ast.OP_EQ, (
                        ast.ExpCol(None, 'a'),
                        ast.ExpNampar(1, ':foo'),
                    )),
                    ast.ExpOp(ast.OP_EQ, (
                        ast.ExpCol(None, 'b'),
                        ast.ExpNampar(2, '@foo'),
                    )),
                )),
                None, None, None),
            2, {':foo': 1, '@foo': 2})]
    assert parse_bql_string('select * from t where a = $foo and b = ?1;') == \
        [ast.Parametrized(ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
                [ast.SelTab('t', None)],
                ast.ExpOp(ast.OP_BOOLAND, (
                    ast.ExpOp(ast.OP_EQ, (
                        ast.ExpCol(None, 'a'),
                        ast.ExpNampar(1, '$foo'),
                    )),
                    ast.ExpOp(ast.OP_EQ, (
                        ast.ExpCol(None, 'b'),
                        ast.ExpNumpar(1),
                    )),
                )),
                None, None, None),
            1, {'$foo': 1})]
    assert parse_bql_string('select * from t' +
            ' where a = ?123 and b = :foo and c = ?124;') == \
        [ast.Parametrized(ast.Select(ast.SELQUANT_ALL, [ast.SelColAll(None)],
                [ast.SelTab('t', None)],
                ast.ExpOp(ast.OP_BOOLAND, (
                    ast.ExpOp(ast.OP_BOOLAND, (
                        ast.ExpOp(ast.OP_EQ, (
                            ast.ExpCol(None, 'a'),
                            ast.ExpNumpar(123),
                        )),
                        ast.ExpOp(ast.OP_EQ, (
                            ast.ExpCol(None, 'b'),
                            ast.ExpNampar(124, ':foo'),
                        )),
                    )),
                    ast.ExpOp(ast.OP_EQ, (
                        ast.ExpCol(None, 'c'),
                        ast.ExpNumpar(124),
                    )),
                )),
                None, None, None),
            124, {':foo': 124})]

def test_complete():
    assert parse.bql_string_complete_p('')
    assert parse.bql_string_complete_p(';')
    assert parse.bql_string_complete_p(';;;')
    assert parse.bql_string_complete_p('\n;\n;;;\n;\n')
    assert not parse.bql_string_complete_p('select 0')
    assert parse.bql_string_complete_p('select 0;')
    assert not parse.bql_string_complete_p('select 0\nfrom t')
    assert parse.bql_string_complete_p('select 0\nfrom t;')
    assert not parse.bql_string_complete_p('select 0;select 1')
    assert parse.bql_string_complete_p('select 0;select 1;')
    assert not parse.bql_string_complete_p('select 0;\nselect 1')
    assert parse.bql_string_complete_p('select 0;\nselect 1;')

def test_simulate():
    with pytest.raises(parse.BQLParseError):
        # Need limit.
        parse_bql_string('create table s as simulate x from t')
    with pytest.raises(parse.BQLParseError):
        # Need limit.
        parse_bql_string('create table s as simulate x from t given y = 0')
    assert parse_bql_string('create table s as'
            ' simulate x from t limit 10') == \
        [ast.CreateTabAs(False, False, 's',
            ast.Simulate(
                [ast.SelColExp(ast.ExpCol(None, 'x'), None)],
                't',
                None,
                None,
                [],
                ast.ExpLit(ast.LitInt(10)),
                None)
        )]
    assert parse_bql_string('create table if not exists s as'
            ' simulate x, y from t given z = 0 limit 10 accuracy 2') == \
        [ast.CreateTabAs(False, True, 's',
            ast.Simulate(
                [
                    ast.SelColExp(ast.ExpCol(None, 'x'), None),
                    ast.SelColExp(ast.ExpCol(None, 'y'), None),
                ],
                't',
                None,
                None,
                [('z', ast.ExpLit(ast.LitInt(0)))],
                ast.ExpLit(ast.LitInt(10)),
                2)
        )]
    assert parse_bql_string('create temp table s as'
            ' simulate x, y from t given z = 0 limit 10') == \
        [ast.CreateTabAs(True, False, 's',
            ast.Simulate(
                [
                    ast.SelColExp(ast.ExpCol(None, 'x'), None),
                    ast.SelColExp(ast.ExpCol(None, 'y'), None),
                ],
                't',
                None,
                None,
                [('z', ast.ExpLit(ast.LitInt(0)))],
                ast.ExpLit(ast.LitInt(10)),
                None),
        )]
    assert parse_bql_string(
            'create temp table if not exists s as'
            ' simulate x, y from t given z = 0, w = 1'
            ' limit 10 accuracy 19') == \
        [ast.CreateTabAs(True, True, 's',
            ast.Simulate(
                [
                    ast.SelColExp(ast.ExpCol(None, 'x'), None),
                    ast.SelColExp(ast.ExpCol(None, 'y'), None),
                ],
                't',
                None,
                None,
                [
                    ('z', ast.ExpLit(ast.LitInt(0))),
                    ('w', ast.ExpLit(ast.LitInt(1))),
                ],
                ast.ExpLit(ast.LitInt(10)),
                19)
        )]
    # Specifying a quantity other than a variable should raise.
    with pytest.raises(parse.BQLParseError):
        parse_bql_string(
            'simulate a, dependence probability of a with b from t limit 10;')
    with pytest.raises(parse.BQLParseError):
        parse_bql_string(
            'create table f as simulate a, dependence probability of a with b '
            'from t limit 10;')

def test_simulate_models():
    assert parse_bql_string(
        'simulate dependence probability of a with b from models of t;') == [
            ast.SimulateModels(
                [
                    ast.SelColExp(ast.ExpBQLDepProb('a', 'b'), None),
                ],
                't', None
            )
    ]
    assert parse_bql_string(
        'simulate dependence probability of a with b AS q, '
        'mutual information of c with d given (e, r=2.7) '
            'using 100 samples as g '
        'from models of p modeled by z') == [
            ast.SimulateModels(
                [
                    ast.SelColExp(
                        ast.ExpBQLDepProb('a', 'b'), 'q'),
                    ast.SelColExp(
                        ast.ExpBQLMutInf(
                            ['c'],
                            ['d'],
                            [
                                ('e', ast.ExpLit(ast.LitNull(0))),
                                ('r', ast.ExpLit(ast.LitFloat(2.7)))
                            ],
                            ast.ExpLit(ast.LitInt(100))),
                        'g'
                    ),
                ],
                'p', 'z'
            )
    ]
    assert parse_bql_string(
        'simulate dependence probability of a with b AS q, '
        'mutual information of (c) with (d, r) given (e, r=2.7) '
            'using 100 samples as g '
        'from models of p modeled by z') == [
            ast.SimulateModels(
                [
                    ast.SelColExp(
                        ast.ExpBQLDepProb('a', 'b'), 'q'),
                    ast.SelColExp(
                        ast.ExpBQLMutInf(
                            ['c'],
                            ['d', 'r'],
                            [
                                ('e', ast.ExpLit(ast.LitNull(0))),
                                ('r', ast.ExpLit(ast.LitFloat(2.7)))
                            ],
                            ast.ExpLit(ast.LitInt(100))),
                        'g'
                    ),
                ],
                'p', 'z'
            )
    ]
    assert parse_bql_string(
        'simulate probability density of (a=2, c=1.1) given (b=0.5) '
        'from models of p') == [
            ast.SimulateModels(
                [
                    ast.SelColExp(
                        ast.ExpBQLProbDensity(
                            [
                                ('a', ast.ExpLit(ast.LitInt(2))),
                                ('c', ast.ExpLit(ast.LitFloat(1.1)))
                            ],
                            [('b', ast.ExpLit(ast.LitFloat(0.5)))]),
                        None
                    ),
                ],
                'p', None
            )
    ]
    for temp, ifnotexists in itertools.product(
            ('temp', ''), ('if not exists', '')):
        assert parse_bql_string(
            'create %s table %s f as '
            'simulate dependence probability of a with b AS q, '
            'mutual information of "bad-col" with d '
                'given (e, r=2.7) using 100 samples as g '
            'from models of p modeled by z' % (temp, ifnotexists)) == [
                ast.CreateTabAs(
                    bool(temp),
                    bool(ifnotexists),
                    'f',
                    ast.SimulateModels(
                        [
                            ast.SelColExp(
                                ast.ExpBQLDepProb('a', 'b'),
                                'q'
                            ),
                            ast.SelColExp(
                                ast.ExpBQLMutInf(
                                    ['bad-col'],
                                    ['d'],
                                    [
                                        ('e', ast.ExpLit(ast.LitNull(0))),
                                        ('r', ast.ExpLit(ast.LitFloat(2.7)))
                                    ],
                                    ast.ExpLit(ast.LitInt(100))),
                                'g'
                            ),
                        ],
                        'p', 'z'
                    )
            )
        ]

def test_is_bql():
    assert ast.is_bql(ast.ExpLit(ast.LitInt(0))) == False
    assert ast.is_bql(ast.ExpNumpar(0)) == False
    assert ast.is_bql(ast.ExpNampar(0, 'x')) == False
    assert ast.is_bql(ast.ExpCol('t', 'c')) == False
    # ...
    assert ast.is_bql(ast.ExpBQLPredProb([ast.ColListLit(['c'])], []))
    assert ast.is_bql(
        ast.ExpBQLProbDensity([('c', ast.ExpLit(ast.LitInt(0)))], []))
    assert ast.is_bql(ast.ExpBQLProbDensityFn(ast.ExpLit(ast.LitInt(0)), []))
    assert ast.is_bql(ast.ExpBQLSim(None, ast.ExpLit(ast.LitInt(0)), []))
    assert ast.is_bql(ast.ExpBQLDepProb('c0', 'c1'))
    assert ast.is_bql(ast.ExpBQLMutInf('c0', 'c1', None, 100))
    assert ast.is_bql(ast.ExpBQLCorrel('c0', 'c1'))
    assert ast.is_bql(ast.ExpBQLPredict('c', ast.ExpLit(ast.LitInt(.5)), None))
    assert ast.is_bql(ast.ExpBQLPredictConf('c', None))

def test_regress():
    assert parse_bql_string('''
        regress t given (y) using 10 samples by pop;
    ''') == [
        ast.Regress(
            target='t',
            givens=[
                ast.SelColExp(
                    expression=ast.ExpCol(table=None, column='y'), name=None),],
            nsamp=ast.ExpLit(value=ast.LitInt(value=10)),
            population='pop',
            generator=None,
            modelnos=None,
    )]
    assert parse_bql_string('''
        regress t given (y, x) using 10 samples by pop modeled by m
        using models 1, 7;
        ''') == [
        ast.Regress(
            target='t',
            givens=[
                ast.SelColExp(
                    expression=ast.ExpCol(table=None, column='y'), name=None),
                ast.SelColExp(
                    expression=ast.ExpCol(table=None, column='x'), name=None)],
            nsamp=ast.ExpLit(value=ast.LitInt(value=10)),
            population='pop',
            generator='m',
            modelnos=[1,7],
    )]
    assert parse_bql_string('regress t given (*) by pop;') == [
        ast.Regress(
            target='t',
            givens=[ast.SelColAll(None)],
            nsamp=None,
            population='pop',
            generator=None,
            modelnos=None,
    )]
    # Disallow this query in the compiler, mixing * with u.
    assert parse_bql_string('''
        regress t given (*, u) using 10 samples by pop modeled by m
        using models 1-3;
    ''') == [
        ast.Regress(
            target='t',
            givens=[
                ast.SelColAll(None),
                ast.SelColExp(
                    expression=ast.ExpCol(table=None, column='u'), name=None)],
            nsamp=ast.ExpLit(value=ast.LitInt(value=10)),
            population='pop',
            generator='m',
            modelnos=[1,2,3],
    )]
    # Disallow this query in the compiler, mixing subquery.
    assert parse_bql_string('''
        regress t given (
            pop.(estimate * from variables of pop limit 2),
            a, y)
        using 10 samples by pop modeled by f;
    ''') == [
        ast.Regress(
            target='t',
            givens=[
                ast.SelColSub(table='pop',
                    query=ast.EstCols(
                        columns=[ast.SelColAll(table=None)],
                        population='pop',
                        generator=None,
                        modelnos=None,
                        condition=None,
                        order=None,
                        limit=ast.Lim(
                            limit=ast.ExpLit(value=ast.LitInt(value=2)),
                            offset=None
                ))),
                ast.SelColExp(
                    expression=ast.ExpCol(table=None, column='a'), name=None),
                ast.SelColExp(
                    expression=ast.ExpCol(table=None, column='y'), name=None)
            ],
            nsamp=ast.ExpLit(value=ast.LitInt(value=10)),
            population='pop',
            generator='f',
            modelnos=None,
    )]

    with pytest.raises(bayeslite.BQLParseError):
        # Missing given.
        assert parse_bql_string('regress t using 10 samples by pop;')
    with pytest.raises(bayeslite.BQLParseError):
        # Missing parenthesis.
        assert parse_bql_string('regress t given * using 10 samples by pop;')

def test_using_model():
    assert parse_bql_string('simulate x from t using model 42'
            ' limit 10') == [
        ast.Simulate(
            columns=[ast.SelColExp(ast.ExpCol(None, 'x'), None)],
            population='t',
            generator=None,
            modelnos=[42],
            constraints=[],
            nsamples=ast.ExpLit(ast.LitInt(10)),
            accuracy=None)
    ]
    with pytest.raises(parse.BQLParseError):
        assert parse_bql_string('simulate x from t'
                ' using model (87)') == [
            ast.Simulate(
                columns=[ast.SelColExp(ast.ExpCol(None, 'x'), None)],
                population='t',
                generator=None,
                modelnos=[87],
                constraints=[],
                nsamples=ast.ExpLit(ast.LitInt(10)),
                accuracy=None)
        ]
    assert parse_bql_string('estimate x from t modeled by g '
            'using models 1, 2') == [
        ast.Estimate(
            quantifier=ast.SELQUANT_ALL,
            columns=[ast.SelColExp(ast.ExpCol(None, 'x'), None)],
            population='t',
            generator='g',
            modelnos=[1,2],
            condition=None,
            grouping=None,
            order=None,
            limit=None)
    ]
    assert parse_bql_string('estimate * from columns of t modeled by z'
            ' using models 1-3, 5, 12-14') == [
        ast.EstCols(
            columns=[ast.SelColAll(None)],
            population='t',
            generator='z',
            modelnos=[1,2,3,5,12,13,14],
            condition=None,
            order=None,
            limit=None)
    ]
    assert parse_bql_string('estimate 42 from pairwise columns of t'
            ' using models 0, 7') == [
        ast.EstPairCols(
            columns=[(ast.ExpLit(ast.LitInt(42)), None)],
            population='t',
            subcolumns=None,
            generator=None,
            modelnos=[0,7],
            condition=None,
            order=None,
            limit=None)
    ]
    assert parse_bql_string('estimate similarity in the context of h '
            'from pairwise t modeled by g using models 8-10') == [
        ast.EstPairRow(
            columns=[ast.SelColExp(
                ast.ExpBQLSim(
                    None, None, [ast.ColListLit(columns=['h'])]), name=None)],
            population='t',
            generator='g',
            modelnos=[8, 9, 10],
            condition=None,
            order=None,
            limit=None)
    ]
    assert parse_bql_string('infer x from t using models 7') == [
        ast.InferAuto(
            columns=[ast.InfColOne('x', None)],
            confidence=ast.ExpLit(ast.LitInt(0)),
            nsamples=None,
            population='t',
            generator=None,
            modelnos=[7],
            condition=None,
            grouping=None,
            order=None,
            limit=None)
    ]
    assert parse_bql_string('infer explicit x from t using models 7, 14') == [
        ast.InferExplicit(
            columns=[ast.SelColExp(ast.ExpCol(None, 'x'), None)],
            population='t',
            generator=None,
            modelnos=[7,14],
            condition=None,
            grouping=None,
            order=None,
            limit=None)
    ]

@contextlib.contextmanager
def raises_str(klass, string):
    with pytest.raises(klass):
        try:
            yield
        except klass as e:
            assert string in str(e)
            raise

def test_estimate_pairwise_deprecation():
    with raises_str(bayeslite.BQLParseError, "deprecated `ESTIMATE COLUMNS'"):
        parse_bql_string('estimate columns from t')
    with raises_str(bayeslite.BQLParseError, "deprecated `ESTIMATE PAIRWISE'"):
        parse_bql_string('estimate pairwise dependence probability from t')
    with raises_str(bayeslite.BQLParseError,
            "deprecated `ESTIMATE PAIRWISE ROW'"):
        parse_bql_string('estimate pairwise row similarity from t')

def test_parse_error_with_context():
    with raises_str(bayeslite.BQLParseError, "select ( 1 +"):
        parse_bql_string('select (1 +')
    with raises_str(bayeslite.BQLParseError,
                    "Syntax error near [] after [select]"):
        parse_bql_string('select')
