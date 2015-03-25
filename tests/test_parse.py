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

import pytest

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
            [ast.SelColExp(ast.ExpBQLPredProb('c'), None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select predictive probability of c, * from t;') \
        == \
        [ast.Select(ast.SELQUANT_ALL,
            [
                ast.SelColExp(ast.ExpBQLPredProb('c'), None),
                ast.SelColAll(None),
            ],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select c, predictive probability of d from t;') \
        == \
        [ast.Select(ast.SELQUANT_ALL,
            [
                ast.SelColExp(ast.ExpCol(None, 'c'), None),
                ast.SelColExp(ast.ExpBQLPredProb('d'), None),
            ],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select predictive probability of c, d from t;') \
        == \
        [ast.Select(ast.SELQUANT_ALL,
            [
                ast.SelColExp(ast.ExpBQLPredProb('c'), None),
                ast.SelColExp(ast.ExpCol(None, 'd'), None),
            ],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select probability of c = 42 from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLProb('c', ast.ExpLit(ast.LitInt(42))),
                None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select typicality from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLTyp(None), None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select typicality of c from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLTyp('c'), None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select similarity from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLSim(None, [ast.ColListAll()]), None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select similarity to (rowid=8) from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpBQLSim(
                    ast.ExpOp(ast.OP_EQ, (
                        ast.ExpCol(None, 'rowid'),
                        ast.ExpLit(ast.LitInt(8))
                    )),
                    [ast.ColListAll()]),
                None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select similarity with respect to c from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLSim(None, [ast.ColListLit(['c'])]),
                None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string(
            'select similarity to (rowid=8) with respect to c from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpBQLSim(
                    ast.ExpOp(ast.OP_EQ, (
                        ast.ExpCol(None, 'rowid'),
                        ast.ExpLit(ast.LitInt(8)),
                    )),
                    [ast.ColListLit(['c'])]),
                None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string(
            'select similarity to (rowid=5) with respect to age from t1;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpBQLSim(
                    ast.ExpOp(ast.OP_EQ, (
                        ast.ExpCol(None, 'rowid'),
                        ast.ExpLit(ast.LitInt(5)),
                    )),
                    [ast.ColListLit(['age'])]),
                None)],
            [ast.SelTab('t1', None)], None, None, None, None)]
    assert parse_bql_string(
            'select similarity to (rowid=8) with respect to c, d from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [
                ast.SelColExp(
                    ast.ExpBQLSim(
                        ast.ExpOp(ast.OP_EQ, (
                            ast.ExpCol(None, 'rowid'),
                            ast.ExpLit(ast.LitInt(8)),
                        )),
                        [ast.ColListLit(['c'])]),
                    None),
                ast.SelColExp(ast.ExpCol(None, 'd'), None),
            ],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select similarity to (rowid=8)'
            ' with respect to (c, d) from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpBQLSim(
                    ast.ExpOp(ast.OP_EQ, (
                        ast.ExpCol(None, 'rowid'),
                        ast.ExpLit(ast.LitInt(8)),
                    )),
                    [ast.ColListLit(['c']), ast.ColListLit(['d'])]),
                None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select similarity to (rowid=8) with respect to' +
            ' (estimate columns from t order by typicality limit 1)' +
            ' from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(
                ast.ExpBQLSim(
                    ast.ExpOp(ast.OP_EQ, (
                        ast.ExpCol(None, 'rowid'),
                        ast.ExpLit(ast.LitInt(8)),
                    )),
                    [ast.ColListSub(
                        ast.EstCols([], 't', None,
                            [ast.Ord(ast.ExpBQLTyp(None), ast.ORD_ASC)],
                            ast.Lim(ast.ExpLit(ast.LitInt(1)), None),
                            None)
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
            [ast.SelColExp(ast.ExpBQLMutInf('c', None, None), None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string(
            'select mutual information of c with d from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLMutInf('c', 'd', None), None)],
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select mutual information of c with d' +
            ' using (1+2) samples from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLMutInf('c', 'd',
                    ast.op(ast.OP_ADD, ast.ExpLit(ast.LitInt(1)),
                        ast.ExpLit(ast.LitInt(2)))),
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
    # introduced ParseError, this simply caught Exception -- which
    # covered the AssertionError that this turned into.
    #
    # with pytest.raises(parse.ParseError):
    #     parse_bql_string('select probability of x = 1 -' +
    #         ' probability of y = 0 from t;')
    #     # XXX Should really be this test, but getting the grammar to
    #     # admit this unambiguously is too much of a pain at the
    #     # moment.
    #     assert parse_bql_string('select probability of x = 1 -' +
    #             ' probability of y = 0 from t;') == \
    #         [ast.Select(ast.SELQUANT_ALL,
    #             [ast.SelColExp(ast.ExpBQLProb('x',
    #                     ast.ExpOp(ast.OP_SUB, (
    #                         ast.ExpLit(ast.LitInt(1)),
    #                         ast.ExpBQLProb('y', ast.ExpLit(ast.LitInt(0))),
    #                     ))),
    #                 None)],
    #             [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select probability of c1 = f(c2) from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            [ast.SelColExp(ast.ExpBQLProb('c1',
                    ast.ExpApp(False, 'f', [ast.ExpCol(None, 'c2')])),
                None)],
            [ast.SelTab('t', None)], None, None, None, None)]

def test_trivial_scan_error():
    with pytest.raises(parse.ParseError):
        parse_bql_string('select 0c;')
    with pytest.raises(parse.ParseError):
        parse_bql_string('select 1.0p1;')

def test_trivial_precedence_error():
    with pytest.raises(parse.ParseError):
        parse_bql_string('select similarity to similarity to 0' +
            ' with respect to c from t;')

def test_trivial_commands():
    assert parse_bql_string('create generator t_cc for t using crosscat'
            '(xyz numerical, pqr categorical, lmn cyclic)') == \
        [ast.CreateGen('t_cc', False, 't', 'crosscat',
            ast.GenSchema([
                ast.GenColumn('xyz', 'numerical'),
                ast.GenColumn('pqr', 'categorical'),
                ast.GenColumn('lmn', 'cyclic'),
            ]))]
    assert parse_bql_string('create generator t_cc if not exists'
            ' for t using crosscat'
            '(xyz numerical, pqr categorical, lmn cyclic)') == \
        [ast.CreateGen('t_cc', True, 't', 'crosscat',
            ast.GenSchema([
                ast.GenColumn('xyz', 'numerical'),
                ast.GenColumn('pqr', 'categorical'),
                ast.GenColumn('lmn', 'cyclic'),
            ]))]
    assert parse_bql_string('initialize 1 model for t;') == \
        [ast.InitModels(False, 't', 1, None)]
    assert parse_bql_string('initialize 1 model if not exists for t;') == \
        [ast.InitModels(True, 't', 1, None)]
    assert parse_bql_string('initialize 2 models for t;') == \
        [ast.InitModels(False, 't', 2, None)]
    assert parse_bql_string('initialize 2 models if not exists for t;') == \
        [ast.InitModels(True, 't', 2, None)]
    assert parse_bql_string('analyze t for 1 iteration;') == \
        [ast.AnalyzeModels('t', None, 1, None, False)]
    assert parse_bql_string('analyze t for 1 iteration wait;') == \
        [ast.AnalyzeModels('t', None, 1, None, True)]
    assert parse_bql_string('analyze t for 1 minute;') == \
        [ast.AnalyzeModels('t', None, None, 60, False)]
    assert parse_bql_string('analyze t for 1 minute wait;') == \
        [ast.AnalyzeModels('t', None, None, 60, True)]
    assert parse_bql_string('analyze t for 2 minutes;') == \
        [ast.AnalyzeModels('t', None, None, 120, False)]
    assert parse_bql_string('analyze t for 2 minutes wait;') == \
        [ast.AnalyzeModels('t', None, None, 120, True)]
    assert parse_bql_string('analyze t for 1 second;') == \
        [ast.AnalyzeModels('t', None, None, 1, False)]
    assert parse_bql_string('analyze t for 1 second wait;') == \
        [ast.AnalyzeModels('t', None, None, 1, True)]
    assert parse_bql_string('analyze t for 2 seconds;') == \
        [ast.AnalyzeModels('t', None, None, 2, False)]
    assert parse_bql_string('analyze t for 2 seconds wait;') == \
        [ast.AnalyzeModels('t', None, None, 2, True)]
    assert parse_bql_string('analyze t model 1 for 1 iteration;') == \
        [ast.AnalyzeModels('t', [1], 1, None, False)]
    assert parse_bql_string('analyze t models 1,2,3 for 1 iteration;') == \
        [ast.AnalyzeModels('t', [1,2,3], 1, None, False)]
    assert parse_bql_string('analyze t models 1-3,5 for 1 iteration;') == \
        [ast.AnalyzeModels('t', [1,2,3,5], 1, None, False)]

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
    with pytest.raises(parse.ParseError):
        # Need limit.
        parse_bql_string('create table s as simulate x from t')
    with pytest.raises(parse.ParseError):
        # Need limit.
        parse_bql_string('create table s as simulate x from t given y = 0')
    assert parse_bql_string('create table s as'
            ' simulate x from t limit 10') == \
        [ast.CreateTableSim(False, False, 's',
            ast.Simulate(['x'], 't', [], ast.ExpLit(ast.LitInt(10))))]
    assert parse_bql_string('create table if not exists s as'
            ' simulate x, y from t given z = 0 limit 10') == \
        [ast.CreateTableSim(False, True, 's',
            ast.Simulate(['x', 'y'], 't', [('z', ast.ExpLit(ast.LitInt(0)))],
                ast.ExpLit(ast.LitInt(10))))]
    assert parse_bql_string('create temp table s as'
            ' simulate x, y from t given z = 0 limit 10') == \
        [ast.CreateTableSim(True, False, 's',
            ast.Simulate(['x', 'y'], 't', [('z', ast.ExpLit(ast.LitInt(0)))],
                ast.ExpLit(ast.LitInt(10))))]
    assert parse_bql_string('create temp table if not exists s as'
            ' simulate x, y from t given z = 0, w = 1 limit 10') == \
        [ast.CreateTableSim(True, True, 's',
            ast.Simulate(['x', 'y'], 't',
                [
                    ('z', ast.ExpLit(ast.LitInt(0))),
                    ('w', ast.ExpLit(ast.LitInt(1))),
                ],
                ast.ExpLit(ast.LitInt(10))))]
