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
    assert parse_bql_string('select 0;') == \
        [ast.Select(ast.SELQUANT_ALL,
            ast.SelCols([ast.SelColExp(ast.ExpLit(ast.LitInt(0)), None)]),
            None, None, None, None, None)]
    assert parse_bql_string('select 0 as z;') == \
        [ast.Select(ast.SELQUANT_ALL,
            ast.SelCols([ast.SelColExp(ast.ExpLit(ast.LitInt(0)), 'z')]),
            None, None, None, None, None)]
    assert parse_bql_string('select * from t;') == \
        [ast.Select(ast.SELQUANT_ALL, ast.SelCols([ast.SelColAll(None)]),
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select t.* from t;') == \
        [ast.Select(ast.SELQUANT_ALL, ast.SelCols([ast.SelColAll('t')]),
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select c from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            ast.SelCols([ast.SelColExp(ast.ExpCol(None, 'c'), None)]),
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select c as d from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            ast.SelCols([ast.SelColExp(ast.ExpCol(None, 'c'), 'd')]),
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select t.c as d from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            ast.SelCols([ast.SelColExp(ast.ExpCol('t', 'c'), 'd')]),
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select t.c as d, p as q, x from t;') == \
        [ast.Select(ast.SELQUANT_ALL,
            ast.SelCols([
                ast.SelColExp(ast.ExpCol('t', 'c'), 'd'),
                ast.SelColExp(ast.ExpCol(None, 'p'), 'q'),
                ast.SelColExp(ast.ExpCol(None, 'x'), None),
            ]),
            [ast.SelTab('t', None)], None, None, None, None)]
    assert parse_bql_string('select * from t, u;') == \
        [ast.Select(ast.SELQUANT_ALL, ast.SelCols([ast.SelColAll(None)]),
            [ast.SelTab('t', None), ast.SelTab('u', None)],
            None, None, None, None)]
    assert parse_bql_string('select * from t as u;') == \
        [ast.Select(ast.SELQUANT_ALL, ast.SelCols([ast.SelColAll(None)]),
            [ast.SelTab('t', 'u')],
            None, None, None, None)]
    assert parse_bql_string('select * where x;') == \
        [ast.Select(ast.SELQUANT_ALL, ast.SelCols([ast.SelColAll(None)]),
            None, ast.ExpCol(None, 'x'), None, None, None)]
    assert parse_bql_string('select * from t where x;') == \
        [ast.Select(ast.SELQUANT_ALL, ast.SelCols([ast.SelColAll(None)]),
            [ast.SelTab('t', None)],
            ast.ExpCol(None, 'x'), None, None, None)]
