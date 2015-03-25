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

import itertools
import math

import bayeslite
import bayeslite.crosscat
from bayeslite.guess import bayesdb_guess_stattypes
from bayeslite.guess import bayesdb_guess_generator

import crosscat.LocalEngine

def test_guess_stattypes():
    n = ['a', 'b']
    a_z = range(ord('a'), ord('z') + 1)
    rows = [[chr(c), c % 2] for c in a_z]
    assert bayesdb_guess_stattypes(n, rows) == ['key', 'categorical']
    rows = [[chr(c), c % 2] for c in a_z] + [['q', ord('q') % 2]]
    assert bayesdb_guess_stattypes(n, rows) == ['categorical', 'categorical']
    rows = [[c % 2, chr(c)] for c in a_z]
    assert bayesdb_guess_stattypes(n, rows) == ['categorical', 'key']
    rows = [[c % 2, chr(c)] for c in a_z] + [[0, 'k']]
    assert bayesdb_guess_stattypes(n, rows) == ['categorical', 'categorical']
    rows = [[chr(c), i] for i, c in enumerate(a_z)]
    assert bayesdb_guess_stattypes(n, rows) == ['key', 'numerical']
    rows = [[chr(c), math.sqrt(i)] for i, c in enumerate(a_z)]
    assert bayesdb_guess_stattypes(n, rows) == ['key', 'numerical']
    rows = [[chr(c) + chr(d), isqrt(i)] for i, (c, d)
        in enumerate(itertools.product(a_z, a_z))]
    assert bayesdb_guess_stattypes(n, rows) == ['key', 'numerical']
    rows = [[chr(c) + chr(d) + chr(e), isqrt(i)] for i, (c, d, e)
        in enumerate(itertools.product(a_z, a_z, a_z))]
    assert bayesdb_guess_stattypes(n, rows) == ['key', 'categorical']
    rows = [[i, chr(c)] for i, c in enumerate(a_z)]
    assert bayesdb_guess_stattypes(n, rows) == ['key', 'categorical']
    rows = [[isqrt(i), chr(c) + chr(d)] for i, (c, d)
        in enumerate(itertools.product(a_z, a_z))]
    assert bayesdb_guess_stattypes(n, rows) == ['numerical', 'key']
    rows = [[isqrt(i), chr(c) + chr(d) + chr(e)] for i, (c, d, e)
        in enumerate(itertools.product(a_z, a_z, a_z))]
    assert bayesdb_guess_stattypes(n, rows) == ['categorical', 'key']

def test_guess_generator():
    bdb = bayeslite.BayesDB()
    bdb.sql_execute('CREATE TABLE t(x NUMERIC, y NUMERIC, z NUMERIC)')
    a_z = range(ord('a'), ord('z') + 1)
    aa_zz = ((c, d) for c in a_z for d in a_z)
    data = ((chr(c) + chr(d), (c + d) % 2, math.sqrt(c + d)) for c, d in aa_zz)
    for row in data:
        bdb.sql_execute('INSERT INTO t (x, y, z) VALUES (?, ?, ?)', row)
    cc = crosscat.LocalEngine.LocalEngine(seed=0)
    metamodel = bayeslite.crosscat.CrosscatMetamodel(cc)
    bayeslite.bayesdb_register_metamodel(bdb, metamodel)
    bayesdb_guess_generator(bdb, 't_cc', 't', 'crosscat')
    assert list(bdb.sql_execute('SELECT * FROM bayesdb_generator_column')) == [
        (1, 1, 'categorical'),
        (1, 2, 'numerical'),
    ]

def isqrt(n):
    x = n
    y = (x + 1)//2
    while y < x:
        x = y
        y = (x + n//x)//2
    return x
