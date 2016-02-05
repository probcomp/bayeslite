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

import itertools
import math
import pytest

import bayeslite
from bayeslite.metamodels.crosscat import CrosscatMetamodel
from bayeslite.guess import bayesdb_guess_stattypes
from bayeslite.guess import bayesdb_guess_generator

import crosscat.LocalEngine

def test_guess_stattypes():
    n = ['a', 'b']
    a_z = range(ord('a'), ord('z') + 1)
    rows = [[chr(c), c % 2] for c in a_z]
    with pytest.raises(ValueError):
        # Duplicate column names.
        bayesdb_guess_stattypes(['a', 'a'], rows)
    with pytest.raises(ValueError):
        # Too many columns in data.
        bayesdb_guess_stattypes(['a'], rows)
    with pytest.raises(ValueError):
        # Too few columns in data.
        bayesdb_guess_stattypes(['a', 'b', 'c'], rows)
    assert bayesdb_guess_stattypes(n, rows) == ['key', 'categorical']
    rows = [[chr(c), c % 2] for c in a_z] + [['q', ord('q') % 2]]
    # Ignore the first column, rather than calling it categorical, because
    # it's almost entirely unique, so one category cannot say much about others.
    assert bayesdb_guess_stattypes(n, rows) == ['ignore', 'categorical']
    rows = [[c % 2, chr(c)] for c in a_z]
    assert bayesdb_guess_stattypes(n, rows) == ['categorical', 'key']
    rows = [[c % 2, chr(c)] for c in a_z] + [[0, 'k']]
    # Ignore the second column because it is almost unique, as above.
    assert bayesdb_guess_stattypes(n, rows) == ['categorical', 'ignore']
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
    # second field is unique, and we already have a key.
    assert bayesdb_guess_stattypes(n, rows) == ['key', 'ignore']
    rows = [[isqrt(i), chr(c) + chr(d)] for i, (c, d)
        in enumerate(itertools.product(a_z, a_z))]
    assert bayesdb_guess_stattypes(n, rows) == ['numerical', 'key']
    rows = [[isqrt(i), chr(c) + chr(d) + chr(e)] for i, (c, d, e)
        in enumerate(itertools.product(a_z, a_z, a_z))]
    assert bayesdb_guess_stattypes(n, rows) == ['categorical', 'key']
    with pytest.raises(ValueError):
        # Nonunique key.
        bayesdb_guess_stattypes(n, rows, overrides=[('a', 'key')])
    with pytest.raises(ValueError):
        # Two keys.
        bayesdb_guess_stattypes(n, rows,
            overrides=[('a', 'key'), ('b', 'key')])
    with pytest.raises(ValueError):
        # No such column.
        bayesdb_guess_stattypes(n, rows, overrides=[('c', 'numerical')])
    with pytest.raises(ValueError):
        # Column overridden twice.
        bayesdb_guess_stattypes(n, rows,
            overrides=[('a', 'key'), ('a', 'ignore')])
    with pytest.raises(ValueError):
        # Column overridden twice, even to the same stattype.
        bayesdb_guess_stattypes(n, rows,
            overrides=[('a', 'key'), ('a', 'key')])
    assert bayesdb_guess_stattypes(n, rows, overrides=[('b', 'key')]) == \
        ['categorical', 'key']
    assert bayesdb_guess_stattypes(n, rows, overrides=[('b', 'ignore')]) == \
        ['categorical', 'ignore']
    assert bayesdb_guess_stattypes(n, rows, overrides=[('a', 'numerical')]) ==\
        ['numerical', 'key']
    rows = [['none' if c < ord('m') else c, chr(c)] for c in a_z]
    # Nullify 'none' because it is in the nullify list.
    # Categorical because <20 remaining.
    assert bayesdb_guess_stattypes(n, rows) == ['categorical', 'key']
    rows = [[3 if c < ord('y') else 5, chr(c)] for c in a_z]
    # Nullify 3 because it holds so many of the values.
    # Ignore because <2 remaining.
    assert bayesdb_guess_stattypes(n, rows) == ['ignore', 'key']


def test_guess_generator():
    bdb = bayeslite.bayesdb_open(builtin_metamodels=False)
    bdb.sql_execute('CREATE TABLE t(x NUMERIC, y NUMERIC, z NUMERIC)')
    a_z = range(ord('a'), ord('z') + 1)
    aa_zz = ((c, d) for c in a_z for d in a_z)
    data = ((chr(c) + chr(d), (c + d) % 2, math.sqrt(c + d)) for c, d in aa_zz)
    for row in data:
        bdb.sql_execute('INSERT INTO t (x, y, z) VALUES (?, ?, ?)', row)
    cc = crosscat.LocalEngine.LocalEngine(seed=0)
    metamodel = CrosscatMetamodel(cc)
    bayeslite.bayesdb_register_metamodel(bdb, metamodel)
    with pytest.raises(ValueError):
        # No modelled columns.  (x is key.)
        bayesdb_guess_generator(bdb, 't_cc', 't', 'crosscat',
            overrides=[('y', 'ignore'), ('z', 'ignore')])
    bayesdb_guess_generator(bdb, 't_cc', 't', 'crosscat')
    with pytest.raises(ValueError):
        # Generator already exists.
        bayesdb_guess_generator(bdb, 't_cc', 't', 'crosscat')
    assert bdb.sql_execute('SELECT *'
            ' FROM bayesdb_generator_column').fetchall() == [
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
