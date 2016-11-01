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

'''Ensures that INFER EXPLICIT PREDICT col for a _rowid_ which exists in the
base table, but not incorporated into the metamodel (i.e. did not exist in the
table during INITIALIZE) correctly retrieves all other values in the row. The
test cases creates a population of (0,1) and (1,0) -- the complement function --
and ensures its recovery on a held-out test set.'''

import pytest

from bayeslite import bayesdb_open

from bayeslite.core import bayesdb_get_generator
from bayeslite.core import bayesdb_get_population
from bayeslite.core import bayesdb_population_fresh_row_id
from bayeslite.util import cursor_value


@pytest.fixture(scope='module')
def bdb():
    bdb = bayesdb_open(':memory:')

    # Create the population of complements.
    bdb.sql_execute('CREATE TABLE t (a TEXT, b TEXT)')
    for _ in xrange(20):
        bdb.sql_execute('INSERT INTO t (a, b) VALUES (0,1)')
    for _ in xrange(20):
        bdb.sql_execute('INSERT INTO t (a, b) VALUES (1,0)')

    # Create the population and metamodel on the existing rows.
    bdb.execute('CREATE POPULATION p FOR t (MODEL a, b AS NOMINAL)')
    bdb.execute('CREATE METAMODEL m FOR p;')
    bdb.execute('INITIALIZE 1 MODELS FOR m;')
    bdb.execute('ANALYZE m FOR 1000 ITERATION WAIT (OPTIMIZED);')

    # Add new 'hypothetical' rows into the base table to serve as out-of-
    # sample probe points; only zeros, only ones, and nothing.
    for _ in xrange(40, 50):
        bdb.sql_execute('INSERT INTO t (a) VALUES (0)')
    for _ in xrange(50, 60):
        bdb.sql_execute('INSERT INTO t (b) VALUES (1)')
    for _ in xrange(60, 80):
        bdb.sql_execute('INSERT INTO t (a,b) VALUES (NULL, NULL)')

    # Make sure fresh_row_id 80 from the base table, not metamodel.
    population_id = bayesdb_get_population(bdb, 'p')
    assert bayesdb_population_fresh_row_id(bdb, population_id) == 81

    # Make sure the cgpm only has 40 rowids incorporated.
    generator_id = bayesdb_get_generator(bdb, population_id, 'm')
    cursor = bdb.sql_execute('''
        SELECT MAX(table_rowid) FROM bayesdb_cgpm_individual
        WHERE generator_id = ?
    ''', (generator_id,))
    assert cursor_value(cursor) == 40

    # Turn off multiprocessing for sequence of queries.
    bdb.metamodels['cgpm'].set_multiprocess(False)
    return bdb


def test_infer_ones(bdb):
    # All the b's should be one (check 90%).
    b_ones = bdb.execute('''
        INFER EXPLICIT
            a,
            PREDICT b CONFIDENCE conf USING 20 SAMPLES
        FROM p WHERE oid BETWEEN 41 AND 50
    ''').fetchall()
    assert all(r[0] == '0' for r in b_ones)
    assert len([r for r in b_ones if r[1] == '1' and r[2] > 0.8]) >= 9


def test_infer_zeros(bdb):
    # All the a's should be zero (check 90%).
    a_zeros = bdb.execute('''
        INFER EXPLICIT
            PREDICT a CONFIDENCE conf USING 20 SAMPLES,
            b
        FROM p WHERE oid BETWEEN 51 AND 60
    ''').fetchall()
    assert all(r[2] == '1' for r in a_zeros)
    assert len([r for r in a_zeros if r[0] == '0' and r[1] > 0.8]) >= 9


def test_infer_uniform_marginal(bdb):
    # None of these queries should illustrate a dominant pattern of 0s or 1s. We
    # test heuristically not statistically.

    uniform_a = bdb.execute('''
        INFER EXPLICIT PREDICT a CONFIDENCE conf USING 20 SAMPLES
        FROM p WHERE oid BETWEEN 61 AND 80
    ''').fetchall()
    assert len([r for r in uniform_a if r[0] == '1' and r[1] > 0.55]) < 15
    assert len([r for r in uniform_a if r[0] == '0' and r[1] > 0.55]) < 15

    uniform_b = bdb.execute('''
        INFER EXPLICIT PREDICT b CONFIDENCE conf USING 20 SAMPLES
        FROM p WHERE oid BETWEEN 61 AND 80
    ''').fetchall()
    assert len([r for r in uniform_a if r[0] == '1' and r[1] > 0.55]) < 15
    assert len([r for r in uniform_a if r[0] == '0' and r[1] > 0.55]) < 15


def test_infer_uniform_joint(bdb):
    # Should be roughly independent.
    uniform_ab = bdb.execute('''
        INFER EXPLICIT
            PREDICT a CONFIDENCE confa USING 10 SAMPLES,
            PREDICT b CONFIDENCE confb USING 10 SAMPLES
        FROM p WHERE oid BETWEEN 61 AND 80
    ''').fetchall()
    a0b0 = [r for r in uniform_ab if r[0] == '0' and r[1] == '0']
    a0b1 = [r for r in uniform_ab if r[0] == '0' and r[1] == '1']
    a1b0 = [r for r in uniform_ab if r[0] == '1' and r[1] == '0']
    a1b1 = [r for r in uniform_ab if r[0] == '1' and r[1] == '1']

    # None of these should comprise more than 50% of the samples.
    assert all([len(s) < 10 for s in [a0b0, a0b1, a1b0, a1b1]])
