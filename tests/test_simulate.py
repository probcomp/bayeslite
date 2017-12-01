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

import os

import pytest

import bayeslite
import bayeslite.read_csv as read_csv

from bayeslite.guess import bayesdb_guess_population


root = os.path.dirname(os.path.abspath(__file__))
dha_csv = os.path.join(root, 'dha.csv')


# Test that simulating a column constrained to have a specific value
# returns an error.
#
# XXX This should be a backend-independent test.
def test_simulate_drawconstraint_error__ci_slow():
    with bayeslite.bayesdb_open() as bdb:
        with open(dha_csv, 'rU') as f:
            read_csv.bayesdb_read_csv(bdb, 'dha', f, header=True, create=True)
        bdb.backends['cgpm'].set_multiprocess(False)
        bayesdb_guess_population(
            bdb, 'hospital', 'dha', overrides=[('name', 'key')])
        bdb.execute(
            'CREATE GENERATOR hospital_cc FOR hospital USING cgpm;')
        bdb.execute('INITIALIZE 1 MODEL FOR hospital_cc')
        bdb.execute('ANALYZE hospital_cc FOR 1 ITERATION (OPTIMIZED);')
        with pytest.raises(ValueError):
            bdb.execute('''
                SIMULATE ttl_mdcr_spnd, n_death_ill FROM hospital
                    GIVEN TTL_MDCR_SPND = 40000
                    LIMIT 100
            ''').fetchall()
        samples = bdb.execute('''
            SIMULATE n_death_ill FROM hospital
                GIVEN TTL_MDCR_SPND = 40000
                LIMIT 100
        ''').fetchall()
        assert len(samples) == 100
        assert all(len(s) == 1 for s in samples)


data = [
    ('foo', 56),
    ('bar', 0),
    ('baz', 1),
    ('quux', 1),
    ('zot', 0),
    ('mumble', 2),
    ('frotz', 0),
    ('gargle', 0),
    ('mumph', 1),
    ('hunf', 3),
    ('blort', 0)
]


def test_simulate_given_rowid():
    # Test simulation of a variable given a rowid. Uses synthetic data with
    # one variable, in which one value of the variable is different from the
    # others by an order of magnitude. Thus, simulating the value for that row
    # should produce values that are significantly different from simulated
    # values of the variable for another row.
    with bayeslite.bayesdb_open() as bdb:
        bdb.backends['cgpm'].set_multiprocess(False)
        bdb.sql_execute('CREATE TABLE t(x TEXT, y NUMERIC)')
        for row in data:
            bdb.sql_execute('INSERT INTO t (x, y) VALUES (?, ?)', row)
        bdb.execute('''
            CREATE POPULATION t_p FOR t WITH SCHEMA {
                IGNORE x;
                MODEL y AS NUMERICAL;
            }
        ''')
        bdb.execute('''
            CREATE GENERATOR t_g FOR t_p;
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR t_g')
        bdb.execute('ANALYZE t_g FOR 3 ITERATION')
        bdb.execute('''
            CREATE TABLE row1 AS
                SIMULATE y FROM t_p
                GIVEN _rowid_ = 1
            LIMIT 100
        ''')
        bdb.execute('''
            CREATE TABLE row5 AS
                SIMULATE y FROM t_p
                GIVEN oid = 5
            LIMIT 100
        ''')
        row1_avg = bdb.execute('SELECT AVG(y) FROM row1').fetchall()[0][0]
        row5_avg = bdb.execute('SELECT AVG(y) FROM row5').fetchall()[0][0]
        # Mean of simulations for row 1 should be "significantly" larger.
        assert row5_avg + 10 < row1_avg

        # Multiple specified rowids should produce an error.
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('''
                SIMULATE y FROM t_p
                GIVEN oid = 5, rowid = 2 LIMIT 10;
            ''')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('''
                SIMULATE y FROM t_p
                GIVEN _rowid_ = 5, OID = 1, w = 3 LIMIT 10;
            ''')


data_multivariate = [
    ('foo',     6,      7,      None),      # rowid = 1
    ('bar',     1,      1,      2),         # rowid = 2
    ('baz',     100,    100,    200),       # rowid = 3
    ('quux',    1000,   2000,   3000),      # rowid = 4
    ('zot',     0,      2,      2),         # rowid = 5
    ('mumble',  20,     10,     30),        # rowid = 6
    ('frotz',   4,      13,     17),        # rowid = 7
    ('gargle',  34,     2,      36),        # rowid = 8
    ('mumph',   78,     4,      82),        # rowid = 9
    ('hunf',    90,     1,      91),        # rowid = 10
    ('blort',   80,     80,     160),       # rowid = 11
    ('wip',     None,   9,      5),         # rowid = 12
]


def test_simulate_given_rowid_multivariate():
    # Test that GIVEN statement can accept a multivariate constraint clause in
    # which one of the constraints is on _rowid_.
    with bayeslite.bayesdb_open() as bdb:
        bdb.backends['cgpm'].set_multiprocess(False)
        bdb.sql_execute(
            'CREATE TABLE t(x TEXT, y NUMERIC, z NUMERIC, w NUMERIC)')
        for row in data_multivariate:
            bdb.sql_execute(
                'INSERT INTO t (x, y, z, w) VALUES (?, ?, ?, ?)', row)
        bdb.execute('''
            CREATE POPULATION t_p FOR t WITH SCHEMA {
                MODEL y, z, w AS NUMERICAL;
                IGNORE x
            }
        ''')
        bdb.execute('CREATE GENERATOR t_g FOR t_p;')
        bdb.execute('INITIALIZE 1 MODEL FOR t_g')
        bdb.execute('ANALYZE t_g FOR 100 ITERATION (OPTIMIZED)')
        bdb.execute('''
            CREATE TABLE row1_1 AS
                SIMULATE y FROM t_p
                GIVEN _rowid_ = 1, w = 3000
            LIMIT 1500
        ''')
        bdb.execute('''
            CREATE TABLE row1_2 AS
                SIMULATE y FROM t_p
                GIVEN ROWID = 1, w = 1
            LIMIT 1500
        ''')
        row1_1_avg = bdb.execute('SELECT AVG(y) FROM row1_1').fetchall()[0][0]
        row1_2_avg = bdb.execute('SELECT AVG(y) FROM row1_2').fetchall()[0][0]
        # We expect these values to be close to each other, because conditioning
        # on _rowid_ decouples the dependencies between other variables in the
        # CrossCat generator, so the additional condition on w should have no
        # effect.
        assert abs(row1_1_avg - row1_2_avg) < 2

        bdb.execute('''
            CREATE TABLE row_w3000 AS
                SIMULATE y FROM t_p
                GIVEN w = 3000
            LIMIT 100
        ''')
        bdb.execute('''
            CREATE TABLE row_w1 AS
                SIMULATE y FROM t_p
                GIVEN w = 1
            LIMIT 100
        ''')
        row_w1 = bdb.execute('SELECT AVG(y) FROM row_w1').fetchall()[0][0]
        row_w3000 = bdb.execute('SELECT AVG(y) FROM row_w3000').fetchall()[0][0]
        # We expect these values to be far from each other, because the
        # condition on w should specify very different posteriors for y.
        assert abs(row_w1 - row_w3000) > 100

        # A call to SIMULATE without CREATE TABLE. Since oid 1 has w = None, the
        # constraint specification w = 3 is legal.
        result = bdb.execute('''
            SIMULATE y FROM t_p
            GIVEN oid = 1, w = 3
            LIMIT 10;
        ''').fetchall()
        assert len(result) == 10


def test_simulate_given_rowid_unincorporated():
    '''Ensure specifying rowid loads constraints for unincorporated rows'''
    with bayeslite.bayesdb_open() as bdb:
        bdb.backends['cgpm'].set_multiprocess(False)
        bdb.sql_execute(
            'CREATE TABLE t(x TEXT, y NUMERIC, z NUMERIC, w NUMERIC)')
        for row in data_multivariate[:-5]:
            bdb.sql_execute(
                'INSERT INTO t (x, y, z, w) VALUES (?, ?, ?, ?)', row)
        bdb.execute('''
            CREATE POPULATION t_p FOR t WITH SCHEMA {
                MODEL y, z, w AS NUMERICAL;
                IGNORE x
            }
        ''')
        bdb.execute('CREATE GENERATOR t_g FOR t_p;')
        bdb.execute('INITIALIZE 1 MODEL FOR t_g')
        bdb.execute('ANALYZE t_g FOR 20 ITERATION (OPTIMIZED)')

        # User cannot override values in incorporated rowids. A ValueError is
        # captured because checking for observed rowids is performed by cgpm.
        with pytest.raises(ValueError):
            bdb.execute('''
                SIMULATE y FROM t_p
                GIVEN rowid = 3, z = 99
                LIMIT 10
            ''')

        # Insert remaining five rows into base table without incorporating
        # the data into the generator.
        for row in data_multivariate[-5:]:
            bdb.sql_execute(
                'INSERT INTO t (x, y, z, w) VALUES (?, ?, ?, ?)', row)

        # Since rowid = 12 has y = None, the override to y = 1 is legal.
        bdb.execute('''
            SIMULATE z FROM t_p
            GIVEN rowid = 12, y = 1
            LIMIT 10
        ''').fetchall()
