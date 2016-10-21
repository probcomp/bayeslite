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

import crosscat.LocalEngine

import bayeslite
import bayeslite.read_csv as read_csv

from bayeslite.guess import bayesdb_guess_population
from bayeslite.metamodels.crosscat import CrosscatMetamodel


root = os.path.dirname(os.path.abspath(__file__))
dha_csv = os.path.join(root, 'dha.csv')


# Test that simulating a column constrained to have a specific value
# returns that value, not any old random draw from the observed
# variables given the conditionally drawn latent variables.
#
# XXX This should be a metamodel-independent test.
def test_simulate_drawconstraint():
    with bayeslite.bayesdb_open() as bdb:
        with open(dha_csv, 'rU') as f:
            read_csv.bayesdb_read_csv(bdb, 'dha', f, header=True, create=True)
        bayesdb_guess_population(
            bdb, 'hospital', 'dha', overrides=[('name', 'key')])
        bdb.execute('''
            CREATE GENERATOR hospital_cc FOR hospital USING crosscat()
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR hospital_cc')
        bdb.execute('ANALYZE hospital_cc FOR 1 ITERATION WAIT')
        samples = bdb.execute('''
            SIMULATE ttl_mdcr_spnd, n_death_ill FROM hospital
                GIVEN TTL_MDCR_SPND = 40000
                LIMIT 100
        ''').fetchall()
        assert [s[0] for s in samples] == [40000] * 100


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
        bdb.sql_execute('CREATE TABLE t(x TEXT, y NUMERIC)')
        for row in data:
            bdb.sql_execute('INSERT INTO t (x, y) VALUES (?, ?)', row)
        bdb.execute('''
            CREATE POPULATION t_p FOR t WITH SCHEMA {
                IGNORE x;
                MODEL y AS NUMERICAL
            }
        ''')
        bdb.execute('''
            CREATE GENERATOR t_g FOR t_p;
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR t_g')
        bdb.execute('ANALYZE t_g FOR 3 ITERATION WAIT')
        bdb.execute('''CREATE TABLE row1 AS
            SIMULATE y FROM t_p
            GIVEN _rowid_ = 1
            LIMIT 100
        ''')
        bdb.execute('''CREATE TABLE row5 AS
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
    ('foo', 6, 7, None),
    ('bar', 1, 1, 2),
    ('baz', 100, 100, 200),
    ('quux', 1000, 2000, 3000),
    ('zot', 0, 2, 2),
    ('mumble', 20, 10, 30),
    ('frotz', 4, 13, 17),
    ('gargle', 34, 2, 36),
    ('mumph', 78, 4, 82),
    ('hunf', 90, 1, 91),
    ('blort', 80, 80, 160)
]


def test_simulate_given_rowid_multivariate():
    # Test that GIVEN statement can accept a multivariate constraint clause in
    # which one of the constraints is on _rowid_.
    with bayeslite.bayesdb_open() as bdb:
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
        bdb.execute('''
            CREATE GENERATOR t_g FOR t_p;
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR t_g')
        bdb.execute('ANALYZE t_g FOR 20 ITERATION WAIT ( OPTIMIZED )')
        bdb.execute('''CREATE TABLE row1_1 AS
            SIMULATE y FROM t_p
            GIVEN _rowid_ = 1, w = 3000
            LIMIT 100
        ''')
        bdb.execute('''CREATE TABLE row1_2 AS
            SIMULATE y FROM t_p
            GIVEN ROWID = 1, w = 1
            LIMIT 100
        ''')
        row1_1_avg = bdb.execute('SELECT AVG(y) FROM row1_1').fetchall()[0][0]
        row1_2_avg = bdb.execute('SELECT AVG(y) FROM row1_2').fetchall()[0][0]
        # We expect these values to be close to each other, because conditioning
        # on _rowid_ decouples the dependencies between other variables in the
        # CrossCat metamodel, so the additional condition on w should have no
        # effect.
        assert abs(row1_1_avg - row1_2_avg) < 2

        # A call to SIMULATE without CREATE TABLE.
        result = bdb.execute('''
            SIMULATE y FROM t_p
            GIVEN oid = 1, w = 3 LIMIT 10;
        ''').fetchall()
        assert len(result) == 10
