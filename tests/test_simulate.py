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
        bayesdb_guess_population(bdb, 'hospital', 'dha',
            overrides=[('name', 'key')])
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
    with bayeslite.bayesdb_open() as bdb:
        bdb.sql_execute('CREATE TABLE t(x TEXT, y NUMERIC)')
        for row in data:
            bdb.sql_execute('INSERT INTO t (x, y) VALUES (?, ?)', row)
        bdb.execute('''
            CREATE POPULATION t_p FOR t WITH SCHEMA {
                MODEL y AS NUMERICAL;
                IGNORE x
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
            GIVEN _rowid_ = 5
            LIMIT 100
        ''')
        row1_avg = query(bdb, 'SELECT AVG(y) FROM row1')
        row1_avg = row1_avg.iloc[0, 0]
        row5_avg = query(bdb, 'SELECT AVG(y) FROM row5')
        row5_avg = row5_avg.iloc[0, 0]
        # Mean of simulations for row 1 should be "significantly" larger.
        assert row1_avg > row5_avg + 10
