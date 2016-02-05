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

import bayeslite
from bayeslite.core import bayesdb_get_generator
from bayeslite.metamodels.crosscat import CrosscatMetamodel
import bayeslite.read_csv as read_csv
import crosscat.LocalEngine

root = os.path.dirname(os.path.abspath(__file__))
dha_csv = os.path.join(root, 'dha.csv')

# Test that simulating a column constrained to have a specific value
# returns that value, not any old random draw from the observed
# variables given the conditionally drawn latent variables.
#
# XXX This should be a metamodel-independent test.
def test_simulate_drawconstraint():
    with bayeslite.bayesdb_open(builtin_metamodels=False) as bdb:
        cc = crosscat.LocalEngine.LocalEngine(seed=0)
        metamodel = CrosscatMetamodel(cc)
        bayeslite.bayesdb_register_metamodel(bdb, metamodel)
        with open(dha_csv, 'rU') as f:
            read_csv.bayesdb_read_csv(bdb, 'dha', f, header=True, create=True)
        bdb.execute('''
            CREATE GENERATOR dha_cc FOR dha USING crosscat (
                GUESS(*),
                name KEY
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR dha_cc')
        bdb.execute('ANALYZE dha_cc FOR 1 ITERATION WAIT')
        samples = bdb.execute('''
            SIMULATE ttl_mdcr_spnd, n_death_ill FROM dha_cc
                GIVEN TTL_MDCR_SPND = 40000
                LIMIT 100
        ''').fetchall()
        assert [s[0] for s in samples] == [40000] * 100
