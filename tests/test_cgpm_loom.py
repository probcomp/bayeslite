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

from bayeslite import bayesdb_open
from bayeslite import bayesdb_nullify
from bayeslite.exception import BQLError

os.environ['LOOM_VERBOSITY'] = '0'

root = os.path.dirname(os.path.abspath(__file__))
dha_csv = os.path.join(root, 'dha.csv')
satellites_csv = os.path.join(root, 'satellites.csv')

'''
Integration test for using `ANALYZE <generator> FOR <k> ITERATION (loom); on
dha.csv and satellites.csv.
'''

def loom_analyze(csv_filename):
    try:
        import loom
    except ImportError:
        pytest.skip('no loom')
        return
    with bayesdb_open(':memory:') as bdb:
        bdb = bayesdb_open(':memory:')
        bdb.execute('CREATE TABLE t FROM \'%s\'' % (csv_filename))
        bayesdb_nullify(bdb, 't', 'NaN')
        bdb.execute('''
            CREATE POPULATION p FOR t WITH SCHEMA(
                GUESS STATTYPES OF (*);
            )
        ''')
        bdb.execute('CREATE GENERATOR m FOR p;')
        bdb.execute('INITIALIZE 10 MODELS FOR m')
        bdb.execute('ANALYZE m FOR 2 ITERATIONS (loom);')

        # targeted analysis for Loom not supported.
        with pytest.raises(BQLError):
            bdb.execute('''
                ANALYZE m FOR 1 ITERATION (loom; variables TTL_MDCR_SPND);
            ''')
        # progress for Loom not supported (error from cgpm).
        with pytest.raises(ValueError):
            bdb.execute('''
                ANALYZE m FOR 1 ITERATION (loom; quiet);
            ''')
        # timing for Loom not supported  (error from cgpm).
        with pytest.raises(ValueError):
            bdb.execute('''
                ANALYZE m FOR 1 SECONDS (loom);
            ''')
        # Run a BQL query.
        bdb.execute('''
            ESTIMATE DEPENDENCE PROBABILITY FROM PAIRWISE VARIABLES OF p;
        ''')
        # Make sure we can run inference afterwards.
        bdb.execute('ANALYZE m FOR 2 ITERATION;')

def test_loom_dha__ci_slow():
    loom_analyze(dha_csv)

def test_loom_satellites__ci_slow():
    loom_analyze(satellites_csv)
