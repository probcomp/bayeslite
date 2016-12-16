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

from bayeslite.core import bayesdb_get_generator
from bayeslite.guess import bayesdb_guess_population
from bayeslite.metamodels.crosscat import CrosscatMetamodel

root = os.path.dirname(os.path.abspath(__file__))
dha_csv = os.path.join(root, 'dha.csv')

def test_subsample():
    with bayeslite.bayesdb_open(builtin_metamodels=False) as bdb:
        cc = crosscat.LocalEngine.LocalEngine(seed=0)
        metamodel = CrosscatMetamodel(cc)
        bayeslite.bayesdb_register_metamodel(bdb, metamodel)
        with open(dha_csv, 'rU') as f:
            read_csv.bayesdb_read_csv(bdb, 'dha', f, header=True, create=True)
        bayesdb_guess_population(bdb, 'hospitals_full', 'dha',
            overrides=[('name', 'key')])
        bayesdb_guess_population(bdb, 'hospitals_sub', 'dha',
            overrides=[('name', 'key')])
        bdb.execute('''
            CREATE GENERATOR hosp_full_cc FOR hospitals_full USING crosscat (
                SUBSAMPLE(OFF)
            )
        ''')
        bdb.execute('''
            CREATE GENERATOR hosp_sub_cc FOR hospitals_sub USING crosscat (
                SUBSAMPLE(100)
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR hosp_sub_cc')
        bdb.execute('ANALYZE hosp_sub_cc FOR 1 ITERATION WAIT')
        bdb.execute('''
            ESTIMATE SIMILARITY TO (_rowid_=2) WITH RESPECT TO (PNEUM_SCORE)
            FROM hospitals_sub WHERE _rowid_ = 1 OR _rowid_ = 101
        ''').fetchall()
        bdb.execute('''
            ESTIMATE SIMILARITY TO (_rowid_=102) WITH RESPECT TO
            (N_DEATH_ILL) FROM hospitals_sub
            WHERE _rowid_ = 1 OR _rowid_ = 101
        ''').fetchall()
        bdb.execute('''
            ESTIMATE PREDICTIVE PROBABILITY OF mdcr_spnd_amblnc
            FROM hospitals_sub
            WHERE _rowid_ = 1 OR _rowid_ = 101
        ''').fetchall()
        bdb.execute('''
            ESTIMATE SIMILARITY WITH RESPECT TO (PNEUM_SCORE)
            FROM PAIRWISE hospitals_sub
            WHERE (r0._rowid_ = 1 OR r0._rowid_ = 101) AND
            (r1._rowid_ = 1 OR r1._rowid_ = 101)
        ''').fetchall()
        bdb.execute('''
            INFER mdcr_spnd_amblnc FROM hospitals_sub
            WHERE _rowid_ = 1 OR _rowid_ = 101
        ''').fetchall()
        sql = '''
            SELECT sql_rowid FROM bayesdb_crosscat_subsample
                WHERE generator_id = ?
                ORDER BY cc_row_id ASC
                LIMIT 100
        '''
        gid_full = bayesdb_get_generator(bdb, None, 'hosp_full_cc')
        cursor = bdb.sql_execute(sql, (gid_full,))
        assert [row[0] for row in cursor] == range(1, 100 + 1)
        gid = bayesdb_get_generator(bdb, None, 'hosp_sub_cc')
        cursor = bdb.sql_execute(sql, (gid,))
        assert [row[0] for row in cursor] != range(1, 100 + 1)
        bdb.execute('DROP GENERATOR hosp_sub_cc')
        bdb.execute('DROP GENERATOR hosp_full_cc')
        bdb.execute('DROP POPULATION hospitals_sub')
        bdb.execute('DROP POPULATION hospitals_full')
