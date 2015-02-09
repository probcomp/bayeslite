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

import os

import bayeslite
import crosscat.LocalEngine

root = os.path.dirname(os.path.abspath(__file__))
dha_csv = root + '/dha.csv'
dha_models = root + '/dha_models.pkl.gz'

def test_legacy_models():
    bdb = bayeslite.BayesDB()
    engine = crosscat.LocalEngine.LocalEngine(seed=0)
    bayeslite.bayesdb_register_metamodel(bdb, 'crosscat', engine)
    bayeslite.bayesdb_set_default_metamodel(bdb, 'crosscat')
    bayeslite.bayesdb_import_csv_file(bdb, 'dha', dha_csv)
    bayeslite.bayesdb_load_legacy_models(bdb, 'dha', dha_models)
    bql = '''
        SELECT name FROM dha
            ORDER BY SIMILARITY TO (SELECT rowid FROM dha WHERE name = ?) DESC
            LIMIT 10
    '''
    assert list(bdb.execute(bql, ('Albany NY',))) == [
        ('Albany NY',),
        ('Scranton PA',),
        ('United States US',),
        ('Norfolk VA',),
        ('Reading PA',),
        ('Salisbury MD',),
        ('Louisville KY',),
        ('Cleveland OH',),
        ('Covington KY',),
        ('Akron OH',),
    ]
