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

# XXX This is turning into more than just a test of legacy models...

import os
import pytest

import bayeslite
from bayeslite.metamodels.crosscat import CrosscatMetamodel
import bayeslite.read_csv as read_csv
import crosscat.LocalEngine

root = os.path.dirname(os.path.abspath(__file__))
dha_csv = os.path.join(root, 'dha.csv')
dha_models = os.path.join(root, 'dha_models.pkl.gz')
dha_codebook = os.path.join(root, 'dha_codebook.csv')

def test_legacy_models__ci_slow():
    bdb = bayeslite.bayesdb_open(builtin_metamodels=False)
    cc = crosscat.LocalEngine.LocalEngine(seed=0)
    metamodel = CrosscatMetamodel(cc)
    bayeslite.bayesdb_register_metamodel(bdb, metamodel)
    with pytest.raises(ValueError):
        bayeslite.bayesdb_load_legacy_models(bdb, 'dha_cc', 'dha', 'crosscat',
            dha_models, create=True)
    with open(dha_csv, 'rU') as f:
        read_csv.bayesdb_read_csv(bdb, 'dha', f, header=True, create=True)
    bayeslite.bayesdb_load_legacy_models(bdb, 'dha_cc', 'dha', 'crosscat',
        dha_models, create=True)
    # Make sure guessing also works.
    bdb.execute('create generator dha_cc0 for dha using crosscat(guess(*))')
    bayeslite.bayesdb_load_codebook_csv_file(bdb, 'dha', dha_codebook)
    # Need to be able to overwrite existing codebook.
    #
    # XXX Not sure this is the right API.  What if overwrite is a
    # mistake?
    bayeslite.bayesdb_load_codebook_csv_file(bdb, 'dha', dha_codebook)
    bql = '''
        ESTIMATE name FROM dha_cc
            ORDER BY SIMILARITY TO (name = ?) DESC
            LIMIT 10
    '''
    with bdb.savepoint():
        assert bdb.execute(bql, ('Albany NY',)).fetchall() == [
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
    # Tickles an issue in case-folding of column names.
    bql = '''
        ESTIMATE name
            FROM dha_cc
            ORDER BY PREDICTIVE PROBABILITY OF mdcr_spnd_amblnc ASC
            LIMIT 10
    '''
    with bdb.savepoint():
        assert bdb.execute(bql).fetchall() == [
            ('McAllen TX',),
            ('Worcester MA',),
            ('Beaumont TX',),
            ('Temple TX',),
            ('Corpus Christi TX',),
            ('Takoma Park MD',),
            ('Kingsport TN',),
            ('Bangor ME',),
            ('Lebanon NH',),
            ('Panama City FL',),
        ]
