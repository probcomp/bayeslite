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

# XXX This is turning into more than just a test of legacy models...

import os
import pytest

import bayeslite
import crosscat.LocalEngine

root = os.path.dirname(os.path.abspath(__file__))
dha_csv = root + '/dha.csv'
dha_models = root + '/dha_models.pkl.gz'
dha_codebook = root + '/dha_codebook.csv'

def test_legacy_models():
    bdb = bayeslite.BayesDB()
    engine = crosscat.LocalEngine.LocalEngine(seed=0)
    bayeslite.bayesdb_register_metamodel(bdb, 'crosscat', engine)
    bayeslite.bayesdb_set_default_metamodel(bdb, 'crosscat')
    with pytest.raises(ValueError):
        bayeslite.bayesdb_load_legacy_models(bdb, 'dha', dha_models)
    bayeslite.bayesdb_import_csv_file(bdb, 'dha', dha_csv)
    bayeslite.bayesdb_load_legacy_models(bdb, 'dha', dha_models)
    bayeslite.bayesdb_import_codebook_csv_file(bdb, 'dha', dha_codebook)
    # Need to be able to overwrite existing codebook.
    #
    # XXX Not sure this is the right API.  What if overwrite is a
    # mistake?
    bayeslite.bayesdb_import_codebook_csv_file(bdb, 'dha', dha_codebook)
    bql = '''
        SELECT name FROM dha
            ORDER BY SIMILARITY TO (SELECT rowid FROM dha WHERE name = ?) DESC
            LIMIT 10
    '''
    with bdb.savepoint():
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
    # Tickles an issue in case-folding of column names.
    bql = '''
        SELECT name
            FROM dha
            ORDER BY PREDICTIVE PROBABILITY OF mdcr_spnd_amblnc ASC
            LIMIT 10
    '''
    with bdb.savepoint():
        assert list(bdb.execute(bql)) == [
            ('McAllen TX',),
            ('Beaumont TX',),
            ('Worcester MA',),
            ('Corpus Christi TX',),
            ('Temple TX',),
            ('Kingsport TN',),
            ('Lebanon NH',),
            ('Takoma Park MD',),
            ('Bangor ME',),
            ('Panama City FL',),
        ]

if False:
    bql = '''
        SELECT c0.name, c0.short_name, c1.name, c1.short_name, e.value
            FROM (ESTIMATE PAIRWISE DEPENDENCE PROBABILITY FROM DHA
                    WHERE name0 != \'name\' AND name1 != \'name\'
                    ORDER BY name0 ASC, name1 ASC
                    LIMIT 10) AS e,
                bayesdb_table_column AS c0,
                bayesdb_table_column AS c1
            WHERE c0.table_id = e.table_id AND c0.name = e.name0
                AND c1.table_id = e.table_id AND c1.name = e.name1
    '''
    assert list(bdb.execute(bql)) == [
        ('AMI_SCORE', 'Myocardial infarction score',
         'AMI_SCORE', 'Myocardial infarction score',
         1),
        ('AMI_SCORE', 'Myocardial infarction score',
         'CHF_SCORE', 'Cong heart failure score',
         1.0),
        ('AMI_SCORE', 'Myocardial infarction score',
         'EQP_COPAY_P_DCD', 'Equipment co-pay/dcd',
         0.2),
        ('AMI_SCORE', 'Myocardial infarction score',
         'HHA_VISIT_P_DCD', 'Home health visits/dcd',
         0.1),
        ('AMI_SCORE', 'Myocardial infarction score',
         'HI_IC_BEDS', 'High-intensity IC beds',
         0.0),
        ('AMI_SCORE', 'Myocardial infarction score',
         'HI_IC_DAYS_P_DCD', 'High-intensity IC days/dcd',
         0.0),
        ('AMI_SCORE', 'Myocardial infarction score',
         'HOSP_BEDS', 'Hospital beds',
         0.0),
        ('AMI_SCORE', 'Myocardial infarction score',
         'HOSP_DAY_RATIO', 'Hospital day ratio to US avg',
         0.0),
        ('AMI_SCORE', 'Myocardial infarction score',
         'HOSP_DAYS_P_DCD', 'Hospital days/dcd',
         0.0),
        ('AMI_SCORE', 'Myocardial infarction score',
         'HOSP_DAYS_P_DCD2', 'Hospital days/dcd, end-of-life',
         0.0),
    ]
