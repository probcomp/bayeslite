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
from bayeslite.metamodels.crosscat import CrosscatMetamodel
import bayeslite.read_csv as read_csv
import crosscat.LocalEngine

root = os.path.dirname(os.path.abspath(__file__))
dha_csv = os.path.join(root, 'dha.csv')
dha_models = os.path.join(root, 'dha_models.pkl.gz')
dha_codebook = os.path.join(root, 'dha_codebook.csv')

def test_legacy_models():
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

if False:
    bql = '''
        SELECT gc0.name, gc0.shortname, gc1.name, gc1.shortname, e.value
            FROM (ESTIMATE DEPENDENCE PROBABILITY FROM PAIRWISE COLUMNS OF dha
                    WHERE name0 != \'name\' AND name1 != \'name\'
                    ORDER BY name0 ASC, name1 ASC
                    LIMIT 10) AS e,
                bayesdb_generator AS g,
                (bayesdb_generator_column JOIN bayesdb_column USING (colno))
                    AS gc0,
                (bayesdb_generator_column JOIN bayesdb_column USING (colno))
                    AS gc1
            WHERE g.id = e.generator_id
                AND gc0.generator_id = e.generator_id
                AND gc1.generator_id = e.generator_id
    '''
    assert bdb.execute(bql).fetchall() == [
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
