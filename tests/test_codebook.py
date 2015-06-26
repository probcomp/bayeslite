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

import csv
import os
import pytest
import tempfile

import crosscat.LocalEngine

import bayeslite
import bayeslite.crosscat


dummy_data = iter(['kerberos,age,city',
    'rockerboi,18,RIO',
    'starstruck,19,SF',
    'watergirl,19,SF',
    'kiki,21,RIO'])

dummy_codebook = '''name,shortname,description,value_map
kerberos,student identifier,unique identifer,NaN
age,student age,years,NaN
city,hometown,student hometown,"{""RIO"":""Rio de Janeiro""\
,""DC"":""Washington DC"",""SF"":""San Francisco"",""LA"":\
""Los Angeles""}"'''

def test_codebook_value_map():
    '''
    A categorical column in crosscat can only take on a fixed number of values
    v1, v2, ..., v3.  In this test, we have a categorical column called
    `city` which takes on values `RIO, LA, SF, DC` as specified in the codebook
    value map.

        INITIALIZE dummy table with only RIO and SF appearing in dataset
        ANALYZE dummy_cc
        INSERT rows with `city` names `LA` and `DC`
        ANALYZE dummy_cc
        SIMULATE specifying `city` = `LA` (throws KeyError)
    '''
    
    with bayeslite.bayesdb_open() as bdb:
        cc = crosscat.LocalEngine.LocalEngine(seed=0)
        ccme = bayeslite.crosscat.CrosscatMetamodel(cc)
        bayeslite.bayesdb_register_metamodel(bdb, ccme)
        
        bayeslite.bayesdb_read_csv(bdb,'dummy', dummy_data,
            header=True,create=True)

        tempbook = tempfile.NamedTemporaryFile()
        tempbook.write(dummy_codebook)
        tempbook.flush()
        bayeslite.bayesdb_load_codebook_csv_file(bdb, 'dummy', tempbook.name)

        bdb.execute('''
            CREATE GENERATOR dummy_cc FOR dummy
                USING crosscat(
                    GUESS(*),
                    kerberos IGNORE,
                    age NUMERICAL,
                    city CATEGORICAL
                )
        ''')
        bdb.execute('INITIALIZE 10 MODELS FOR dummy_cc')
        bdb.execute('ANALYZE dummy_cc FOR 20 ITERATIONS WAIT')
        bdb.execute('SIMULATE age FROM dummy_cc GIVEN city = RIO LIMIT 5')
        bdb.sql_execute('''
            INSERT INTO dummy (kerberos, age, city) VALUES
                ('jackie', 18, 'LA'), ('rocker', 22, 'DC')
        ''')
        bdb.execute('ANALYZE dummy_cc FOR 20 ITERATIONS WAIT')
        c = bdb.sql_execute('SELECT * FROM dummy')
        with pytest.raises(KeyError):
            bdb.execute('SIMULATE age FROM dummy_cc GIVEN city = LA LIMIT 5')
