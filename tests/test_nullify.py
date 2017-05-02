# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2017, MIT Probabilistic Computing Project
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

from bayeslite import bayesdb_open
from bayeslite import bayesdb_nullify


def test_nullify():
    with bayesdb_open(':memory:') as bdb:
        bdb.sql_execute('create table t(x,y)')
        for row in [
            ['1',''],
            ['nan','foo'],
            ['2','nan'],
            ['2','""'],
            ['', ''],
        ]:
            bdb.sql_execute('insert into t values(?,?)', row)
        assert bdb.execute('select * from t').fetchall() == [
            ('1',''),
            ('nan','foo'),
            ('2','nan'),
            ('2','""'),
            ('', ''),
        ]
        assert bayesdb_nullify(bdb, 't', '') == 3
        assert bdb.execute('select * from t').fetchall() == [
            ('1',None),
            ('nan','foo'),
            ('2','nan'),
            ('2','""'),
            (None, None),
        ]
        assert bayesdb_nullify(bdb, 't', 'nan', columns=['x']) == 1
        assert bdb.execute('select * from t').fetchall() == [
            ('1',None),
            (None,'foo'),
            ('2','nan'),
            ('2','""'),
            (None, None),
        ]
        assert bayesdb_nullify(bdb, 't', 'fnord') == 0
