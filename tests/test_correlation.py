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

import crosscat.LocalEngine

import bayeslite
import bayeslite.crosscat

def test_correlation():
    with bayeslite.bayesdb_open() as bdb:
        cc = crosscat.LocalEngine.LocalEngine(seed=0)
        ccme = bayeslite.crosscat.CrosscatMetamodel(crosscat)
        bayeslite.bayesdb_register_metamodel(bdb, ccme)
        bdb.sql_execute('create table u(id, c0, c1, cx, n0, n1, nx)')
        bdb.execute('''
            create generator u_cc for u using crosscat (
                c0 CATEGORICAL,
                c1 CATEGORICAL,
                cx CATEGORICAL,
                n0 NUMERICAL,
                n1 NUMERICAL,
                nx NUMERICAL
            )
        ''')
        assert list(bdb.execute('estimate pairwise correlation from u_cc'
                ' where name0 < name1')) == \
            [
                (1, 'c0', 'c1', None),
                (1, 'c0', 'cx', None),
                (1, 'c0', 'n0', None),
                (1, 'c0', 'n1', None),
                (1, 'c0', 'nx', None),
                (1, 'c1', 'cx', None),
                (1, 'c1', 'n0', None),
                (1, 'c1', 'n1', None),
                (1, 'c1', 'nx', None),
                (1, 'cx', 'n0', None),
                (1, 'cx', 'n1', None),
                (1, 'cx', 'nx', None),
                (1, 'n0', 'n1', None),
                (1, 'n0', 'nx', None),
                (1, 'n1', 'nx', None),
            ]
        bdb.sql_execute('create table t(id, c0, c1, cx, n0, n1, nx)')
        data = [
            ('foo', 'quagga', 'x', 0, 42, 0),
            ('bar', 'eland', 'x', 87, 3.1415926, 0),
            ('baz', 'caribou', 'x', 92.1, 73, 0),
        ] * 10
        for i, row in enumerate(data):
            row = (i + 1,) + row
            bdb.sql_execute('insert into t values (?,?,?,?,?,?,?)', row)
        bdb.execute('''
            create generator t_cc for t using crosscat (
                c0 CATEGORICAL,
                c1 CATEGORICAL,
                cx CATEGORICAL,
                n0 NUMERICAL,
                n1 NUMERICAL,
                nx NUMERICAL
            )
        ''')
        assert list(bdb.execute('estimate pairwise correlation from t_cc'
                ' where name0 < name1')) == \
            [
                (2, 'c0', 'c1', 1.),
                (2, 'c0', 'cx', 0.),
                (2, 'c0', 'n0', 1.),
                (2, 'c0', 'n1', 1.),
                (2, 'c0', 'nx', 0.),
                (2, 'c1', 'cx', 0.),
                (2, 'c1', 'n0', 1.),
                (2, 'c1', 'n1', 1.),
                (2, 'c1', 'nx', 0.),
                (2, 'cx', 'n0', 0.),
                (2, 'cx', 'n1', 0.),
                (2, 'cx', 'nx', 1.),
                (2, 'n0', 'n1', 0.00024252650945111442),
                (2, 'n0', 'nx', 0.),
                (2, 'n1', 'nx', 0.),
            ]
