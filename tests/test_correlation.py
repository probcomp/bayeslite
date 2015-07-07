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
        ccme = bayeslite.crosscat.CrosscatMetamodel(cc)
        bayeslite.bayesdb_register_metamodel(bdb, ccme)
        bdb.sql_execute('create table u(id, c0, c1, n0, n1)')
        bdb.execute('''
            create generator u_cc for u using crosscat (
                c0 CATEGORICAL,
                c1 CATEGORICAL,
                n0 NUMERICAL,
                n1 NUMERICAL,
            )
        ''')
        assert list(bdb.execute('estimate pairwise correlation from u_cc'
                ' where name0 < name1')) == \
            [
                (1, 'c0', 'c1', None),
                (1, 'c0', 'n0', None),
                (1, 'c0', 'n1', None),
                (1, 'c1', 'n0', None),
                (1, 'c1', 'n1', None),
                (1, 'n0', 'n1', None),
            ]
        bdb.sql_execute('create table t'
            '(id, c0, c1, cx, cy, n0, n1, nc, nl, nx, ny)')
        data = [
            ('foo', 'quagga', 'x', 'y', 0, -1, +1, 1, 0, 13),
            ('bar', 'eland', 'x', 'y', 87, -2, -1, 2, 0, 13),
            ('baz', 'caribou', 'x', 'y', 92.1, -3, +1, 3, 0, 13),
        ] * 10
        for i, row in enumerate(data):
            row = (i + 1,) + row
            bdb.sql_execute('insert into t values (?,?,?,?,?,?,?,?,?,?,?)',
                row)
        bdb.execute('''
            create generator t_cc for t using crosscat (
                c0 CATEGORICAL,
                c1 CATEGORICAL,
                cx CATEGORICAL,
                cy CATEGORICAL,
                n0 NUMERICAL,
                n1 NUMERICAL,
                nc NUMERICAL,
                nl NUMERICAL,
                nx NUMERICAL,
                ny NUMERICAL
            )
        ''')
        assert list(bdb.execute('estimate pairwise correlation from t_cc'
                ' where name0 < name1')) == \
            [
                (2, 'c0', 'c1', 1.),
                (2, 'c0', 'cx', None),
                (2, 'c0', 'cy', None),
                (2, 'c0', 'n0', 1.),
                (2, 'c0', 'n1', 1.),
                (2, 'c0', 'nc', 1.),
                (2, 'c0', 'nl', 1.),
                (2, 'c0', 'nx', None),
                (2, 'c0', 'ny', None),
                (2, 'c1', 'cx', None),
                (2, 'c1', 'cy', None),
                (2, 'c1', 'n0', 1.),
                (2, 'c1', 'n1', 1.),
                (2, 'c1', 'nc', 1.),
                (2, 'c1', 'nl', 1.),
                (2, 'c1', 'nx', None),
                (2, 'c1', 'ny', None),
                (2, 'cx', 'cy', None),
                (2, 'cx', 'n0', None),
                (2, 'cx', 'n1', None),
                (2, 'cx', 'nc', None),
                (2, 'cx', 'nl', None),
                (2, 'cx', 'nx', None),
                (2, 'cx', 'ny', None),
                (2, 'cy', 'n0', None),
                (2, 'cy', 'n1', None),
                (2, 'cy', 'nc', None),
                (2, 'cy', 'nl', None),
                (2, 'cy', 'nx', None),
                (2, 'cy', 'ny', None),
                (2, 'n0', 'n1', 0.7913965673596881),
                (2, 'n0', 'nc', 0.20860343264031164),
                (2, 'n0', 'nl', 0.7913965673596881),
                (2, 'n0', 'nx', None),
                (2, 'n0', 'ny', None),
                (2, 'n1', 'nc', 0.),
                (2, 'n1', 'nl', 1.0),
                (2, 'n1', 'nx', None),
                (2, 'n1', 'ny', None),
                (2, 'nc', 'nl', 0.),
                (2, 'nc', 'nx', None),
                (2, 'nc', 'ny', None),
                (2, 'nl', 'nx', None),
                (2, 'nl', 'ny', None),
                (2, 'nx', 'ny', None),
            ]
