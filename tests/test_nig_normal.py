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

from bayeslite import bayesdb_open
from bayeslite import bayesdb_register_metamodel
from bayeslite.metamodels.nig_normal import NIGNormalMetamodel

def test_nig_normal_smoke():
    with bayesdb_open(':memory:') as bdb:
        bayesdb_register_metamodel(bdb, NIGNormalMetamodel())
        bdb.sql_execute('create table t(x)')
        for x in xrange(100):
            bdb.sql_execute('insert into t(x) values(?)', (x,))
        bdb.execute('create population p for t(x numerical)')
        bdb.execute('create generator g for p using nig_normal(x normal)')
        bdb.execute('initialize 1 model for g')
        bdb.execute('analyze g for 1 iteration wait')
        bdb.execute('estimate probability of x = 50 from p').fetchall()
        bdb.execute('simulate x from p limit 1').fetchall()
        bdb.execute('drop models from g')
        bdb.execute('drop generator g')
        bdb.execute('drop population p')
        bdb.execute('drop table t')
