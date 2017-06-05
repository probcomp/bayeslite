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

import pytest

import bayeslite
import bayeslite.core as core


def test_case():
    pytest.xfail(reason='Github issue #546')
    with bayeslite.bayesdb_open(':memory:') as bdb:
        bdb.sql_execute('create table t(x,Y)')
        bdb.sql_execute('insert into t values(1,2)')
        bdb.sql_execute('insert into t values(3,4)')
        bdb.sql_execute('insert into t values(1,4)')
        bdb.sql_execute('insert into t values(2,2)')
        bdb.execute('create population p for t(guess(*))')
        population_id = core.bayesdb_get_population(bdb, 'p')
        assert core.bayesdb_variable_names(bdb, population_id, None) == \
            ['x', 'Y']
