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

"""Tests for approximate estimators."""

import numpy as np

from bayeslite import bayesdb_open

from stochastic import stochastic


@stochastic(max_runs=2, min_passes=1)
def test_mutinf__ci_slow(seed):
    with bayesdb_open(':memory:', seed=seed) as bdb:
        npr = bdb.np_prng
        bdb.sql_execute('create table t(x, y, z)')
        D0_XY = npr.multivariate_normal([10,10], [[0,1],[2,0]], size=50)
        D1_XY = npr.multivariate_normal([0,0], [[0,-1],[2,0]], size=50)
        D_XY = np.concatenate([D0_XY, D1_XY])
        D_Z = npr.multivariate_normal([5], [[0.5]], size=100)
        D = np.hstack([D_XY, D_Z])
        for d in D:
            bdb.sql_execute('INSERT INTO t VALUES(?,?,?)', d)
        bdb.execute(
            'create population p for t(x numerical; y numerical; z numerical)')
        bdb.execute('create metamodel m for p with baseline crosscat')
        bdb.execute('initialize 10 models for m')
        bdb.execute('analyze m for 10 iterations wait (optimized; quiet)')
        vars_by_mutinf = bdb.execute('''
            estimate * from variables of p
                order by probability of (mutual information with x > 0.1) desc
        ''').fetchall()
        vars_by_depprob = bdb.execute('''
            estimate * from variables of p
                order by dependence probability with x desc
        ''').fetchall()
        assert vars_by_mutinf == [('x',), ('y',), ('z',)]
        assert vars_by_depprob == [('x',), ('y',), ('z',)]
