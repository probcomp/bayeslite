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

import test_core

from stochastic import stochastic


@stochastic(max_runs=2, min_passes=1)
def test_mutinf_smoke(seed):
    with test_core.t1(seed=seed) as (bdb, population_id, _generator_id):
        def checkmi(n, q, *p):
            i = 0
            for r in bdb.sql_execute(q, *p):
                assert len(r) == 1
                assert isinstance(r[0], float) or (r[0] == 0)
                i += 1
            assert i == n, '%r =/= %r' % (i, n)

        bdb.execute('initialize 10 models for p1_cc')
        checkmi(10, '''
            select mi from bql_mutinf
                where population_id = ?
                    and target_vars = '[1]'
                    and reference_vars = '[2]'
        ''', (population_id,))

        bdb.execute('initialize 11 models if not exists for p1_cc')
        checkmi(11, '''
            select mi from bql_mutinf
                where population_id = ?
                    and target_vars = '[1]'
                    and reference_vars = '[2]'
                    and conditions = '{"3": 42}'
        ''', (population_id,))

        bdb.execute('initialize 12 models if not exists for p1_cc')
        checkmi(12, '''
            select mi from bql_mutinf
                where population_id = ?
                    and target_vars = '[1]'
                    and reference_vars = '[2]'
                    and nsamples = 2
        ''', (population_id,))

        bdb.execute('initialize 13 models if not exists for p1_cc')
        checkmi(13, '''
            select mi from bql_mutinf
                where population_id = ?
                    and target_vars = '[1]'
                    and reference_vars = '[2]'
                    and conditions = '{"3": 42}'
                    and nsamples = 2
        ''', (population_id,))
