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

import bayeslite
import bayeslite.geweke_testing as geweke

def test_geweke_troll():
    with bayeslite.bayesdb_open() as bdb:
        import bayeslite.metamodels.troll_rng as troll
        bayeslite.bayesdb_register_metamodel(bdb, troll.TrollMetamodel())
        kl_est = geweke.geweke_kl(bdb, "troll_rng", [['column', 'numerical']],
            ['column'], [(1,1)], 2, 2, 2, 2)
        assert kl_est == 0

def test_geweke_iid_gaussian():
    with bayeslite.bayesdb_open() as bdb:
        import bayeslite.metamodels.iid_gaussian as gauss
        bayeslite.bayesdb_register_metamodel(bdb, gauss.StdNormalMetamodel())
        kl_est = geweke.geweke_kl(bdb, "std_normal", [['column', 'numerical']],
            ['column'], [(1,1), (1,2)], 2, 2, 2, 2)
        assert kl_est == 0
