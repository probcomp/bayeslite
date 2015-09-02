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
        kl_est = geweke.geweke_kl(bdb, "troll_rng", [['column', 'numerical']], \
            ['column'], [(1,0)], 2, 2, 2, 2)
        assert kl_est == (2, 0, 0)

def test_geweke_iid_gaussian():
    with bayeslite.bayesdb_open() as bdb:
        import bayeslite.metamodels.iid_gaussian as gauss
        bayeslite.bayesdb_register_metamodel(bdb, gauss.StdNormalMetamodel())
        kl_est = geweke.geweke_kl(bdb, "std_normal", \
            [['column', 'numerical']], ['column'], \
            [(1,0), (2,0)], 2, 2, 2, 2)
        assert kl_est == (2, 0, 0)

def test_geweke_nig_normal():
    with bayeslite.bayesdb_open() as bdb:
        import bayeslite.metamodels.nig_normal as normal
        nig = normal.NIGNormalMetamodel(seed=1)
        bayeslite.bayesdb_register_metamodel(bdb, nig)
        kl_est = geweke.geweke_kl(bdb, "nig_normal", \
            [['column', 'numerical']], ['column'], \
            [(1,0), (2,0)], 2, 2, 2, 2)
        assert kl_est
        assert len(kl_est) == 3
        assert kl_est[0] == 2
        assert kl_est[1] > 0 # KL should be positive
        assert kl_est[1] < 10
        assert kl_est[2] > 0 # The KL error estimate should be positive too
        assert kl_est[2] < 10

def test_geweke_nig_normal_seriously():
    # Note: The actual assertions in this test and the next one were
    # dervied heuristically by inspecting a fuller (and costlier to
    # compute) tableau of values of geweke.geweke_kl and deciding the
    # aggregate impression was "probably no bug" (resp. "definitely
    # bug").  The assertions constitute an attempt to capture the most
    # salient features that give that impression.
    with bayeslite.bayesdb_open() as bdb:
        import bayeslite.metamodels.nig_normal as normal
        nig = normal.NIGNormalMetamodel(seed=1)
        bayeslite.bayesdb_register_metamodel(bdb, nig)
        cells = [(i,0) for i in range(4)]
        for chain_ct in (0, 1, 5):
            kl_est = geweke.geweke_kl(bdb, "nig_normal", \
                [['column', 'numerical']], ['column'], cells, \
                200, 200, chain_ct, 3000)
            assert kl_est[0] == 3000
            assert kl_est[1] > 0
            assert kl_est[1] < 0.1
            assert kl_est[2] > 0
            assert kl_est[2] < 0.05

def test_geweke_catches_nig_normal_bug():
    with bayeslite.bayesdb_open() as bdb:
        import bayeslite.metamodels.nig_normal as normal
        class DoctoredNIGNormal(normal.NIGNormalMetamodel):
            def _inv_gamma(self, shape, scale):
                # We actually had a bug that amounted to this
                return float(1.0/scale) / self.prng.gammavariate(shape, 1.0)
        bayeslite.bayesdb_register_metamodel(bdb, DoctoredNIGNormal(seed=1))
        cells = [(i,0) for i in range(4)]
        for chain_ct in (0, 1, 5):
            kl_est = geweke.geweke_kl(bdb, "nig_normal", \
                [['column', 'numerical']], ['column'], cells, \
                200, 200, chain_ct, 3000)
            if chain_ct == 0:
                assert kl_est[0] == 3000
                assert kl_est[1] > 0
                assert kl_est[1] < 0.1
                assert kl_est[2] > 0
                assert kl_est[2] < 0.05
            else:
                assert kl_est[0] == 3000
                assert kl_est[1] > 5
                assert kl_est[2] > 0
                assert kl_est[2] < 4
