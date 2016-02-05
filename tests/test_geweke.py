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

import bayeslite
import bayeslite.geweke_testing as geweke

import bayeslite.metamodels.troll_rng as troll
import bayeslite.metamodels.iid_gaussian as gauss
import bayeslite.metamodels.nig_normal as normal

def test_geweke_troll():
    with bayeslite.bayesdb_open(builtin_metamodels=False) as bdb:
        bayeslite.bayesdb_register_metamodel(bdb, troll.TrollMetamodel())
        kl_est = geweke.geweke_kl(bdb, "troll_rng", [['column', 'numerical']],
            ['column'], [(1,0)], 2, 2, 2, 2)
        assert kl_est == (2, 0, 0)

def test_geweke_iid_gaussian():
    with bayeslite.bayesdb_open(builtin_metamodels=False) as bdb:
        bayeslite.bayesdb_register_metamodel(bdb, gauss.StdNormalMetamodel())
        kl_est = geweke.geweke_kl(bdb, "std_normal",
            [['column', 'numerical']], ['column'],
            [(1,0), (2,0)], 2, 2, 2, 2)
        assert kl_est == (2, 0, 0)

def test_geweke_nig_normal():
    with bayeslite.bayesdb_open(builtin_metamodels=False) as bdb:
        nig = normal.NIGNormalMetamodel(seed=1)
        bayeslite.bayesdb_register_metamodel(bdb, nig)
        (ct, kl, error) = geweke.geweke_kl(bdb, "nig_normal",
            [['column', 'numerical']], ['column'],
            [(1,0), (2,0)], 2, 2, 2, 2)
        assert ct == 2
        assert 0 < kl and kl < 10 # KL should be positive
        assert 0 < error and error < 10 # KL error estimate too

def test_geweke_nig_normal_seriously__ci_slow():
    # Note: The actual assertions in this test and the next one were
    # dervied heuristically by inspecting a fuller (and costlier to
    # compute) tableau of values of geweke.geweke_kl and deciding the
    # aggregate impression was "probably no bug" (resp. "definitely
    # bug").  The assertions constitute an attempt to capture the most
    # salient features that give that impression.
    with bayeslite.bayesdb_open(builtin_metamodels=False) as bdb:
        nig = normal.NIGNormalMetamodel(seed=1)
        bayeslite.bayesdb_register_metamodel(bdb, nig)
        cells = [(i,0) for i in range(4)]
        for chain_ct in (0, 1, 5):
            (ct, kl, error) = geweke.geweke_kl(bdb, "nig_normal",
                [['column', 'numerical']], ['column'], cells,
                200, 200, chain_ct, 3000)
            assert ct == 3000
            assert 0 < kl and kl < 0.1
            assert 0 < error and error < 0.05

class DoctoredNIGNormal(normal.NIGNormalMetamodel):
    def _inv_gamma(self, shape, scale):
        # We actually had a bug that amounted to this
        return float(1.0/scale) / self.prng.gammavariate(shape, 1.0)

def test_geweke_catches_nig_normal_bug__ci_slow():
    with bayeslite.bayesdb_open(builtin_metamodels=False) as bdb:
        bayeslite.bayesdb_register_metamodel(bdb, DoctoredNIGNormal(seed=1))
        cells = [(i,0) for i in range(4)]
        for chain_ct in (0, 1, 5):
            (ct, kl, error) = geweke.geweke_kl(bdb, "nig_normal",
                [['column', 'numerical']], ['column'], cells,
                200, 200, chain_ct, 3000)
            if chain_ct == 0:
                assert ct == 3000
                assert 0 < kl and kl < 0.1
                assert 0 < error and error < 0.05
            else:
                assert ct == 3000
                assert kl > 5
                assert 0 < error and error < 4
