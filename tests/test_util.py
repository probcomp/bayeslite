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

import math

import bayeslite.util as util

def test_gauss_suff_stats():
    # High mean, tiny variance would lead to catastrophic cancellation
    # in a naive implementation that maintained the sum of squares.
    big = 400
    small = 0.0000001
    data = [big - small, big, big + small]
    true_sigma = math.sqrt(2 * small**2 / 3)
    (ct, mean, sigma) = util.gauss_suff_stats(data)
    print sigma, true_sigma
    assert ct == 3
    assert mean == big
    assert abs(sigma - true_sigma)/true_sigma < 1e-5
