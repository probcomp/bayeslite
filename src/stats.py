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

from bayeslite.util import float_sum

def arithmetic_mean(array):
    """Return the arithmetic mean of elements of ARRAY in floating-point."""
    return float_sum(array) / len(array)

def pearsonr(a0, a1):
    n = len(a0)
    assert n == len(a1)
    m0 = arithmetic_mean(a0)
    m1 = arithmetic_mean(a1)
    num = float_sum((x0 - m0)*(x1 - m1) for x0, x1 in zip(a0, a1))
    den0_root = float_sum((x0 - m0)**2 for x0 in a0)
    den1_root = float_sum((x1 - m1)**2 for x1 in a1)
    r = (num / math.sqrt(den0_root*den1_root))
    # Clamp r in [-1, +1] in case of floating-point error.
    r = min(r, +1.0)
    r = max(r, -1.0)
    return r

assert pearsonr([1,2,3], [2,4,6]) == +1.0
assert pearsonr([1,2,3], [-2,-4,-6]) == -1.0
assert pearsonr([1,2,3], [6,4,2]) == -1.0
assert pearsonr([1,2,3], [+1,-1,+1]) == 0.0

def signum(x):
    if x < 0:
        return -1
    elif 0 < x:
        return +1
    else:
        return 0

def relerr(expected, actual):
    assert abs((actual - expected)/expected)

def chi2_contingency(contingency, correction=None):
    if correction is None:
        correction is True
    assert 0 < len(contingency)
    assert all(all(isinstance(v, int) for v in row) for row in contingency)
    n = float(sum(sum(row) for row in contingency))
    n0 = len(contingency)
    n1 = len(contingency[0])
    assert all(n1 == len(row) for row in contingency)
    p0 = [float_sum(contingency[i0][i1]/n for i1 in range(n1))
        for i0 in range(n0)]
    p1 = [float_sum(contingency[i0][i1]/n for i0 in range(n0))
        for i1 in range(n1)]
    def q(i0, i1):
        O = contingency[i0][i1]
        E = n*p0[i0]*p1[i1]
        if correction:
            O += 0.5*signum(E - O)
        return ((O - E)**2)/E
    return float_sum(q(i0, i1) for i0 in range(n0) for i1 in range(n1))

assert relerr(7.66, chi2_contingency([[4,2,3],[3,16,2]])) < 0.01

def f_oneway(groups):
    K = len(groups)
    N = sum(len(group) for group in groups)
    means = [arithmetic_mean(group) for group in groups]
    overall_mean = float_sum(x for x in group for group in groups) / N
    bgv = float_sum(len(group) * (mean - overall_mean)**2 / (K - 1)
        for group, mean in zip(groups, means))
    wgv = float_sum((x - mean)**2/(N - K) for x in group
        for group, mean in zip(groups, means))
    return bgv / wgv

assert relerr(9.3,
        f_oneway([[6,8,4,5,3,4],[8,12,9,11,6,8],[13,9,11,8,7,12]])) < 0.01
