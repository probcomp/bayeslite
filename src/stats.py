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

"""Miscellaneous statistics utilities."""

import math

from bayeslite.util import float_sum

def arithmetic_mean(array):
    """Arithmetic mean of elements of `array` in floating-point."""
    return float_sum(array) / len(array)

def pearsonr(a0, a1):
    """Return Pearson r: correlation coefficient for a sample.

    https://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient
    """
    n = len(a0)
    assert n == len(a1)
    if n == 0:
        return float('NaN')
    m0 = arithmetic_mean(a0)
    m1 = arithmetic_mean(a1)
    num = float_sum((x0 - m0)*(x1 - m1) for x0, x1 in zip(a0, a1))
    den0_sq = float_sum((x0 - m0)**2 for x0 in a0)
    den1_sq = float_sum((x1 - m1)**2 for x1 in a1)
    den = math.sqrt(den0_sq*den1_sq)
    if den == 0.0:
        # Not enough variation in at least one column.
        return float('NaN')
    r = num / den
    # Clamp r in [-1, +1] in case of floating-point error.
    r = min(r, +1.0)
    r = max(r, -1.0)
    return r

def signum(x):
    """Sign of x: -1 if x<0, 0 if x=0, +1 if x>0."""
    if x < 0:
        return -1
    elif 0 < x:
        return +1
    else:
        return 0

def chi2_contingency(contingency, correction=None):
    """Pearson chi^2 test of independence on contingency table.

    If correction=True, move each observation count in the direction
    of the expectation by 1/2.

    https://en.wikipedia.org/wiki/Pearson%27s_chi-squared_test#Test_of_independence
    """
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

def f_oneway(groups):
    """F-test in one-way analysis of variance (ANOVA).

    https://en.wikipedia.org/wiki/F-test#Multiple-comparison_ANOVA_problems
    """
    K = len(groups)
    N = sum(len(group) for group in groups)
    means = [arithmetic_mean(group) for group in groups]
    overall_mean = float_sum(x for group in groups for x in group) / N
    bgv = float_sum(len(group) * (mean - overall_mean)**2 / (K - 1)
        for group, mean in zip(groups, means))
    wgv = float_sum(float((x - mean)**2)/float(N - K)
        for group, mean in zip(groups, means)
        for x in group)
    if wgv == 0.0:
        if bgv == 0.0:
            return 0.0
        else:
            return float('+inf')
    return bgv / wgv
