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

from bayeslite.math_util import gamma_above
from bayeslite.util import float_sum

def arithmetic_mean(array):
    """Arithmetic mean of elements of `array`."""
    return float_sum(array) / len(array)

def pearsonr(a0, a1):
    """Pearson r, product-moment correlation coefficient, of two samples.

    Covariance divided by product of standard deviations.

    https://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient#For_a_sample
    """
    n = len(a0)
    assert n == len(a1)
    if n == 0:
        # No data, so no notion of correlation.
        return float('NaN')
    m0 = arithmetic_mean(a0)
    m1 = arithmetic_mean(a1)
    num = float_sum((x0 - m0)*(x1 - m1) for x0, x1 in zip(a0, a1))
    den0_sq = float_sum((x0 - m0)**2 for x0 in a0)
    den1_sq = float_sum((x1 - m1)**2 for x1 in a1)
    den = math.sqrt(den0_sq*den1_sq)
    if den == 0.0:
        # No variation in at least one column, so no notion of
        # correlation.
        return float('NaN')
    r = num / den
    # Clamp r in [-1, +1] in case of floating-point error.
    r = min(r, +1.0)
    r = max(r, -1.0)
    return r

def signum(x):
    """Sign of `x`: ``-1 if x<0, 0 if x=0, +1 if x>0``."""
    if x < 0:
        return -1
    elif 0 < x:
        return +1
    else:
        return 0

def chi2_contingency(contingency):
    """Pearson chi^2 test of independence statistic on contingency table.

    https://en.wikipedia.org/wiki/Pearson%27s_chi-squared_test#Test_of_independence
    """
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
        return ((O - E)**2)/E
    return float_sum(q(i0, i1) for i0 in range(n0) for i1 in range(n1))

def f_oneway(groups):
    """F-test statistic for one-way analysis of variance (ANOVA).

    https://en.wikipedia.org/wiki/F-test#Multiple-comparison_ANOVA_problems

    ``groups[i][j]`` is jth observation in ith group.
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
            # No variation between or within groups, so we cannot
            # ascertain any correlation between them -- it is as if we
            # had no data about the groups: every value in every group
            # is the same.
            return float('NaN')
        else:
            # Within-group variability is zero, meaning for each
            # group, each value is the same; between-group variability
            # is nonzero, meaning there is variation between the
            # groups.  So if there were zero correlation we could not
            # possibly observe this, whereas all finite F statistics
            # could be observed with zero correlation.
            return float('+inf')
    return bgv / wgv

def t_cdf(x, df):
    """Approximate CDF for Student's t distribution.

    ``t_cdf(x, df) = P(T_df < x)``
    """
    import numpy

    if df <= 0:
        raise ValueError('Degrees of freedom must be positive.')
    if x == 0:
        return 0.5

    MONTE_CARLO_SAMPLES = 1e5
    random = numpy.random.RandomState(seed=0)
    T = random.standard_t(df, size=MONTE_CARLO_SAMPLES)
    return numpy.sum(T < x) / MONTE_CARLO_SAMPLES

def chi2_sf(x, df):
    """Survival function for chi^2 distribution."""
    if df <= 0:
        raise ValueError('Nonpositive df: %f' % (df,))
    if x < 0:
        return 1.
    x = float(x)
    df = float(df)
    return gamma_above(df/2., x/2.)

def f_sf(x, df_num, df_den):
    """Approximate survival function for the F distribution.

    ``f_sf(x, df_num, df_den) = P(F_{df_num, df_den} > x)``
    """
    import numpy

    if df_num <= 0 or df_den <= 0:
        raise ValueError('Degrees of freedom must be positive.')
    if x <= 0:
        return 1.0

    MONTE_CARLO_SAMPLES = 1e5
    random = numpy.random.RandomState(seed=0)
    F = random.f(df_num, df_den, size=MONTE_CARLO_SAMPLES)
    return numpy.sum(F > x) / MONTE_CARLO_SAMPLES

def gauss_suff_stats(data):
    """Summarize an array of data as (count, mean, standard deviation).

    The algorithm is the "Online algorithm" presented in Knuth Volume
    2, 3rd ed, p. 232, originally credited to "Note on a Method for
    Calculating Corrected Sums of Squares and Products" B. P. Welford
    Technometrics Vol. 4, No. 3 (Aug., 1962), pp. 419-420.  This has
    the advantage over naively accumulating the sum and sum of squares
    that it is less subject to precision loss through massive
    cancellation.

    This version collected 8/31/15 from
    https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
    """
    n = 0
    mean = 0.0
    M2 = 0.0 # n * sigma^2

    for x in data:
        n = n + 1
        delta = x - mean
        mean = mean + delta/n
        M2 = M2 + delta*(x - mean)

    if n < 1:
        return (n, mean, 0.0)
    else:
        return (n, mean, math.sqrt(M2 / float(n)))
