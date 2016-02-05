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

"""Miscellaneous statistics utilities."""

import math
import numpy

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
    a0 = numpy.array(a0)
    a1 = numpy.array(a1)
    m0 = numpy.mean(a0)
    m1 = numpy.mean(a1)
    num = numpy.sum((a0 - m0)*(a1 - m1))
    den0_sq = numpy.sum((a0 - m0)**2)
    den1_sq = numpy.sum((a1 - m1)**2)
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
    contingency = numpy.array(contingency, dtype=int, ndmin=2)
    assert contingency.ndim == 2
    n = float(numpy.sum(contingency))
    n0 = contingency.shape[0]
    n1 = contingency.shape[1]
    assert 0 < n0
    assert 0 < n1
    p0 = numpy.sum(contingency, axis=1)/n
    p1 = numpy.sum(contingency, axis=0)/n
    expected = n * numpy.outer(p0, p1)
    return numpy.sum(((contingency - expected)**2)/expected)

def f_oneway(groups):
    """F-test statistic for one-way analysis of variance (ANOVA).

    https://en.wikipedia.org/wiki/F-test#Multiple-comparison_ANOVA_problems

    ``groups[i][j]`` is jth observation in ith group.
    """
    # We turn groups into a list of numpy 1d-arrays, not into a numpy
    # 2d-array, because the lengths are heterogeneous.
    groups = [numpy.array(group, dtype=float, ndmin=1) for group in groups]
    assert all(group.ndim == 1 for group in groups)
    K = len(groups)
    N = sum(len(group) for group in groups)
    means = [numpy.mean(group) for group in groups]
    overall_mean = numpy.sum(numpy.sum(group) for group in groups) / N
    bgv = numpy.sum(len(group) * (mean - overall_mean)**2 / (K - 1)
        for group, mean in zip(groups, means))
    wgv = numpy.sum(numpy.sum((group - mean)**2)/float(N - K)
        for group, mean in zip(groups, means))
    # Special cases for which Python wants to raise an error rather
    # than giving the sensible IEEE 754 result.
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
