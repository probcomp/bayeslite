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
import numpy
from bayeslite.util import float_sum

# Constants for numerical integration.
MACHEP = 2**-56
MAXNUM = 2**127
BIG = 4.503599627370496e15
BIGINV = 2.22044604925031308085e-16


def arithmetic_mean(array):
    """Computes the arithmetic mean of elements of `array`.

    """
    return float_sum(array) / len(array)


def pearsonr(a0, a1):
    """Computes the Pearson r correlation coefficient of two samples.
    https://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient

    :param list<float> a0: Observations of the first random variable.
    :param list<float> a1: Observations of the second random variable.

    :return: Perason r correlation coefficient of samples `a0` and `a1`.
    :rtype: float
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
    """Computes the sign of `x`.
    
    :param float x: Argument to signum.
    
    :return: Sign of `x`: (``-1 if x<0, 0 if x=0, +1 if x>0``).
    :rtype: int
    """
    if x < 0:
        return -1
    elif 0 < x:
        return +1
    else:
        return 0


def chi2_contingency(contingency, correction=None):
    """Computes observed Pearson Chi2 test statistic for a test of independence 
    on a contingency table.
    http://en.wikipedia.org/wiki/Pearson%27s_chi-squared_test#Test_of_independence

    :param list<list> contingency: 2D table of observed frequencies. The
    dimensions must be M by N, where M (resp N) is the number of discrete 
    values taken by the first (resp second) random variable.
    
    :param boolean correction: If ``True``, moves each observation count in the
    direction of the expectation by 1/2.
 
    :return: The observed Pearson chi2 test statistic on the `contingency` table.
    :rtype: float
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
    """Computes observed F test statistic for a one-way analysis of variance
    (ANOVA).
    https://en.wikipedia.org/wiki/F-test#Multiple-comparison_ANOVA_problems

    :param list<list> groups: List of lists of the observed values of each 
    group. The outer list must length equal to the number of groups.
    
    :return: The observed F test statistic on `groups`.
    :rtype: float
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


def t_cdf(x, df):
    """Approximate cumulative distribution function for Student's t probability
    distribution.
    ``t_cdf(x,df) = P(T_df < x)``

    :param float x: argument to the survival function, must be positive
    :param float df: degrees of freedom of the chi2 distribution
 
    :return: The area from negative infinity to `x` under the t probability
        distribution with degrees of freedom `df`.
    :rtype: float
    """
    if df <= 0:
        raise ValueError("Degrees of freedom must be positive")
    if x == 0:
        return 0.5
    
    MONTE_CARLO_SAMPLES = 1e5
    T = numpy.random.standard_t(df, size = MONTE_CARLO_SAMPLES)
    p = numpy.sum(T < x) / MONTE_CARLO_SAMPLES
    return p


def chi2_sf(x, df):
    """Approximate survival function (tail) for the chi2 probability 
    distribution.
    ``chi2_sf(x, df) = P(X_df > x)``

    :param float x: argument to the survival function, must be positive
    :param float df: degrees of freedom of the chi2 distribution
 
    :return: The area from `x` to infinity under the chi2 probability
        distribution with degrees of freedom df.
    :rtype: float
    """
    if df <= 0:
        raise ValueError("Degrees of freedom must be positive.")
    if x <= 0:
        return 1.0
    if x < 1.0 or x < df:
        return 1.0 - _igam(0.5 * df, 0.5 * x)
    return _igamc(0.5 * df, 0.5 * x)


def _igamc(a, x):
    """Computes the complemented incomplete Gamma integral.
    The function is defined by:

                                inf.
                                   -
                          1       | |  -t  a-1
                    =   -----     |   e   t   dt.
                         -      | |
                        | (a)    -
                                    x

    :param float a: exponent in the integral, must be positive.
    :param float x: lower limit of the integral, must be positive.
 
    :return: The area from `x` to infnity under the aforementioned integral.
    :rtype: float
    """
    # Compute  x**a * exp(-x) / Gamma(a).
    ax = math.exp(a * math.log(x) - x - math.lgamma(a))

    # Continued fraction.
    y = 1.0 - a
    z = x + y + 1.0
    c = 0.0
    pkm2 = 1.0
    qkm2 = x
    pkm1 = x + 1.0
    qkm1 = z * x
    ans = pkm1 / qkm1
    while True:
        c += 1.0
        y += 1.0
        z += 2.0
        yc = y * c
        pk = pkm1 * z - pkm2 * yc
        qk = qkm1 * z - qkm2 * yc
        if qk != 0:
            r = pk / qk
            t = abs((ans - r) / r)
            ans = r
        else:
            t = 1.0
        pkm2 = pkm1
        pkm1 = pk
        qkm2 = qkm1
        qkm1 = qk
        if abs(pk) > BIG:
                pkm2 *= BIGINV
                pkm1 *= BIGINV
                qkm2 *= BIGINV
                qkm1 *= BIGINV
        if t <= MACHEP:
            return ans * ax


def _igam(a, x):
    """Computes the left tail of incomplete Gamma function.
    The function is defined by:

                 inf.      k
          a  -x   -       x
         x  e     >   ----------
                  -     -
                k=0   | (a+k+1)
    
    :param float a: exponent in the integral, must be positive.
    :param float x: upper limit of the integral, must be positive.
 
    :return: The area from 0 to `x` under the aforementioned integral.
    :rtype: float
    """

    # Compute  x**a * exp(-x) / Gamma(a).
    ax = math.exp(a * math.log(x) - x - math.lgamma(a))

    # Power series.
    r = a
    c = 1.0
    ans = 1.0
    while True:
        r += 1.0
        c *= x / r
        ans += c
        if c / ans <= MACHEP:
            return ans * ax / a
