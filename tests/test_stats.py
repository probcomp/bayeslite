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

import math
import pytest

import bayeslite.stats as stats

from bayeslite.math_util import relerr

def abserr(expected, actual):
    """Computes the absolute error between `expected` and `actual`.

    :param float expected: The expected value.
    :param float actual: The actual value.

    :return: ``abs(actual-expected)``
    :rtype: float
    """
    return abs(actual - expected)

def test_pearsonr():
    assert math.isnan(stats.pearsonr([], []))
    assert stats.pearsonr([1,2,3], [2,4,6]) == +1.0
    assert stats.pearsonr([1,2,3], [-2,-4,-6]) == -1.0
    assert stats.pearsonr([1,2,3], [6,4,2]) == -1.0
    assert stats.pearsonr([1,2,3], [+1,-1,+1]) == 0.0

def test_chi2_contingency():
    assert stats.chi2_contingency([[42]]) == 0.
    assert relerr(7.66, stats.chi2_contingency([[4,2,3], [3,16,2]])) < 0.01

def test_f_oneway():
    data = [[6,8,4,5,3,4], [8,12,9,11,6,8], [13,9,11,8,7,12]]
    assert relerr(9.3, stats.f_oneway(data)) < 0.01

def test_chi2_sf():
    # Non-positive degrees of freedom should throw an error.
    with pytest.raises(ValueError):
        stats.chi2_sf(0, 0)
    with pytest.raises(ValueError):
        stats.chi2_sf(2, -10)
    
    # Survival of x = 0 should be 1.
    assert relerr(1., stats.chi2_sf(0,12)) < .05
    assert relerr(1., stats.chi2_sf(0,6)) < .05
    assert relerr(1., stats.chi2_sf(0,130)) < .05

    # Test x < 1, x >= df against reference values.
    assert relerr(.0357175, stats.chi2_sf(.8,.1)) < .05
    assert relerr(.2730426, stats.chi2_sf(.6,.6)) < .05
    assert relerr(.0602823, stats.chi2_sf(.1,.05)) < .05

    # Test x >= 1, x <= df against reference values.
    assert relerr(.7029304, stats.chi2_sf(9,12)) < .05
    assert relerr(.5934191, stats.chi2_sf(1.9,3)) < .05
    assert relerr(.9238371, stats.chi2_sf(1,4.2)) < .05

    # Test x >= 1, x > df against reference values.
    assert relerr(.3325939, stats.chi2_sf(8,7)) < .05
    assert relerr(.0482861, stats.chi2_sf(3.9,1)) < .05
    assert relerr(.3464377e-4, stats.chi2_sf(193,121)) < .05

def test_f_sf():
    # Non-positive degrees of freedom should throw an error.
    with pytest.raises(ValueError):
        stats.f_sf(0,0,0)
    with pytest.raises(ValueError):
        stats.f_sf(2,-10,0)
    with pytest.raises(ValueError):
        stats.f_sf(2,0,-10)
    with pytest.raises(ValueError):
        stats.f_sf(2,-1,1)
    with pytest.raises(ValueError):
        stats.f_sf(2,1,-1)

    # Survival of x = 0 should be 1.
    assert relerr(1, stats.f_sf(0,1,12)) < .05
    assert relerr(1, stats.f_sf(0,6,0.5)) < .05
    assert relerr(1, stats.f_sf(0,130,121)) < .05
    
    # Survival of x < 0 should be 1.
    assert relerr(1, stats.f_sf(-1,1,12)) < .05
    assert relerr(1, stats.f_sf(-100,6,0.5)) < .05
    assert relerr(1, stats.f_sf(-0.02,130,121)) < .05

    # Test against reference values.
    assert relerr(.5173903, stats.f_sf(1,12,8)) < .05
    assert relerr(.2618860, stats.f_sf(1.9,1,3)) < .05
    assert relerr(.5000000, stats.f_sf(1,100,100)) < .05
    assert relerr(.1781364, stats.f_sf(19,14,1)) < .05
    assert relerr(.7306588, stats.f_sf(0.76,23,15)) < .05
    assert relerr(.0602978, stats.f_sf(4.3,1,12)) < .05
    assert relerr(.5590169, stats.f_sf(1.1,2,1)) < .05
    assert relerr(.1111111, stats.f_sf(8,2,2)) < .05
    assert relerr(.9999999, stats.f_sf(0.2,432,123)) < .05
    assert relerr(.9452528, stats.f_sf(0.8,432,123)) < .05
    assert relerr(.0434186, stats.f_sf(10,5,3)) < .05
    
    # Test against reference very close to zero.
    assert abserr(.0158130, stats.f_sf(11,19,4)) < .01
    assert abserr(.0022310, stats.f_sf(14,9,6)) < .01
    assert abserr(.1458691e-112, stats.f_sf(200,432,123)) < .01
    assert abserr(.2489256e-13, stats.f_sf(29,23,29)) < .01
    assert abserr(.1656276e-06, stats.f_sf(31,11,13)) < .01
    assert abserr(.6424023e-5, stats.f_sf(18,14,12)) < .01

def test_t_cdf():
    # Non-positive degrees of freedom should throw an error.
    with pytest.raises(ValueError):
        stats.t_cdf(0,0)
    with pytest.raises(ValueError):
        stats.t_cdf(2,-10)
    
    # CDF of x = 0 should be 0.5.
    assert relerr(.5, stats.t_cdf(0,12)) < .01
    assert relerr(.5, stats.t_cdf(0,6)) < .01
    assert relerr(.5, stats.t_cdf(0,130)) < .01

    # Test against various reference values.
    assert relerr(.57484842931039226, stats.t_cdf(.8, .1)) < .05
    assert relerr(.64922051214061649, stats.t_cdf(.6, .6)) < .05
    assert relerr(.51046281131211058, stats.t_cdf(.1, .05)) < .05
    assert relerr(.99999944795492968, stats.t_cdf(9, 12)) < .05
    assert relerr(.92318422834700042, stats.t_cdf(1.9, 3)) < .05
    assert relerr(.81430689864299455, stats.t_cdf(1, 4.2)) < .05
    assert relerr(.99995442539414559, stats.t_cdf(8, 7)) < .05
    assert relerr(.92010336338282994, stats.t_cdf(3.9, 1)) < .05
    assert relerr(1.0, stats.t_cdf(193, 121)) < .05
    assert relerr(.42515157068960779, stats.t_cdf(-.8, .1)) < .05
    assert relerr(.35077948785938345, stats.t_cdf(-.6, .6)) < .05
    assert relerr(.48953718868788948, stats.t_cdf(-.1, .05)) < .05
    assert relerr(.076815771652999562, stats.t_cdf(-1.9, 3)) < .05
    assert relerr(.18569310135700545, stats.t_cdf(-1, 4.2)) < .05
    assert relerr(.17530833141010374, stats.t_cdf(-1, 7)) < .05
    assert relerr(.079896636617170003, stats.t_cdf(-3.9, 1)) < .05
    assert relerr(.30899158341328747, stats.t_cdf(-0.5, 121)) < .05
    
    # Test against reference very close to zero.
    # XXX Why are we testing chi2_sf here?
    assert relerr(.346437e-4, stats.chi2_sf(193,121)) < .01

def test_gauss_suff_stats():
    # High mean, tiny variance would lead to catastrophic cancellation
    # in a naive implementation that maintained the sum of squares.
    big = 400
    small = 0.0000001
    data = [big - small, big, big + small]
    true_sigma = math.sqrt(2 * small**2 / 3)
    (ct, mean, sigma) = stats.gauss_suff_stats(data)
    assert ct == 3
    assert mean == big
    assert relerr(true_sigma, sigma) < 1e-5
