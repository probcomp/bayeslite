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
import pytest

import bayeslite.stats as stats

def relerr(expected, actual):
    """Computes the absolute relative error between `expected` and `actual`.

    :param float expected: The expected value.
    :param float actual: The actual value.

    :return: ``abs((actual-expected)/expected)``
    :rtype: float
    """
    return abs((actual - expected)/expected)


def test_pearsonr():
    assert math.isnan(stats.pearsonr([], []))
    assert stats.pearsonr([1,2,3], [2,4,6]) == +1.0
    assert stats.pearsonr([1,2,3], [-2,-4,-6]) == -1.0
    assert stats.pearsonr([1,2,3], [6,4,2]) == -1.0
    assert stats.pearsonr([1,2,3], [+1,-1,+1]) == 0.0

def test_chi2_contingency():
    assert stats.chi2_contingency([[42]]) == 0.
    assert relerr(7.66, stats.chi2_contingency([[4,2,3],[3,16,2]])) < 0.01

def test_f_oneway():
    data = [[6,8,4,5,3,4],[8,12,9,11,6,8],[13,9,11,8,7,12]]
    assert relerr(9.3, stats.f_oneway(data)) < 0.01

def test_chi2_sf():
    # Non-positive degrees of freedom should throw an error.
    with pytest.raises(ValueError):
        stats.chi2_sf(0,0)
    with pytest.raises(ValueError):
        stats.chi2_sf(2,-10)
    
    # Survival of x = 0 should be 1.
    assert relerr(stats.chi2_sf(0,12) , 1) < 0.01
    assert relerr(stats.chi2_sf(0,6) , 1) < 0.01
    assert relerr(stats.chi2_sf(0,130) , 1) < 0.01

    # Test x < 1, x >= df against reference values.
    assert relerr(stats.chi2_sf(.8, .1), .0357175) < .01
    assert relerr(stats.chi2_sf(.6, .6), .2730426) < .01
    assert relerr(stats.chi2_sf(.1, .05), .0602823) < .01

    # Test x >= 1, x <= df against reference values.
    assert relerr(stats.chi2_sf(9, 12), .70293043) < .01
    assert relerr(stats.chi2_sf(1.9, 3), .59341917) < .01
    assert relerr(stats.chi2_sf(1, 4.2), .92383714) < .01

    # Test x >= 1, x > df against reference values.
    assert relerr(stats.chi2_sf(8, 7), .332593) < .01
    assert relerr(stats.chi2_sf(3.9, 1), .048286) < .01
    assert relerr(stats.chi2_sf(193, 121), .346437e-4) < .01

def test_t_cdf():
    # Non-positive degrees of freedom should throw an error.
    with pytest.raises(ValueError):
        stats.t_cdf(0,0)
    with pytest.raises(ValueError):
        stats.t_cdf(2,-10)
    
    # CDF of x = 0 should be 0.5.
    assert relerr(stats.t_cdf(0,12) , .5) < .01
    assert relerr(stats.t_cdf(0,6) , .5) < .01
    assert relerr(stats.t_cdf(0,130) , .5) < .01

    # Test against various reference values.
    assert relerr(stats.t_cdf(.8, .1), .57484842931039226) < .05
    assert relerr(stats.t_cdf(.6, .6), .64922051214061649) < .05
    assert relerr(stats.t_cdf(.1, .05), .51046281131211058) < .05
    assert relerr(stats.t_cdf(9, 12), .99999944795492968) < .05
    assert relerr(stats.t_cdf(1.9, 3), .92318422834700042) < .05
    assert relerr(stats.t_cdf(1, 4.2), .81430689864299455) < .05
    assert relerr(stats.t_cdf(8, 7), .99995442539414559) < .05
    assert relerr(stats.t_cdf(3.9, 1), .92010336338282994) < .05
    assert relerr(stats.t_cdf(193, 121), 1.0) < .05
    assert relerr(stats.t_cdf(-.8, .1), .42515157068960779) < .05
    assert relerr(stats.t_cdf(-.6, .6), .35077948785938345) < .05
    assert relerr(stats.t_cdf(-.1, .05), .48953718868788948) < .05
    assert relerr(stats.t_cdf(-1.9, 3), .076815771652999562) < .05
    assert relerr(stats.t_cdf(-1, 4.2), .18569310135700545) < .05
    assert relerr(stats.t_cdf(-1, 7), .17530833141010374) < .05
    assert relerr(stats.t_cdf(-3.9, 1), .079896636617170003) < .05
    assert relerr(stats.t_cdf(-0.5, 121), .30899158341328747) < .05
