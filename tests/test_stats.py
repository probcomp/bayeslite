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
    assert relerr(stats.chi2_sf(0,12), 1.) < .05
    assert relerr(stats.chi2_sf(0,6), 1.) < .05
    assert relerr(stats.chi2_sf(0,130), 1.) < .05

    # Test x < 1, x >= df against reference values.
    assert relerr(stats.chi2_sf(.8,.1), .0357175) < .05
    assert relerr(stats.chi2_sf(.6,.6), .2730426) < .05
    assert relerr(stats.chi2_sf(.1,.05), .0602823) < .05

    # Test x >= 1, x <= df against reference values.
    assert relerr(stats.chi2_sf(9,12), .7029304) < .05
    assert relerr(stats.chi2_sf(1.9,3), .5934191) < .05
    assert relerr(stats.chi2_sf(1,4.2), .9238371) < .05

    # Test x >= 1, x > df against reference values.
    assert relerr(stats.chi2_sf(8,7), .3325939) < .05
    assert relerr(stats.chi2_sf(3.9,1), .0482861) < .05
    assert abserr(stats.chi2_sf(193,121), .3464377e-4) < .05

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
    assert relerr(stats.f_sf(0,1,12) , 1) < .05
    assert relerr(stats.f_sf(0,6,0.5) , 1) < .05
    assert relerr(stats.f_sf(0,130,121) , 1) < .05
    
    # Survival of x < 0 should be 1.
    assert relerr(stats.f_sf(-1,1,12) , 1) < .05
    assert relerr(stats.f_sf(-100,6,0.5) , 1) < .05
    assert relerr(stats.f_sf(-0.02,130,121) , 1) < .05

    # Test against reference values.
    assert relerr(stats.f_sf(1,12,8), .5173903) < .05
    assert relerr(stats.f_sf(1.9,1,3), .2618860) < .05
    assert relerr(stats.f_sf(1,100,100), .5000000) < .05
    assert relerr(stats.f_sf(19,14,1), .1781364) < .05
    assert relerr(stats.f_sf(0.76,23,15),  .7306588) < .05
    assert relerr(stats.f_sf(4.3,1,12), .0602978) < .05
    assert relerr(stats.f_sf(1.1,2,1), .5590169) < .05
    assert relerr(stats.f_sf(8,2,2), .1111111) < .05
    assert relerr(stats.f_sf(0.2,432,123), .9999999) < .05
    assert relerr(stats.f_sf(0.8,432,123), .9452528) < .05
    assert relerr(stats.f_sf(10,5,3), .0434186) < .05
    
    # Test against reference very close to zero.
    assert abserr(stats.f_sf(11,19,4), .0158130) < .01
    assert abserr(stats.f_sf(14,9,6), .0022310) < .01
    assert abserr(stats.f_sf(200,432,123), .1458691e-112) < .01
    assert abserr(stats.f_sf(29,23,29), .2489256e-13) < .01
    assert abserr(stats.f_sf(31,11,13), .1656276e-06) < .01
    assert abserr(stats.f_sf(18,14,12), .6424023e-5) < .01

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
    
    # Test against reference very close to zero.
    assert abserr(stats.chi2_sf(193,121), .346437e-4) < .01
