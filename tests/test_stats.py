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

import bayeslite.stats as stats

def relerr(expected, actual):
    """Relative error between expected and actual: ``abs((a - e)/e)``."""
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
