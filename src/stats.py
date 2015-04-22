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
