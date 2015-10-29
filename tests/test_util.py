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

from bayeslite.math_util import relerr
from bayeslite.math_util import logmeanexp
from bayeslite.math_util import logsumexp
from bayeslite.util import cursor_value

def test_logsumexp():
    assert logsumexp([-1000.]) == -1000.
    assert logsumexp([-1000., -1000.]) == -1000. + math.log(2.)
    assert relerr(math.log(2.), logsumexp([0., 0.])) < 1e-15
    assert logsumexp([-float('inf'), 1]) == 1
    assert logsumexp([-float('inf'), -float('inf')]) == -float('inf')
    assert logsumexp([float('inf'), float('inf')]) == float('inf')
    assert math.isnan(logsumexp([float('nan'), -float('inf')]))

def test_logmeanexp():
    assert logmeanexp([-1000., -1000.]) == -1000.
    assert relerr(math.log(0.5 * (1 + math.exp(-1.))), logmeanexp([0., -1.])) \
        < 1e-15
    assert relerr(math.log(0.5), logmeanexp([0., -1000.])) < 1e-15

def test_cursor_value():
    with pytest.raises(ValueError):
        cursor_value(iter([]))
    with pytest.raises(TypeError):
        cursor_value(iter([1]))
    with pytest.raises(ValueError):
        cursor_value(iter([1, 2]))
    with pytest.raises(ValueError):
        cursor_value(iter([()]))
    with pytest.raises(ValueError):
        cursor_value(iter([(1, 2)]))
    with pytest.raises(ValueError):
        cursor_value(iter([(1, 2), ()]))
    with pytest.raises(ValueError):
        cursor_value(iter([(1, 2), 3]))
    with pytest.raises(ValueError):
        cursor_value(iter([(1, 2), (3,)]))
    with pytest.raises(ValueError):
        cursor_value(iter([(1,), (2,)]))
    with pytest.raises(ValueError):
        cursor_value(iter([(1,), (2, 3)]))
    assert cursor_value(iter([(42,)])) == 42
