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

import pytest

from stochastic import StochasticError
from stochastic import stochastic

class Quagga(Exception):
    pass

@stochastic(max_runs=1, min_passes=1)
def _test_fail(seed):
    raise Quagga

@stochastic(max_runs=1, min_passes=1)
def _test_pass(_seed):
    pass

passthenfail_counter = 0
@stochastic(max_runs=2, min_passes=1)
def _test_passthenfail(seed):
    global passthenfail_counter
    passthenfail_counter += 1
    passthenfail_counter %= 2
    if passthenfail_counter == 0:
        raise Quagga

failthenpass_counter = 0
@stochastic(max_runs=2, min_passes=1)
def _test_failthenpass(seed):
    global failthenpass_counter
    failthenpass_counter += 1
    failthenpass_counter %= 2
    if failthenpass_counter == 1:
        raise Quagga

@stochastic(max_runs=2, min_passes=1)
def _test_failthenfail(seed):
    raise Quagga

@stochastic(max_runs=1, min_passes=1)
def test_stochastic(seed):
    with pytest.raises(StochasticError):
        _test_fail()
    try:
        _test_fail()
    except StochasticError as e:
        assert isinstance(e.excvalue, Quagga)
    with pytest.raises(Quagga):
        _test_fail(seed)
    _test_pass()
    _test_pass(seed)
    _test_passthenfail()
    with pytest.raises(Quagga):
        _test_passthenfail(seed)
    _test_failthenpass()
    with pytest.raises(Quagga):
        _test_failthenpass(seed)
    with pytest.raises(StochasticError):
        _test_failthenfail()
    with pytest.raises(Quagga):
        _test_failthenfail(seed)
