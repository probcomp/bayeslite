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

from bayeslite.util import cursor_value

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
