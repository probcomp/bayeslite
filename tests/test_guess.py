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

import itertools

from bayeslite.guess import bayesdb_guess_stattypes

def test_guess_stattypes():
    n = ['a', 'b']
    a_z = range(ord('a'), ord('z') + 1)
    rows = [[chr(c), 0] for c in a_z]
    assert bayesdb_guess_stattypes(n, rows) == ['key', 'categorical']
    rows = [[chr(c), 0] for c in a_z] + [['q', 0]]
    assert bayesdb_guess_stattypes(n, rows) == ['categorical', 'categorical']
    rows = [[0, chr(c)] for c in a_z]
    assert bayesdb_guess_stattypes(n, rows) == ['categorical', 'key']
    rows = [[0, chr(c)] for c in a_z] + [[0, 'k']]
    assert bayesdb_guess_stattypes(n, rows) == ['categorical', 'categorical']
    rows = [[chr(c), i] for i, c in enumerate(a_z)]
    assert bayesdb_guess_stattypes(n, rows) == ['key', 'numerical']
    rows = [[chr(c) + chr(d), isqrt(i)] for i, (c, d)
        in enumerate(itertools.product(a_z, a_z))]
    assert bayesdb_guess_stattypes(n, rows) == ['key', 'numerical']
    rows = [[chr(c) + chr(d) + chr(e), isqrt(i)] for i, (c, d, e)
        in enumerate(itertools.product(a_z, a_z, a_z))]
    assert bayesdb_guess_stattypes(n, rows) == ['key', 'categorical']
    rows = [[i, chr(c)] for i, c in enumerate(a_z)]
    assert bayesdb_guess_stattypes(n, rows) == ['key', 'categorical']
    rows = [[isqrt(i), chr(c) + chr(d)] for i, (c, d)
        in enumerate(itertools.product(a_z, a_z))]
    assert bayesdb_guess_stattypes(n, rows) == ['numerical', 'key']
    rows = [[isqrt(i), chr(c) + chr(d) + chr(e)] for i, (c, d, e)
        in enumerate(itertools.product(a_z, a_z, a_z))]
    assert bayesdb_guess_stattypes(n, rows) == ['categorical', 'key']

def isqrt(n):
    x = n
    y = (x + 1)//2
    while y < x:
        x = y
        y = (x + n//x)//2
    return x
