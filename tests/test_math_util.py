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

from bayeslite.math_util import *

def pi_cf():
    """Compute pi with a generalized continued fraction.

    The continued fraction is[1]::

                       1
        pi/4 = -------------------.
                         1^2
               1 + ---------------
                           2^2
                   3 + -----------
                             3^2
                       5 + -------
                           7 + ...

    [1] https://en.wikipedia.org/wiki/Generalized_continued_fraction#.CF.80,
    no citation given.
    """
    def contfrac():
        i = 0
        while True:
            i += 1
            yield i*i, 2*i + 1
    return 4/(1 + limit(convergents(contfrac())))

def phi_cf():
    """Compute the golden ratio phi by its continued fraction.

    The well-known continued fraction is [1; 1, 1, 1, 1, ...].
    """
    def contfrac():
        while True:
            yield 1, 1
    return 1 + limit(convergents(contfrac()))

def pi_ps():
    """Compute pi with a power series representation of arctan.

    The power series for arctan is Gregory's series::

                       z^3   z^5   z^7
        arctan z = z - --- + --- - --- + ....
                        3     5     7

    We use a Machin-like formula attributed on Wikipedia to Euler::

        pi/4 = 20 arctan(1/7) + 8 arctan(3/79).
    """
    def arctan(z):
        def seq():
            z2 = z*z
            zn = z
            d = 1
            sign = 1.
            while True:
                yield sign*zn/d
                zn *= z2
                d += 2
                sign *= -1
        return limit(partial_sums(seq()))
    return 20*arctan(1./7) + 8*arctan(3./79)

def test_misc():
    assert relerr(100., 99.) == .01
    assert relerr(math.pi, pi_cf()) < EPSILON
    assert relerr(math.pi, pi_ps()) < EPSILON
    assert relerr((1 + math.sqrt(5))/2, phi_cf()) < EPSILON

def test_logsumexp():
    inf = float('inf')
    nan = float('nan')
    with pytest.raises(OverflowError):
        math.log(sum(map(math.exp, range(1000))))
    assert relerr(999.4586751453871, logsumexp(range(1000))) < 1e-15
    assert logsumexp([]) == -inf
    assert logsumexp([-1000.]) == -1000.
    assert logsumexp([-1000., -1000.]) == -1000. + math.log(2.)
    assert relerr(math.log(2.), logsumexp([0., 0.])) < 1e-15
    assert logsumexp([-inf, 1]) == 1
    assert logsumexp([-inf, -inf]) == -inf
    assert logsumexp([+inf, +inf]) == +inf
    assert math.isnan(logsumexp([-inf, +inf]))
    assert math.isnan(logsumexp([nan, inf]))
    assert math.isnan(logsumexp([nan, -3]))

def test_logmeanexp():
    inf = float('inf')
    nan = float('nan')
    assert logmeanexp([]) == -inf
    assert relerr(992.550919866405, logmeanexp(range(1000))) < 1e-15
    assert logmeanexp([-1000., -1000.]) == -1000.
    assert relerr(math.log(0.5 * (1 + math.exp(-1.))),
            logmeanexp([0., -1.])) \
        < 1e-15
    assert relerr(math.log(0.5), logmeanexp([0., -1000.])) < 1e-15
    assert relerr(-3 - math.log(2.), logmeanexp([-inf, -3])) < 1e-15
    assert relerr(-3 - math.log(2.), logmeanexp([-3, -inf])) < 1e-15
    assert logmeanexp([+inf, -3]) == +inf
    assert logmeanexp([-3, +inf]) == +inf
    assert logmeanexp([-inf, 0, +inf]) == +inf
    assert math.isnan(logmeanexp([nan, inf]))
    assert math.isnan(logmeanexp([nan, -3]))
    assert math.isnan(logmeanexp([nan]))
