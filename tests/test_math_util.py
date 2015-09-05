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

from bayeslite.math_util import *

def pi_cf():
    """Compute pi with a generalized continued fraction.

    The continued fraction is[1]:

                       1
        pi/4 = -------------------
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

    The power series for arctan is Gregory's series:

                       z^3   z^5   z^7
        arctan z = z - --- + --- - --- + ...
                        3     5     7

    We use a Machin-like formula attributed on Wikipedia to Euler:

        pi/4 = 20 arctan(1/7) + 8 arctan(3/79)
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
