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

"""Miscellaneous special math functions and analysis utilities.

This is not a general-purpose nor highly optimized math library: it
is limited to the purposes of bayeslite for modest data sets, written
for clarity and maintainability over speed.
"""

import math

EMAX = 1
while True:
    try:
        math.ldexp(1., EMAX + 1)
    except OverflowError:
        break
    else:
        EMAX += 1

EPSILON = 1.
while 1. + (EPSILON/2.) != 1.:
    EPSILON /= 2.
EPSINV = 1./EPSILON

# log 2^(emax + 1) = log 2 + log 2^emax
MAXLOG = math.log(2.) + math.log(2.**EMAX)

def relerr(expected, actual):
    """Relative error between `expected` and `actual`: ``abs((a - e)/e)``."""
    return abs((actual - expected)/expected)

def ieee_exp(x):
    try:
        return math.exp(x)
    except OverflowError:
        return float("inf")

def logsumexp(array):
    if len(array) == 0:
        return float('-inf')
    m = max(array)

    # m = +inf means addends are all +inf, hence so are sum and log.
    # m = -inf means addends are all zero, hence so is sum, and log is
    # -inf.  But if +inf and -inf are among the inputs, or if input is
    # NaN, let the usual computation yield a NaN.
    if math.isinf(m) and min(array) != -m and \
       all(not math.isnan(a) for a in array):
        return m

    # Since m = max{a_0, a_1, ...}, it follows that a <= m for all a,
    # so a - m <= 0; hence exp(a - m) is guaranteed not to overflow.
    return m + math.log(sum(math.exp(a - m) for a in array))

def logmeanexp(array):
    inf = float('inf')
    if len(array) == 0:
        # logsumexp will DTRT, but math.log(len(array)) will fail.
        return -inf

    # Treat -inf values as log 0 -- they contribute zero to the sum in
    # logsumexp, but one to the count.
    #
    # If we pass -inf values through to logsumexp, and there are also
    # +inf values, then we get NaN -- but if we had averaged exp(-inf)
    # = 0 and exp(+inf) = +inf, we would sensibly get +inf, whose log
    # is still +inf, not NaN.  So strip -inf values first.
    #
    # Can't say `a > -inf' because that excludes NaNs, but we want to
    # include them so they propagate.
    noninfs = [a for a in array if not a == -inf]

    # probs = map(exp, logprobs)
    # log(mean(probs)) = log(sum(probs) / len(probs))
    #   = log(sum(probs)) - log(len(probs))
    #   = log(sum(map(exp, logprobs))) - log(len(logprobs))
    #   = logsumexp(logprobs) - log(len(logprobs))
    return logsumexp(noninfs) - math.log(len(array))

def logavgexp_weighted(log_W, log_A):
    # Given log W_0, log W_1, ..., log W_{n-1} and log A_0, log A_1,
    # ... log A_{n-1}, compute
    #
    #   log ((W_0 A_0 + ... + W_{n-1} A_{n-1})/(W_0 + ... + W_{n-1}))
    #   = log (exp log (W_0 A_0) + ... + exp log (W_{n-1} A_{n-1}))
    #     - log (exp log W_0 + ... + exp log W_{n-1})
    #   = log (exp (log W_0 + log A_0) + ... + exp (log W_{n-1} + log A_{n-1}))
    #     - log (exp log W_0 + ... + exp log W_{n-1})
    #   = logsumexp (log W_0 + log A_0, ..., log W_{n-1} + log A_{n-1})
    #     - logsumexp (log W_0, ..., log W_{n-1})
    #
    # XXX Pathological cases -- infinities, NaNs.
    assert len(log_W) == len(log_A)
    return logsumexp([log_w + log_a for log_w, log_a in zip(log_W, log_A)]) \
        - logsumexp(log_W)

def continuants(contfrac):
    """Continuants of a continued fraction.

    contfrac must yield an infinite sequence (n0, d0), (n1, d1), (n2,
    d2), ..., representing the continued fraction::

                n0
        ------------------
                  n1
        d0 + -------------
                     n2
             d1 + --------
                  d2 + ...

    The kth continuant is the numerator and denominator of the
    continued fraction truncated at the kth term, i.e. with zero
    instead of the rest in the ellipsis.

    If the numerator or denominator grows large in magnitude, both are
    multiplied by the machine epsilon, leaving their quotient
    unchanged.
    """
    p0 = 1.
    q0 = 0.
    p = 0.
    q = 1.
    for n, d in contfrac:
        p0, q0, p, q = p, q, d*p + n*p0, d*q + n*q0
        if p >= EPSINV or q >= EPSINV:
            p0 *= EPSILON
            q0 *= EPSILON
            p *= EPSILON
            q *= EPSILON
        assert q != 0
        yield p, q

def convergents(contfrac):
    """Convergents of a continued fraction.

    contfrac must yield an infinite sequence (n0, d0), (n1, d1), (n2,
    d2), ..., representing the continued fraction::

                n0
        ------------------
                  n1
        d0 + -------------
                     n2
             d1 + --------
                  d2 + ...

    The kth convergent is the continued fraction truncated at the kth
    term, i.e. with zero instead of the rest in the ellipsis.
    """
    cs = continuants(contfrac)
    p0, q0 = cs.next()
    yield p0/q0
    for p, q in cs:
        if p == 0:
            break
        yield p/q
        p0, q0 = p, q
    c = p0/q0
    while True:
        yield c

def abs_summation(sequence):
    """Approximate summation of a nonnegative convergent sequence.

    The sequence is assumed to converge to zero quickly without
    oscillating -- it is truncated at the first term whose magnitude
    relative to the partial sum is bounded by the machine epsilon.
    """
    s = sequence.next()
    for t in sequence:
        assert 0 <= t
        s += t
        if (t/s) <= EPSILON:
            return s

def limit(sequence):
    """Approximate limit of a convergent sequence.

    The sequence is assumed to converge quickly without oscillating --
    it is truncated at the first term whose relative error from the
    previous term is bounded by the machine epsilon.
    """
    c0 = sequence.next()
    for c in sequence:
        if c == 0:
            return c
        if relerr(c0, c) <= EPSILON:
            return c
        c0 = c

def partial_sums(sequence):
    """Sequence of partial sums of a sequence."""
    s = sequence.next()
    yield s
    for t in sequence:
        s += t
        yield s

def gamma_below(a, x):
    """Normalized lower incomplete gamma integral.

    Equal to::

        (1/\Gamma(a)) \int_0^x e^{-t} t^{a - 1} dt.

    gamma_below is the complement of gamma_above::

        gamma_below(a, x) = 1 - gamma_above(a, x).

    As x grows, gamma_below(a, x) converges to 1.

    For x <= max(1, a), this is computed by the power series[1]::

         x^a e^-x   /           x^2            x^3           \ 
        ----------- | 1 + x + ------- + -------------- + ... |.
        a \Gamma(a) \         (a + 1)   (a + 1)(a + 2)       /

    For x > max(1, a), this is computed by ``1 - gamma_above(a, x)``.


    [1] NIST Digital Library of Mathematical Functions, Release 1.0.9
    of 2014-08-29, Eq. 8.7.1 <http://dlmf.nist.gov/8.7.E1>.

    In the NIST DLMF notation, ``gamma_below(a, x)`` is ``P(a, x)``,
    related to ``\gamma^*`` by ``\gamma^*(a, x) = x^{-a} P(a, x)``.

    To derive the power series, multiply (8.7.1) by ``x^a``, expand
    ``\Gamma(a + k + 1)`` into ``\Gamma(a) a(a + 1)...(a + k)``, and
    factor ``a \Gamma(a)`` out of the sum.
    """
    assert 0. < a               # XXX NaN?
    assert 0. <= x              # XXX NaN?
    if x == 0.:
        return 0.
    if x > max(1., a):
        return 1. - gamma_above(a, x)

    # m = exp [a log x - x - log Gamma(a)] = x^a e^-x / Gamma(a)
    w = a*math.log(x) - x - math.lgamma(a)
    if w < -MAXLOG:
        return 0.
    m = ieee_exp(w)

    def seq():
        k = 0
        t = 1.
        while True:
            yield t
            k += 1
            t *= x/(a + k)

    return (m/a)*abs_summation(seq())

def gamma_above(a, x):
    """Normalized upper incomplete gamma integral.

    Equal to::

        (1/\Gamma(a)) \int_x^\infty e^{-t} t^{a - 1} dt.

    gamma_above is the complement of gamma_below::

        gamma_above(a, x) = 1 - gamma_below(a, x).

    As x goes to zero, gamma_above(a, x) converges to 1.

    For x > max(1, a), this is computed by the continued fraction[1]::

                   1
        -----------------------,
                  1 - s
        x + -------------------
                       1
            1 + ---------------
                       2 - s
                x + -----------
                           2
                    1 + -------
                        x + ...

    which is then multiplied by ``x^a e^{-x} / \Gamma(a)``.

    For x <= max(1, a), this is computed by ``1 - gamma_below(a, x)``.

    [1] Abramowitz & Stegun, p. 263, 6.5.31
    """
    assert 0. < a               # XXX NaN?
    assert 0. <= x              # XXX NaN?
    if x <= max(1., a):
        return 1. - gamma_below(a, x)

    # m = \exp [a \log x - x - \log \Gamma(a)] = x^a e^{-x} / \Gamma(a)
    w = a*math.log(x) - x - math.lgamma(a)
    if w < -MAXLOG:
        return 0.
    m = ieee_exp(w)

    def contfrac():
        yield 1, x              # 1/(x + ...), without division.
        i = 0
        while True:
            if (i % 2) == 0:
                yield (i//2) + 1 - a, 1
            else:
                yield (i + 1)//2, x
            i += 1

    return m*limit(convergents(contfrac()))
