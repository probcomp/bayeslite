#!/usr/bin/env python

import itertools
import math
import numbers
from numpy.random import RandomState
from scipy import stats

def nones(n):
    """Returns an iterator which generates None n times."""
    return itertools.repeat(None, n)


def lbeta(m, n):
    """Return log(Beta(m,n))"""
    return math.lgamma(m) + math.lgamma(n) - math.lgamma(m + n)


def failprob_threshold(observed, ns, threshold):
    """Takes ([float] observed, int ns, float threshold)

    observed: iid numeric samples of some test statistic,
    ns: number of iid samples to be drawn when the test statistic is being used
        in an integration test,
    threshold: desired maximum probability that "ns" iid samples from
    the same underlying distribution "observed" was drawn from are all less
    than some bound "x".

    Returns (float p, float x). Return value "p" is the posterior probability
    of drawing "ns" samples from the underlying distribution which are all less
    than "x" is less than "threshold". "x" is the ML estimate of the
    threshold**(1/ns) quantile

    """

    # Type checking
    if not all(isinstance(d, numbers.Number) for d in observed):
        raise ValueError('observed is not a list of numeric values')
    if (threshold > 1) or (threshold < 0):
        raise ValueError('threshold is not a probability')
    if (round(ns) != ns) or (ns < 1):
        raise ValueError('ns is not a natural number')

    # Compute the quantile which should be tested for in each subtest
    observed = sorted(observed)
    sub_threshold = threshold**(1. / ns)
    mlxidx = int(len(observed) * sub_threshold)
    mlx = observed[mlxidx]
    if observed.count(mlx) > 1:
        # If mlx occurs more than once, it's likely that it contains
        # non-trivial probability mass. If the target quantile lies within that
        # mass then the key assumption of this approach (i.e. that P(y<mlx) is
        # approximately sub_threshold) has broken down and a different
        # threshold should be chosen.

        # Note: This can also happen if you've got repeated "random" state in
        # your test statistic.
        raise ValueError('Requested quantile may lie in Dirac delta fn')

    # Compute the observed counts below and above the threshold mlx
    below, above = max(0, mlxidx - 1), len(observed) - mlxidx - 1

    # We have observed "below" samples less than or equal to mlx, "above"
    # samples above it. If we treat these as observations of a binomial, the
    # posterior on P(y<mlx) is a Beta(below+1,above+1) distribution, call it
    # PB. The posterior probability of "ns" iid samples less than or equal to
    # mlx is the integral over the unit interval of (q**ns)*PB(q,1-q), which
    # is, modulo a constant, the integrand of the density
    # Beta(below+ns+1,above+1). I.e.
    lpfail = lbeta(below + ns + 1, above + 1) - lbeta(below + 1, above + 1)
    return math.exp(lpfail), mlx


def test_failprob_threshold(prngstate=0):
    """Sanity check on failprob_threshold: Verify, for a relatively large failure
    probability, that the failure threshold it returns for a simple test
    statistic actually results in failures at approximately the right
    frequency.

    """
    prngstate = RandomState(prngstate)

    def sample(n):
        return stats.norm(0, 1).rvs(n, random_state=prngstate)

    target_prob, test_sample_size = 1e-1, 6
    prob, thresh = failprob_threshold(
        sample(1000), test_sample_size, target_prob)
    samples = [all(v < thresh for v in sample(test_sample_size))
               for _ in nones(int(100 / target_prob))]
    assert 50 < samples.count(True) < 200


def compute_sufficiently_stringent_threshold(generator, ns, threshold):
    """generator is a function which takes no arguments and returns a float.

    Its return values are assumed to be iid. ns and threshold are as in
    sufficiently_stringent_p. Returns a float x such that the
    probability of drawing ns samples from generator which are all less
    than x is less than threshold, a float probfail which is the actual
    estimated probability of ns samples less than x, and an int which is
    the number of samples drawn to make the estimate.

    "generator" is expected to look after its own stochastic state. You can
    conveniently make such a generator using functools.partial or
    utils.entropy.seeded. E.g.

    from test_normal import normal_prior_test_statistic
    from utils.stats import seeded
    ntest_statistic = seeded(normal_prior_test_statistic, 17)

    """
    batchsize = int(threshold**(-1. / ns)) + 1
    observed = []
    while True:
        observed.extend(generator() for _ in nones(batchsize))
        probfail, x = failprob_threshold(observed, ns, 0.9 * threshold)
        if probfail < threshold:
            return x, probfail, len(observed)
        print x, probfail, len(observed)


class MultipleTestStatisticFailures(RuntimeError):
    """Raised when a test statistic is too low too many times."""

    def __init__(self, generator, ns, threshold, statistics):
        self.generator, self.ns, self.threshold = generator, ns, threshold
        self.statistics = statistics


def check_generator(generator, ns, threshold, probfail):
    """Check that "generator" is not producing absurdly low values.

    generator: the test statistic

    ns: The number of times "generator" is allowed to return a value less
        than "threshold" before failure is reported.

    probfail: A bound on probability of failure being reported.

    These values should be computed using
    'compute_sufficiently_stringent_threshold'

    """

    statistics = []
    for numfailures in range(ns):
        statistics.append(generator())
        if statistics[-1] >= threshold:
            return numfailures
    raise MultipleTestStatisticFailures(generator, ns, threshold, statistics),\
        ('%s has been less than %.3g %i times, the probability of which was '
         ' estimated to happen one time in %.3g') % (
             generator, threshold, ns, 1 / probfail)
