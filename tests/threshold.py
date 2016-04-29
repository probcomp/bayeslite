"""Framework for empirical estimation of thresholds for flaky tests.

The main entry points for this module are
`compute_sufficiently_stringent_threshold` and `check_generator`.
`compute_sufficiently_stringent_threshold` is run once to derive threshold
values, which are then used in unit tests which call `check_generator` to
verify that the thresholds are satisfied.

A `generator` is expected to look after its own stochastic state. You can
conveniently make such a generator using functools.partial. E.g.

    >>> import math, functools, numpy.random, scipy.stats
    >>> r = numpy.random.RandomState(0)
    >>> test_statistic = scipy.stats.norm(0, 1).rvs
    >>> seeded_test_statistc = partial(test_statistic, random_state=r)
    >>> t = compute_sufficiently_stringent_threshold(
                seeded_test_statistc, 10, 1e-6)
    ...
    >>> check_generator(seeded_test_statistc, 10, t.threshold, t.failprob)
    0  # Number of times the test ran before success

    # Force a failure by lying about the number of times the test needs to run
    >>> for _ in xrange(100):
            check_generator(seeded_test_statistc, 2, t.threshold, t.failprob)
    ...
    MultipleTestStatisticFailures: For 2 times in a row, <functools.partial
    object at 0x116dd7fc8> has returned a value less than -0.708. The
    probability of this was empirically estimated to be less than 1e-06,
    suggesting that the distribution of return values has changed.

"""

from collections import namedtuple
import math
import numbers


def lbeta(m, n):
    """Return log(Beta(m,n))"""
    return math.lgamma(m) + math.lgamma(n) - math.lgamma(m + n)


def isnumber(n):
    return isinstance(n, numbers.Number)


class FailProbThreshold(namedtuple('FailProbThreshold', 'fprob fthreshold')):

    """Container for return value of `failprob_threshold`. See its docstring for
    notation unspecified here.

    `fprob`: The estimated probability that `ns` samples from `D` will be less
    than `fthreshold`.

    `fthreshold`: The Maximum-Likelihood estimate, given `observed`, of the
    `threshold`**(1/ns) quantile for `D`, i.e. it's chosen so that the
    probability P(X < fthreshold | D) is approximately `threshold`**(1/ns).

    """
    pass


def failprob_threshold(observed, ns, threshold):
    """Takes ([float] observed, int ns, float threshold)

    `observed`: iid numeric samples of some test statistic following some
    distribution `D`.

    `ns`: number of iid samples to be drawn when the test statistic is being
          used in a test of a statistical method.

    `threshold`: desired maximum probability that `ns` iid samples from `D` are
    all less than some bound to be determined, `fthreshold`.

    Returns a `FailProbThreshold`, which contains the computed `fthreshold` and
    a probability `fprob` which is guaranteed to be less than `threshold`. See
    `FailProbThreshold`'s docstring for more details.

    """

    # Type checking
    if not all(isnumber(d) for d in observed):
        raise ValueError('observed is not a list of numeric values')
    if (not isnumber(threshold)) or (threshold > 1) or (threshold < 0):
        raise ValueError('threshold is not a probability')
    if (not isnumber(ns)) or (round(ns) != ns) or (ns < 1):
        raise ValueError('ns is not a natural number')

    # Compute the quantile which should be tested for in each subtest
    observed = sorted(observed)
    sub_threshold = threshold**(1. / ns)
    mlxidx = int(len(observed) * sub_threshold)
    # ...this is the estimated `threshold**(1/ns)` quantile for `D`.
    mlx = observed[mlxidx]
    if observed.count(mlx) > 1:
        # If mlx occurs more than once, it's likely that it contains
        # non-trivial probability mass. If the target quantile lies within that
        # mass then the key assumption of this approach (i.e. that P(y<mlx) is
        # approximately sub_threshold) has broken down and a different
        # threshold should be chosen.
        raise ValueError('''Requested quantile may lie in Dirac delta fn.

        Note: This may happen because of accidentally repeated "random" state
        in your test statistic.''')

    # Compute the observed counts below and above the threshold mlx
    below = max(0, mlxidx - 1)
    above = len(observed) - mlxidx - 1

    # We have observed "below" samples less than or equal to mlx, "above"
    # samples above it. If we treat these as observations of a binomial, the
    # posterior on P(y<mlx) is a Beta(below+1,above+1) distribution, call it
    # PB. The posterior probability of "ns" iid samples less than or equal to
    # mlx is the integral over the unit interval of (q**ns)*PB(q,1-q), which
    # is, modulo a constant, the integrand of the density
    # Beta(below+ns+1,above+1). I.e.
    lpfail = lbeta(below + ns + 1, above + 1) - lbeta(below + 1, above + 1)
    return FailProbThreshold(fprob=math.exp(lpfail), fthreshold=mlx)

test_threshold_fields = ['threshold', 'failprob', 'sample_size']


class TestThreshold(namedtuple('TestThreshold', test_threshold_fields)):

        """Container for return value of `compute_sufficiently_stringent_threshold`.
        See its docstring for notation unspecified here.

        `threshold`: Threshold value, below which a single instance of the
                     `generator` test statistic is to be deemed failed.

        `failprob`: Estimated maximum probability that `ns` draws from
                    `generator` will all be less than `threshold`

        `sample_size`: How many samples we had to draw to be confident that
                       `threshold` meets the requirements.

        """
        pass


def compute_sufficiently_stringent_threshold(generator, ns, maxprob):
    """Compute a failure threshold for return values of `generator`.

    `generator`: A function which takes no arguments and returns a float. Its
                 return values are assumed to be iid.

    `ns`: Number of samples to be drawn when the generator is being used as a
          test of a statistical method.

    `maxprob`: Desired maximum probability of failure in `ns` samples in a row.

    Returns a `TestThreshold`. See its docstring for details, but the value you
    care most about is `threshold`. The probability of `generator` getting a
    value less than this `ns` times in a row is less than `maxprob`.

    """
    # XXX: The batch size can be computed exactly ahead of time. This is good
    # enough for now, though.
    batchsize = int(maxprob**(-1. / ns)) + 1
    observed = []
    while True:
        observed.extend(generator() for _ in xrange(batchsize))
        probfail, x = failprob_threshold(observed, ns, 0.9 * maxprob)
        if probfail < maxprob:
            return TestThreshold(x, probfail, len(observed))


class MultipleTestStatisticFailures(RuntimeError):
    """Raised when a test statistic is too low too many times."""

    def __init__(self, generator, ns, threshold, probfail, statistics):
        self.generator = generator
        self.ns = ns
        self.threshold = threshold
        self.statistics = statistics
        # Set args so that raising this gives a meaningful message
        failure_template = '''For % i times in a row, %s has returned a value
less than %.3g. The probability of this was empirically estimated to be less
than %.3g, suggesting that the distribution of return values has changed.'''
        super(MultipleTestStatisticFailures, self).__init__(
            failure_template % (ns, generator, threshold, probfail))


def check_generator(generator, ns, threshold, probfail):
    """Check that "generator" is not producing absurdly low values.

    `generator`: the test statistic

    `ns`: The number of times "generator" is allowed to return a value less
        than "threshold" before failure is reported.

    `probfail`: A bound on probability of failure being reported.

    These values should be computed using
    `compute_sufficiently_stringent_threshold`

    Returns the number of failing tests prior to succeeding.

    """

    statistics = []
    for numfailures in range(ns):
        statistics.append(generator())
        if statistics[-1] >= threshold:
            return numfailures
    raise MultipleTestStatisticFailures(generator, ns, threshold, probfail,
                                        statistics)
