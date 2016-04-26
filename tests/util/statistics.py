"""Useful math routines."""

from functools import partial
import numpy as np
from numpy import sqrt, exp, inf
from numpy.random import RandomState
import scipy
import scipy.integrate
from scipy.spatial.distance import pdist, squareform
from scipy.stats.mstats import rankdata

from util import statistic_threshold as tst


def kullback_leibler(postsample, postlpdf, complpdf):
    """Estimate KL-divergence of sample (a collection of values) w.r.t. known pdf,
    complpdf, which returns the density when passed a sample. Return value is
    (estimated_kl, variance), where variance is the estimated sampling variance
    of estimated_kl. postsample is an approximate sample from the distribution
    approximately represented by postlpdf

    """
    klsamples = scipy.array([postlpdf(x) - complpdf(x) for x in postsample])
    # In general the mean and variance of log(P(x)) is not known to be finite,
    # but it will be for any distribution crosscat generates at the moment,
    # because they all have finite entropy.  Hence CLT applies.

    # Comments in response to
    # https://github.com/alxempirical/bayeslite/pull/1#discussion_r52820300 :
    # Computing these formulae using logsumexp would not afford any numerical
    # benefit, because overflows are extremely unlikely, and for any reasonable
    # sample size an underflow would imply that the KL divergence is so small
    # as to be effectively zero. The dynamic range of a float64 is from
    # approximately 1e-323 to 1e308, which is a difference in logs of about
    # 1500. Hence the largest value we could expect from the variance is about
    # 2.2e6, and it would take more values than AWS could store to oveflow the
    # sum.

    # There is a risk of precision loss due to rounding error, because this is
    # a cancelling sum, but logsumexp doesn't mitigate that risk. Assuming
    # float64's, catastrophic cancellation in the paired log terms would imply
    # a pointwise difference in probability less than 1e-8, and again we would
    # need an absurdly large number of terms for that to add up to anything
    # meaningful.
    return klsamples.mean(), klsamples.var() / sqrt(len(klsamples))


def kullback_leibler_numerical(lpdf1, lpdf2):
    """Computed the KL divergence of log pdfs by numerical integration."""
    def klf(x):
        return exp(lpdf1(x)) * (lpdf1(x) - lpdf2(x))
    return scipy.integrate.quad(klf, -inf, inf)


def kullback_leibler_check_statistic(n=100, prngstate=None):
    """Compute the cdf of the numerically estimated KL divergence w.r.t.

    the estimated sample distribution of the KL divergence estimated by
    sampling

    """
    prngstate = RandomState(prngstate)
    dist1, dist2 = scipy.stats.norm(0, 1), scipy.stats.norm(0, 2)
    lpdf1, lpdf2 = dist1.logpdf, dist2.logpdf
    exact, _ = kullback_leibler_numerical(lpdf1, lpdf2)
    estimate, std = kullback_leibler(dist1.rvs(n, random_state=prngstate),
                                     lpdf1, lpdf2)
    return scipy.stats.norm(estimate, std).sf(exact)

kl_test_stat = partial(kullback_leibler_check_statistic,
                       prngstate=RandomState(32399))

# Ran tst.compute_sufficiently_stringent_threshold(
# kullback_leibler_check_statistic, 6, 1e-20), stopped it after 32399 runs, at
# which time failprob_threshold reported that 4.516e-6 was an acceptable
# threshold, for a failure probability of 1.7e-20.


def test_kullback_leibler():
    """Check kullback_leibler_check_statistic doesn't give absurdly low
    values."""
    tst.check_generator(kl_test_stat, 6, 4.516e-6, 1.7e-20)


def graph_test_statistic():
    """Plot histogram of kullback_leibler_test_statistic."""
    from matplotlib import pyplot as plt
    plt.ion()
    plt.clf()
    plt.hist([kl_test_stat() for _ in xrange(100)])
    plt.show()
