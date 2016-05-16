"""Kullback Leibler divergence estimates"""

from collections import namedtuple
from numpy import array, sqrt

class KLEstimate(namedtuple('KLEstimate', ['estimate', 'se'])):
    """Container for return value from kullback_leibler.

    `estimate`: The estimated KL divergence, mean of the sampled integrand
    values.

    `se`: Estimated standard deviation of the samples from which the mean was
    calculated. In general the mean and variance of log(P(x)) is not known to
    be finite, but it will be for any distribution crosscat generates at the
    moment, because they all have finite entropy. Hence the Central Limit
    Theorem applies at some sample size, and this can in principle be used as a
    rough guide to the precision of the estimate. In tests comparing the
    univariate gaussians N(0,1) and N(0,2), it tended to have a visually
    obvious bias for sample sizes below 100,000.

    """
    pass

def kullback_leibler(postsample, postlpdf, complpdf):
    """Estimate KL-divergence of sample (a collection of values) w.r.t. known pdf,
    `complpdf`, which returns the density when passed a sample. Return value is
    a `KLEstimate`. The attribute you probably care most about is
    `KLEstimate.estimate`. See `KLEstimate.__doc__` for more details. The
    `postsample` argument is an approximate sample from the distribution
    approximately represented by `postlpdf`.

    """
    klsamples = array([postlpdf(x) - complpdf(x) for x in postsample])
    std = klsamples.std() / sqrt(len(klsamples))
    return KLEstimate(estimate=klsamples.mean(), se=std)
