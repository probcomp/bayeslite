from __future__ import division  # For type safety in gaussian_kl_divergence

from functools import partial
from math import erfc

import numpy as np
from numpy.random import RandomState

import kl
import threshold


def gaussian_kl_divergence(mu1, s1, mu2, s2):
    "Return KL(N(mu1,s1)||N(mu2,s2))"
    # http://stats.stackexchange.com/a/7443/40686
    return np.log(s2 / s1) + ((s1**2 + (mu1 - mu2)**2) / (2 * s2**2)) - 0.5


def gaussian_log_pdf(mu, s):
    def lpdf(x):
        normalizing_constant = -(np.log(2 * np.pi) / 2) - np.log(s)
        return normalizing_constant - ((x - mu)**2 / (2 * s**2))
    return lpdf


def compute_kullback_leibler_check_statistic(n=100, prngstate=None):
    """Compute the lowest of the survival function and the CDF of the exact KL
    divergence KL(N(mu1,s1)||N(mu2,s2)) w.r.t. the sample distribution of the
    KL divergence drawn by computing log(P(x|N(mu1,s1)))-log(P(x|N(mu2,s2)))
    over a sample x~N(mu1,s1). If we are computing the KL divergence
    accurately, the exact value should fall squarely in the sample, and the
    tail probabilities should be relatively large.

    """
    if prngstate is None:
        raise TypeError('Must explicitly specify numpy.random.RandomState')
    mu1 = mu2 = 0
    s1 = 1
    s2 = 2
    exact = gaussian_kl_divergence(mu1, s1, mu2, s2)
    sample = prngstate.normal(mu1, s1, n)
    lpdf1 = gaussian_log_pdf(mu1, s1)
    lpdf2 = gaussian_log_pdf(mu2, s2)
    estimate, std = kl.kullback_leibler(sample, lpdf1, lpdf2)
    # This computes the minimum of the left and right tail probabilities of the
    # exact KL divergence vs a gaussian fit to the sample estimate. There is a
    # distinct negative skew to the samples used to compute `estimate`, so this
    # statistic is not uniform. Nonetheless, we do not expect it to get too
    # small.
    return erfc(abs(exact - estimate) / std) / 2


def kl_test_stat():
    prngstate = RandomState(17)
    return partial(compute_kullback_leibler_check_statistic,
                   prngstate=prngstate)


def compute_kl_threshold():
    """Compute the values used in test_kullback_leibler

    >>> threshold.compute_sufficiently_stringent_threshold(
            kl_test_stat(), 6, 1e-20)
    ...
    TestThreshold(threshold=4.3883148424367044e-13,
                  failprob=9.724132259513859e-21,
                  sample_size=252135)

    This means that after generating 252135 check statistics, it was found that
    the least value of six samples will be less than 4.3883148424367044e-13
    with probability less than 9.724132259513859e-21 (< 1e-20).

    """
    return threshold.compute_sufficiently_stringent_threshold(
        kl_test_stat(), 6, 1e-20)


def test_kullback_leibler():
    """Check kullback_leibler_check_statistic doesn't give absurdly low
    values."""
    # See compute_kl_threshold for derivation
    kl_threshold = threshold.TestThreshold(threshold=4.3883148424367044e-13,
                                           failprob=9.724132259513859e-21,
                                           sample_size=252135)
    threshold.check_generator(kl_test_stat(), 6, kl_threshold.threshold, 1e-20)
