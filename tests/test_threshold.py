from numpy.random import RandomState

from threshold import failprob_threshold


def test_failprob_threshold_basic():
    """Sanity check on failprob_threshold: Verify, for a relatively large failure
    probability, that the failure threshold it returns for a simple test
    statistic actually results in failures at approximately the right
    frequency.

    """
    prngstate = RandomState(0)

    def sample(n):
        return prngstate.normal(0, 1, n)

    target_prob = 1e-1
    test_sample_size = 6
    prob, thresh = failprob_threshold(
        sample(1000), test_sample_size, target_prob)
    samples = [all(v < thresh for v in sample(test_sample_size))
               for _ in xrange(int(100 / target_prob))]
    assert 50 < samples.count(True) < 200
