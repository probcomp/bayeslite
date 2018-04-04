import numpy as np
import pytest
import scipy as sp

from tests.data_generator.data_gen_source.gaussian.gaussian_component import GaussianComponent
from tests.data_generator.data_gen_test.seed_string_to_int import hash_32_unsigned
from tests.stochastic import stochastic

simulate_sample_size = 500
likelihood_precision = 0.00000001


def logpdf_1D(value, mean, standard_deviation):
    """
    Probability density function of a value on a normal distribution, e^(-x^2/2)/sqrt(2pi)
    """
    # hand calculated (not working for certain values)
    # return math.e ** (-(value - mean) ** 2 / (2 * standard_deviation ** 2)) \
    #        / math.sqrt(2 * math.pi * standard_deviation ** 2)

    return np.log(sp.stats.norm.pdf(value, mean, standard_deviation))


def logpdf_MD(value, mean, standard_deviation):
    return np.log(sp.stats.multivariate_normal.pdf(value, mean, standard_deviation))


"""
Partitions:
    constructor:
        component_parameters:
        -dimension of component d: d == 1, d > 1
    likelihood:
        data: univariate unwrapped data, univariate wrapped [data], multivariate
    simulate:
        size s: s == 1, s > 1
        output data: univariate, multivariate
    errors:
        -constructor: mismatched number of means and standard deviations

Testing Strategy:
    constructor: verify state of all attributes
    simulate: sample a lot, verify all outputs are within range, properly distributed (within a threshold)
    likelihood: verify probability is the same as hand calculations
"""


@stochastic(max_runs=2, min_passes=1)
def test_standard_1D_1COMP_comprehensive(seed):
    """
    constructor:
        component_parameters
        -dimension of components d: d == 1
    likelihood:
        data: univariate unwrapped data, univariate wrapped [data]
    simulate:
        size s: s == 1, s > 1
        output data: univariate
    """
    seed = hash_32_unsigned(seed)
    mean = 5
    std_dev = 1
    component = GaussianComponent(mean, std_dev)

    # constructor
    assert component.mean == mean, "incorrect mean attribute"
    assert component.standard_deviation == std_dev, "incorrect standard_deviation attribute"
    assert component.dimensions == 1, "incorrect dimension attribute"

    # simulate:
    single = component.simulate(1, seed)
    assert len(single) == 1, "simulated incorrect number of values"
    samples = component.simulate(simulate_sample_size, seed)
    assert len(samples) == simulate_sample_size, "simulated incorrect number of values"
    assert abs(np.mean(samples)) - mean < 0.1, "simulated mean averaged over " + str(simulate_sample_size) + \
                                               " samples was too far from true mean"

    # likelihood:
    # quick spot check:
    assert abs(component.likelihood(0) - logpdf_1D(0, mean, std_dev)) < likelihood_precision, \
        "likelihood of 0 (not wrapped in list) wrong"
    assert abs(component.likelihood([0]) - logpdf_1D(0, mean, std_dev)) < likelihood_precision, \
        "likelihood of 0 (wrapped in list) wrong"
    assert abs(component.likelihood(1) - logpdf_1D(1, mean, std_dev)) < likelihood_precision, \
        "likelihood of 1 (not wrapped in list) wrong"
    assert abs(component.likelihood(single) - logpdf_1D(single[0], mean, std_dev)) < likelihood_precision, \
        "likelihood of " + str(single) + " wrong"
    # full data_generator:
    for sample in samples:
        assert abs(component.likelihood(sample) - logpdf_1D(sample, mean, std_dev)) < likelihood_precision, \
            "likelihood of " + str(sample) + " wrong"
    # multiple samples:
    assert abs(component.likelihood([single[0], 1]) - logpdf_1D(single, mean, std_dev) - logpdf_1D(1, mean, std_dev)) \
           < likelihood_precision, "likelihood of " + str(single[0]) + " and 1 together wrong"


@stochastic(max_runs=2, min_passes=1)
def test_standard_2D_2COMP_comprehensive(seed):
    """
    constructor:
        component_parameters
        -dimension of components d: d > 1
    likelihood:
        data: multivariate
    simulate:
        size s: s == 1, s > 1
        output data: multivariate
    """
    seed = hash_32_unsigned(seed)
    mean = [0, 5]
    std_dev = [0.1, 0.5]
    component = GaussianComponent(mean, std_dev)

    # constructor
    assert component.mean == mean, "incorrect mean attribute"
    assert component.standard_deviation == std_dev, "incorrect standard_deviation attribute"
    assert component.dimensions == 2, "incorrect dimension attribute"

    # simulate:
    single = component.simulate(1, seed)
    assert len(single) == 1, "simulated incorrect number of values"
    samples = component.simulate(simulate_sample_size, seed)
    assert len(samples) == simulate_sample_size, "simulated incorrect number of values"

    # simulate comes in [ [0.231, 0.412] ... ], need to get list of each dimension
    first_dimension = []
    second_dimension = []
    for s in samples:
        first_dimension.append(s[0])
        second_dimension.append(s[1])

    assert abs(np.mean(first_dimension)) - mean[0] < 0.1, "simulated mean averaged over " + str(
        simulate_sample_size) + " samples was too far from true mean"
    assert abs(np.mean(second_dimension)) - mean[1] < 0.1, "simulated mean averaged over " + str(
        simulate_sample_size) + " samples was too far from true mean"

    # likelihood:
    # quick spot check:
    # self.assertAlmostEqual(component.likelihood([0, 5]),
    #                        logpdf_1D(0, mean[0], std_dev[0]) * logpdf_1D(5, mean[1], std_dev[1]), 10,
    #                        "likelihood of [0, 5] wrong")
    assert abs(component.likelihood([[0, 5]]) - logpdf_MD([0, 5], mean, std_dev)) < likelihood_precision, \
        "likelihood of [0, 5] wrong"
    # full data_generator:
    for s in samples:
        assert abs(component.likelihood([s]) - logpdf_MD(s, mean, std_dev)) < likelihood_precision, \
            "likelihood of " + str(s.tolist()) + " wrong"


def test_error_bad_input():
    """
    errors:
        -constructor: mismatched number of means and standard deviations
    """
    with pytest.raises(ValueError):
        GaussianComponent([0.2, 0.7], [0.5, 0.7, 2])  # 2 means, 3 standard deviations, bad input


        # test_standard_1D_1COMP_comprehensive()
        # test_standard_2D_2COMP_comprehensive()
        # test_error_bad_input()
