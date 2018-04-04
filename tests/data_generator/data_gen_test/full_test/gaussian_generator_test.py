import math

import numpy as np
import scipy as sp

from tests.data_generator.data_gen_source.gaussian.gaussian_generator import GaussianGenerator
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
        -number of components c: c == 1 unwrapped [0, 1], c == 1 wrapped [[0, 1]], c > 1
        -dimension of components d: d == 1 unwrapped [0, 1], d == 1 wrapped [[0, 1]], d > 1
    likelihood:
        data: univariate unwrapped data, univariate wrapped [data], multivariate
    simulate:
        size s: s == 1, s > 1
        output data: univariate, multivariate

Testing Strategy:
    constructor: verify state of all attributes
    simulate: sample a lot, verify all outputs are within range, properly distributed (within a threshold)
    likelihood: verify probability is the same as hand calculations
"""

@stochastic(max_runs=2, min_passes=1)
def test_standard_1D_1COMP_comprehensive(seed):
    """
    constructor:
        component_parameters:
        -number of components c: c == 1 unwrapped [0, 1]
        -dimension of components d: d == 1 unwrapped [0, 1]
    likelihood:
        data: univariate unwrapped data, univariate wrapped [data]
    simulate:
        size s: s == 1, s > 1
        output data: univariate
    """
    seed = hash_32_unsigned(seed)
    mean = 10
    std_dev = 1
    comp_1 = [mean, std_dev]
    generator = GaussianGenerator(comp_1)

    # constructor
    assert generator.components[0].mean == mean, "incorrect mean attribute"
    assert generator.components[0].standard_deviation == std_dev, "incorrect standard_deviation attribute"
    assert generator.components[0].dimensions == 1, "incorrect dimension attribute"

    # simulate:
    single = generator.simulate(1, seed)
    assert len(single) == 1, "simulated incorrect number of values"
    samples = generator.simulate(simulate_sample_size, seed)
    assert len(samples) == simulate_sample_size, "simulated incorrect number of values"
    assert abs(np.mean(samples) - mean) < 0.1, "simulated mean averaged over " + str(simulate_sample_size) + \
                                               " samples was too far from true mean"

    # likelihood:
    # quick spot check:
    assert abs(generator.likelihood(0) - logpdf_1D(0, mean, std_dev)) < likelihood_precision, \
        "likelihood of 0 (not wrapped in list) wrong"
    assert abs(generator.likelihood([0]) - logpdf_1D(0, mean, std_dev)) < likelihood_precision, \
        "likelihood of 0 (wrapped in list) wrong"
    assert abs(generator.likelihood(1) - logpdf_1D(1, mean, std_dev)) < likelihood_precision, \
        "likelihood of 1 (not wrapped in list) wrong"
    assert abs(generator.likelihood(single) - logpdf_1D(single[0], mean, std_dev)) < likelihood_precision, \
        "likelihood of " + str(single) + " wrong"

    # full data_generator:
    for sample in samples:
        assert abs(generator.likelihood(sample) - logpdf_1D(sample, mean, std_dev)) < likelihood_precision, \
            "likelihood of " + str(sample) + " wrong"
    # multiple samples:
    assert abs(generator.likelihood([single[0], 1]) - logpdf_1D(single, mean, std_dev) - logpdf_1D(1, mean, std_dev)) \
           < likelihood_precision, "likelihood of " + str(single[0]) + " and 1 together wrong"


@stochastic(max_runs=2, min_passes=1)
def test_wrapped_1D_1COMP_comprehensive(seed):
    """
    constructor:
        component_parameters:
        -number of components c: c == 1 wrapped [[0, 1]]
        -dimension of components d: d == 1 wrapped [[0, 1]]
    likelihood:
        data: univariate unwrapped data, univariate wrapped [data]
    simulate:
        size s: s == 1, s > 1
        output data: univariate
    """
    seed = hash_32_unsigned(seed)
    mean = 10
    std_dev = 1
    comp_1 = [mean, std_dev]
    generator_wrapped = GaussianGenerator([comp_1])

    # constructor
    assert generator_wrapped.components[0].mean == mean, "incorrect mean attribute"
    assert generator_wrapped.components[0].standard_deviation == std_dev, "incorrect standard_deviation attribute"
    assert generator_wrapped.components[0].dimensions == 1, "incorrect dimension attribute"

    # simulate:
    single = generator_wrapped.simulate(1, seed)
    assert len(single) == 1, "simulated incorrect number of values"
    samples = generator_wrapped.simulate(simulate_sample_size, seed)
    assert len(samples) == simulate_sample_size, "simulated incorrect number of values"
    assert abs(np.mean(samples) - mean) < 0.1, "simulated mean averaged over " + str(simulate_sample_size) + \
                                               " samples was too far from true mean"

    # likelihood:
    # quick spot check:
    assert abs(generator_wrapped.likelihood(0) - logpdf_1D(0, mean, std_dev)) < likelihood_precision, \
        "likelihood of 0 (not wrapped in list) wrong"
    assert abs(generator_wrapped.likelihood([0]) - logpdf_1D(0, mean, std_dev)) < likelihood_precision, \
        "likelihood of 0 (wrapped in list) wrong"
    assert abs(generator_wrapped.likelihood(1) - logpdf_1D(1, mean, std_dev)) < likelihood_precision, \
        "likelihood of 1 (not wrapped in list) wrong"
    assert abs(generator_wrapped.likelihood(single) - logpdf_1D(single[0], mean, std_dev)) < likelihood_precision, \
        "likelihood of " + str(single) + " wrong"

    # full data_generator:
    for sample in samples:
        assert abs(generator_wrapped.likelihood(sample) - logpdf_1D(sample, mean, std_dev)) < likelihood_precision, \
            "likelihood of " + str(sample) + " wrong"
    # multiple samples:
    assert abs(generator_wrapped.likelihood([single[0], 1]) - logpdf_1D(single, mean, std_dev) -
               logpdf_1D(1, mean, std_dev)) < likelihood_precision, \
        "likelihood of " + str(single[0]) + " and 1 together wrong"


@stochastic(max_runs=10, min_passes=1)
def test_standard_2D_2COMP_comprehensive(seed):
    """
    constructor:
        component_parameters:
        -number of components c: c > 1
        -dimension of components d: d > 1
    likelihood:
        data: multivariate
    simulate:
        size s: s == 1, s > 1
        output data: multivariate
    """
    seed = hash_32_unsigned(seed)
    comp_1 = [[0, 5], [1, 1]]
    comp_2 = [[10, 15], [1, 1]]
    generator = GaussianGenerator([comp_1, comp_2])

    # constructor
    # first component
    assert generator.components[0].mean[0] == comp_1[0][0], "incorrect mean attribute"
    assert generator.components[0].standard_deviation[0] == comp_1[1][0], "incorrect standard_deviation attribute"
    assert generator.components[0].mean[1] == comp_1[0][1], "incorrect mean attribute"
    assert generator.components[0].standard_deviation[1] == comp_1[1][1], "incorrect standard_deviation attribute"
    assert generator.components[0].dimensions == 2, "incorrect dimension attribute"
    # second component
    assert generator.components[1].mean[0] == comp_2[0][0], "incorrect mean attribute"
    assert generator.components[1].standard_deviation[0] == comp_2[1][0], "incorrect standard_deviation attribute"
    assert generator.components[1].mean[1] == comp_2[0][1], "incorrect mean attribute"
    assert generator.components[1].standard_deviation[1] == comp_2[1][1], "incorrect standard_deviation attribute"
    assert generator.components[1].dimensions == 2, "incorrect dimension attribute"

    # simulate:
    single = generator.simulate(1, seed)
    assert len(single) == 1, "simulated incorrect number of values"
    samples = generator.simulate(simulate_sample_size, seed)
    assert len(samples) == simulate_sample_size, "simulated incorrect number of values"

    # simulate comes in [ [0.231, 0.412] ... ], need to get list of each dimension
    first_dimension = []
    second_dimension = []
    for s in samples:
        first_dimension.append(s[0])
        second_dimension.append(s[1])

    # average means of all components for a single dimension
    average_mean_first_dimension = (comp_1[0][0] + comp_2[0][0]) / 2
    average_mean_second_dimension = (comp_1[0][1] + comp_2[0][1]) / 2

    assert abs(np.mean(first_dimension) - average_mean_first_dimension) < 0.1, \
        "simulated mean over " + str(simulate_sample_size) + " samples was too far from true mean"
    assert abs(np.mean(second_dimension) - average_mean_second_dimension) < 0.1, \
        "simulated mean over " + str(simulate_sample_size) + " samples was too far from true mean"

    # likelihood:
    # quick spot check:
    # self.assertAlmostEqual(generator.likelihood([0, 5]),
    #                        logpdf_1D(0, comp_1[0][0], comp_1[1][0]) + logpdf_1D(0, comp_2[0][0], comp_2[1][0]) +
    #                        logpdf_1D(5, comp_1[0][1], comp_1[1][1]) + logpdf_1D(5, comp_2[0][1], comp_2[1][1]),
    #                        10, "likelihood of [0, 5] wrong")
    assert abs(generator.likelihood([[0, 5]]) - np.log(
        (math.e ** logpdf_MD([0, 5], comp_1[0], comp_1[1]) + math.e ** logpdf_MD([0, 5], comp_2[0], comp_2[1])) * 0.5
    )) < likelihood_precision, "likelihood of [0, 5] wrong"
    # calculated out logpdf of multi-dimension, multi-component

    # full data_generator:
    for s in samples:
        assert abs(generator.likelihood([s]) - np.log(
            (math.e ** logpdf_MD(s, comp_1[0], comp_1[1]) + math.e ** logpdf_MD(s, comp_2[0], comp_2[1])) * 0.5)) \
               < likelihood_precision, "likelihood of " + str(s.tolist()) + " wrong"


# test_standard_1D_1COMP_comprehensive()
# test_wrapped_1D_1COMP_comprehensive()
# test_standard_2D_2COMP_comprehensive()