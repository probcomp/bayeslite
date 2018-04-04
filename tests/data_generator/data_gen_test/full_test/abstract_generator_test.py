import pytest

from tests.data_generator.data_gen_source.categorical.categorical_generator import CategoricalGenerator
from tests.data_generator.data_gen_source.gaussian.gaussian_generator import GaussianGenerator

"""
Partitions:
    constructor:
        component_parameters:
        -number of components c: c == 1 unwrapped [0, 1], c == 1 wrapped [[0, 1]], c > 1
        -weights: None, sums to 1
    error:
        sum(weights) < 1
        sum(weights) > 1
        len(weights) > len(components)

Testing Strategy:
    Using both GaussianGenerator and Categorical concrete classes for testing
    constructor: just data_gen_test that it doesn't throw error, concrete classes cover validity of everything else
    simulate: sample a lot, verify all outputs are within range, properly distributed (within a threshold)
    likelihood: verify probability is the same as hand calculations
    error: ensure correct exceptions are thrown
"""


def test_single_component():
    """
    constructor:
        component_parameters:
        -number of components c: c == 1 unwrapped [0, 1], c == 1 wrapped [[0, 1]]
        -weights: None
    """
    # Gaussian
    params = [1, 1]
    GaussianGenerator(params)
    GaussianGenerator([params])

    # Categorical
    params = [0.2, 0.3, 0.5]
    CategoricalGenerator(params)
    CategoricalGenerator([params])


def test_multi_component():
    """
    constructor:
        component_parameters:
        -number of components c: c > 1
        -weights: None, sums to 1
    """
    comp1 = [[1, 1], [5, 5]]
    comp2 = [[2, 2], [200, 200]]
    weights = [0.4, 0.6]
    GaussianGenerator([comp1, comp2], weights)

    comp1 = [[0.5, 0.5], {"dog": 1.0}]
    comp2 = [[0.3, 0.7], {"cat": 0.5, "bear": 0.5}]
    comp3 = [[0.2, 0.8], {"bird": 1.0}]
    weights = [0.4, 0.5, 0.1]
    CategoricalGenerator([comp1, comp2, comp3], weights)


def test_error():
    """
    error:
        sum(weights) < 1
        sum(weights) > 1
        len(weights) > len(components)
    """
    comp1 = [[1, 1], [5, 5]]
    comp2 = [[2, 2], [200, 200]]

    with pytest.raises(ValueError):
        weights = [0.4, 0.5]
        GaussianGenerator([comp1, comp2], weights)

    with pytest.raises(ValueError):
        weights = [0.6, 0.5]
        GaussianGenerator([comp1, comp2], weights)

    with pytest.raises(ValueError):
        weights = [1.0]
        GaussianGenerator([comp1, comp2], weights)
