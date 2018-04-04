import pytest
from numpy import histogram

from tests.data_generator.data_gen_source.categorical.categorical_component import CategoricalComponent
from tests.data_generator.data_gen_test.seed_string_to_int import hash_32_unsigned
from tests.stochastic import stochastic

simulate_sample_size_1D = 20000
simulate_sample_size_2D = 5000
precision = 0.05

"""
Partitions:
    constructor:
        categorical
        -dimensions d: d == 1, d > 1
        -number of categories n: n == 1, n > 1
        -form: list, dictionary
    simulate:
        size s: s == 1, s > 1
    likelihood:
        value: in distribution, not in distribution
    error:
        sum(probabilities) < 1
        sum(probabilities) > 1

Testing Strategy:
    constructor: verify state of all attributes
    simulate: sample a lot, verify all outputs are within range, properly distributed (within a threshold)
    likelihood: ensure that probability matches exactly to attributes
    error: ensure correct exceptions are thrown
"""


@stochastic(max_runs=10, min_passes=1)
def test_standard_1D_multi_category(seed):
    """
    constructor:
        categorical
        -dimensions d: d == 1
        -number of categories n: n > 1
        -form: list
    simulate:
        size s: s == 1, s > 1
    likelihood:
        value: in distribution, not in distribution
    """
    seed = hash_32_unsigned(seed)
    cat = CategoricalComponent([0.2, 0.3, 0.5])

    # constructor attributes are correct
    assert cat.dict_forms == [{0: 0.2, 1: 0.3, 2: 0.5}], \
        "list not converted to proper dictionary. expected: {0: 0.2, 1: 0.3, 2: 0.5}. got: " + str(
            cat.dict_forms)
    assert sorted(cat.labels) == [[0, 1, 2]], \
        "incorrect categorical labels. expected: [0, 1, 2]. got: " + str(sorted(cat.labels))
    assert sorted(cat.probabilities) == [[0.2, 0.3, 0.5]], \
        "incorrect categorical probabilities. expected: [0.2, 0.3, 0.5]. got: " + str(
            sorted(cat.probabilities))

    # simulate one
    assert (cat.simulate(1, seed)[0] in cat.labels[0]) is True, "simulated value is supposed to have a 0 probability"

    # simulate averages correctly
    samples = cat.simulate(simulate_sample_size_1D, seed)
    hist = histogram(samples, 3)[0]
    # difference between actual and supposed of a certain value in simulation is too high

    assert abs(hist[0] - simulate_sample_size_1D * 0.2) <= simulate_sample_size_1D * precision, \
        "simulate did not reflect actual distribution close enough"
    assert abs(hist[1] - simulate_sample_size_1D * 0.3) <= simulate_sample_size_1D * precision, \
        "simulate did not reflect actual distribution close enough"
    assert abs(hist[2] - simulate_sample_size_1D * 0.5) <= simulate_sample_size_1D * precision, \
        "simulate did not reflect actual distribution close enough"

    # likelihood values are correct
    assert cat.likelihood(0) == 0.2, "likelihood of 0 wrong. expected 0.2. got: " + str(cat.likelihood(0))
    assert cat.likelihood(1) == 0.3, "likelihood of 1 wrong. expected 0.3. got: " + str(cat.likelihood(1))
    assert cat.likelihood(2) == 0.5, "likelihood of 2 wrong. expected 0.5. got: " + str(cat.likelihood(2))
    assert cat.likelihood(5) == 0.0, \
        "likelihood of 5 wrong (not in distribution). expected 0.0. got: " + str(cat.likelihood(5))
    # multiple samples
    assert cat.likelihood([0, 1]) == 0.06, \
        "likelihood of 0 and 1 together wrong. expected 0.06. got: " + str(cat.likelihood([0, 1]))


@stochastic(max_runs=10, min_passes=1)
def test_dict_form_1D_multi_category(seed):
    """
    constructor:
        categorical
        -dimensions d: d == 1
        -number of categories n: n > 1
        -form: dictionary
    simulate:
        size s: s == 1, s > 1
    likelihood:
        value: in distribution, not in distribution
    """
    seed = hash_32_unsigned(seed)
    params = {"dog": 0.1, "bear": 0.3, "rat": 0.2, "frog": 0.2, "squirrel": 0.15, "deer": 0.05}
    cat = CategoricalComponent(params)

    # constructor attributes are correct
    assert cat.dict_forms == [
        params], "list not converted to proper dictionary. expected: {\"dog\": 0.1, \"bear\": 0.3, \"rat\": " \
                 "0.2, \"frog\": 0.2, \"squirrel\": 0.15, \"deer\": 0.05}. got: " + str(cat.dict_forms)
    assert sorted(cat.labels) == [sorted(params.keys())], "incorrect categorical labels. expected: " + \
                                                          str(sorted(params.keys())) + ". got: " + str(
        sorted(cat.labels[0]))
    # this just sorts labels then matches probabilities to the correct indices
    aligned = list(zip(*sorted(zip(cat.labels[0], cat.probabilities[0]))))
    assert aligned[1] == (0.3, 0.05, 0.1, 0.2, 0.2, 0.15), \
        "incorrect categorical probabilities. expected: [0.3, 0.05, 0.1, 0.2, 0.2, 0.15]. got: " + str(aligned[1])

    # simulate one
    assert (cat.simulate(1, seed)[0] in cat.labels[0]) is True, "simulated value is supposed to have a 0 probability"

    # simulate averages correctly
    samples = cat.simulate(simulate_sample_size_1D, seed)
    hist = {"dog": 0, "bear": 0, "rat": 0, "frog": 0, "squirrel": 0, "deer": 0}
    for s in samples:
        hist[s] += 1
    assert abs(hist["dog"] - simulate_sample_size_1D * params["dog"]) <= simulate_sample_size_1D * precision, \
        "simulated histogram doesn't match probability of dog close enough. " \
        "simulated: " + str(hist["dog"]) + " | expected: " + str(params["dog"] * simulate_sample_size_1D)
    assert abs(hist["bear"] - simulate_sample_size_1D * params["bear"]) <= simulate_sample_size_1D * precision, \
        "simulated histogram doesn't match probability of bear close enough. " \
        "simulated: " + str(hist["bear"]) + " | expected: " + str(params["bear"] * simulate_sample_size_1D)
    assert abs(hist["rat"] - simulate_sample_size_1D * params["rat"]) <= simulate_sample_size_1D * precision, \
        "simulated histogram doesn't match probability of rat close enough. " \
        "simulated: " + str(hist["rat"]) + " | expected: " + str(params["rat"] * simulate_sample_size_1D)
    assert abs(hist["frog"] - simulate_sample_size_1D * params["frog"]) <= simulate_sample_size_1D * precision, \
        "simulated histogram doesn't match probability of frog close enough. " \
        "simulated: " + str(hist["frog"]) + " | expected: " + str(params["frog"] * simulate_sample_size_1D)
    assert abs(hist["squirrel"] - simulate_sample_size_1D * params["squirrel"]) <= simulate_sample_size_1D * precision, \
        "simulated histogram doesn't match probability of squirrel close enough. " \
        "simulated: " + str(hist["squirrel"]) + " | expected: " + str(params["squirrel"] * simulate_sample_size_1D)
    assert abs(hist["deer"] - simulate_sample_size_1D * params["deer"]) <= simulate_sample_size_1D * precision, \
        "simulated histogram doesn't match probability of deer close enough. " \
        "simulated: " + str(hist["deer"]) + " | expected: " + str(params["deer"] * simulate_sample_size_1D)

    # likelihood values are correct
    assert cat.likelihood("dog") == params["dog"], \
        "likelihood of \"dog\" wrong. expected " + str(params["dog"]) + ". got: " + str(cat.likelihood("dog"))
    assert cat.likelihood("bear") == params["bear"], \
        "likelihood of \"bear\" wrong. expected " + str(params["bear"]) + ". got: " + str(cat.likelihood("bear"))
    assert cat.likelihood("rat") == params["rat"], \
        "likelihood of \"rat\" wrong. expected " + str(params["rat"]) + ". got: " + str(cat.likelihood("rat"))
    assert cat.likelihood("frog") == params["frog"], \
        "likelihood of \"frog\" wrong. expected " + str(params["frog"]) + ". got: " + str(cat.likelihood("frog"))
    assert cat.likelihood("squirrel") == params["squirrel"], "likelihood of \"squirrel\" wrong. expected " + str(
        params["squirrel"]) + ". got: " + str(cat.likelihood("squirrel"))
    assert cat.likelihood("deer") == params["deer"], \
        "likelihood of \"deer\" wrong. expected " + str(params["deer"]) + ". got: " + str(cat.likelihood("deer"))


@stochastic(max_runs=4, min_passes=1)
def test_2D_multi_category(seed):
    """
    constructor:
        categorical
        -dimensions d: d > 1
        -number of categories n: n > 1
        -form: list, dictionary
    simulate:
        size s: s == 1, s > 1
    likelihood:
        value: in distribution, not in distribution
    """
    seed = hash_32_unsigned(seed)
    parameters = [{"dog": 0.5, "cat": 0.5}, [0.25, 0.75]]
    cat = CategoricalComponent(parameters)

    # constructor attributes
    assert cat.dict_forms == [parameters[0], {0: 0.25, 1: 0.75}], "dict form incorrect"
    assert [sorted(cat.labels[0]), cat.labels[1]] == [sorted(["dog", "cat"]), [0, 1]], "labels incorrect"
    assert cat.probabilities == [list(parameters[0].values()), sorted(parameters[1])], "probabilities incorrect"

    # simulate one
    single = cat.simulate(1, seed)[0]
    assert single[0] in cat.labels[0], "simulated value is supposed to have a 0 probability"
    assert single[1] in cat.labels[1], "simulated value is supposed to have a 0 probability"

    # simulate averages correctly
    samples = cat.simulate(simulate_sample_size_2D, seed)
    hist = {"dog0": 0, "dog1": 0, "cat0": 0, "cat1": 0}  # string to just represent both dimensions happening
    for s in samples:
        hist[str(s[0]) + str(s[1])] += 1
    assert abs(hist["dog0"] - simulate_sample_size_2D * 0.5 * 0.25) <= simulate_sample_size_2D * precision, \
        "simulate did not reflect actual distribution close enough"
    assert abs(hist["dog1"] - simulate_sample_size_2D * 0.5 * 0.75) <= simulate_sample_size_2D * precision, \
        "simulate did not reflect actual distribution close enough"
    assert abs(hist["cat0"] - simulate_sample_size_2D * 0.5 * 0.25) <= simulate_sample_size_2D * precision, \
        "simulate did not reflect actual distribution close enough"
    assert abs(hist["cat1"] - simulate_sample_size_2D * 0.5 * 0.75) <= simulate_sample_size_2D * precision, \
        "simulate did not reflect actual distribution close enough"


def test_bad_construct():
    """
    error:
        sum(probabilities) < 1
        sum(probabilities) > 1
    """
    with pytest.raises(ValueError):
        CategoricalComponent([0.5, 0.5, 0.5])  # probabilities sum over 1
    with pytest.raises(ValueError):
        CategoricalComponent([0.5, 0.2])  # probabilities sum under 1


@stochastic(max_runs=1, min_passes=1)
def test_single(seed):
    """
    constructor:
        categorical
        -dimensions d: d == 1
        -number of categories n: n == 1
        -form: list
    simulate:
        size s: s == 1, s > 1
    likelihood:
        value: in distribution, not in distribution
    """
    seed = hash_32_unsigned(seed)
    cat = CategoricalComponent([1.0])
    # constructor attributes are correct
    assert cat.dict_forms == [{0: 1.0}], \
        "list not converted to proper dictionary. expected: {0: 1.0}. got: " + str(cat.dict_forms)
    assert cat.labels == [[0]], "incorrect categorical labels. expected: [0]. got: " + str(cat.labels)
    assert cat.probabilities == [[1.0]], \
        "incorrect categorical probabilities. expected: [0.2, 0.3, 0.5]. got: " + str(cat.probabilities)

    # simulate one
    assert cat.simulate(1, seed)[0] == 0, "simulated value is supposed to only be 0"

    # simulate multiple
    samples = cat.simulate(simulate_sample_size_1D, seed)
    assert len(samples) == simulate_sample_size_1D, \
        "number of samples incorrect. expected: " + str(simulate_sample_size_1D) + ". got: " + str(len(samples))
    samples = set(samples)
    assert samples == {0}, "single length categorical error. expected: {0}. got: " + str(samples)

    # likelihood values are correct
    assert cat.likelihood(0) == 1.0, "likelihood of 0 wrong. expected 1.0. got: " + str(cat.likelihood(0))
    assert cat.likelihood(5) == 0.0, \
        "likelihood of 5 wrong (not in distribution). expected 0.0. got: " + str(cat.likelihood(5))


# test_standard_1D_multi_category()
# test_dict_form_1D_multi_category()
# test_2D_multi_category()
# test_single()
# test_bad_construct()
