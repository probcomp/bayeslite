from numpy import histogram

from tests.data_generator.data_gen_source.categorical.categorical_generator import CategoricalGenerator
from tests.data_generator.data_gen_test.seed_string_to_int import hash_32_unsigned
from tests.stochastic import stochastic

simulate_sample_size = 10000
precision = 0.01

"""
Partitions:
    constructor:
        categorical
        -number of components c: c == 1, c > 1
        -dimensions d: d == 1, d > 1
        -number of categories n: n == 1, n > 1
        -form: list, dictionary
    simulate:
        size s: s == 1, s > 1
    likelihood:
        value: in distribution, not in distribution

Testing Strategy:
    constructor: verify state of all attributes
    simulate: sample a lot, verify all outputs are within range, properly distributed (within a threshold)
    likelihood: ensure that probability matches exactly to attributes
"""


@stochastic(max_runs=10, min_passes=1)
def test_standard_1D_1COMP_multi_category(seed):
    """
    constructor:
        categorical
        -number of components c: c == 1
        -dimensions d: d == 1
        -number of categories n: n > 1
        -form: list
    simulate:
        size s: s == 1, s > 1
    likelihood:
        value: in distribution, not in distribution
    """
    seed = hash_32_unsigned(seed)
    generator = CategoricalGenerator([0.2, 0.3, 0.5])

    # simulate one
    assert generator.simulate(1, seed)[0] in generator.components[0].labels[0], \
        "simulated value is supposed to have a 0 probability"

    # simulate averages correctly
    samples = generator.simulate(simulate_sample_size, seed)
    hist = histogram(samples, 3)[0]
    # difference between actual and supposed of a certain value in simulation is too high
    assert abs(hist[0] - simulate_sample_size * 0.2) <= simulate_sample_size * precision, \
        "simulate did not reflect actual distribution close enough"
    assert abs(hist[1] - simulate_sample_size * 0.3) <= simulate_sample_size * precision, \
        "simulate did not reflect actual distribution close enough"
    assert abs(hist[2] - simulate_sample_size * 0.5) <= simulate_sample_size * precision, \
        "simulate did not reflect actual distribution close enough"

    # likelihood values are correct
    assert generator.likelihood(0) == 0.2, "likelihood of 0 wrong. expected 0.2. got: " + str(generator.likelihood(0))
    assert generator.likelihood(1) == 0.3, "likelihood of 1 wrong. expected 0.3. got: " + str(generator.likelihood(1))
    assert generator.likelihood(2) == 0.5, "likelihood of 2 wrong. expected 0.5. got: " + str(generator.likelihood(2))
    assert generator.likelihood(5) == 0.0, \
        "likelihood of 5 wrong (not in distribution). expected 0.0. got: " + str(generator.likelihood(5))


@stochastic(max_runs=10, min_passes=1)
def test_dict_form_1D_1COMP_multi_category(seed):
    """
    constructor:
        categorical
        -number of components c: c == 1
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
    generator = CategoricalGenerator(params)

    # simulate one
    assert generator.simulate(1, seed)[0] in generator.components[0].labels[0], \
        "simulated value is supposed to have a 0 probability"

    # simulate averages correctly
    samples = generator.simulate(simulate_sample_size, seed)
    hist = {"dog": 0, "bear": 0, "rat": 0, "frog": 0, "squirrel": 0, "deer": 0}
    for s in samples:
        hist[s] += 1
    assert abs(hist["dog"] - simulate_sample_size * params["dog"]) <= simulate_sample_size * precision, \
        "simulated histogram doesn't match probability of dog close enough. " \
        "simulated: " + str(hist["dog"]) + " | expected: " + str(params["dog"] * simulate_sample_size)
    assert abs(hist["bear"] - simulate_sample_size * params["bear"]) <= simulate_sample_size * precision, \
        "simulated histogram doesn't match probability of bear close enough. " \
        "simulated: " + str(hist["bear"]) + " | expected: " + str(params["bear"] * simulate_sample_size)
    assert abs(hist["rat"] - simulate_sample_size * params["rat"]) <= simulate_sample_size * precision, \
        "simulated histogram doesn't match probability of rat close enough. " \
        "simulated: " + str(hist["rat"]) + " | expected: " + str(params["rat"] * simulate_sample_size)
    assert abs(hist["frog"] - simulate_sample_size * params["frog"]) <= simulate_sample_size * precision, \
        "simulated histogram doesn't match probability of frog close enough. " \
        "simulated: " + str(hist["frog"]) + " | expected: " + str(params["frog"] * simulate_sample_size)
    assert abs(hist["squirrel"] - simulate_sample_size * params["squirrel"]) <= simulate_sample_size * precision, \
        "simulated histogram doesn't match probability of squirrel close enough. " \
        "simulated: " + str(hist["squirrel"]) + " | expected: " + str(params["squirrel"] * simulate_sample_size)
    assert abs(hist["deer"] - simulate_sample_size * params["deer"]) <= simulate_sample_size * precision, \
        "simulated histogram doesn't match probability of deer close enough. " \
        "simulated: " + str(hist["deer"]) + " | expected: " + str(params["deer"] * simulate_sample_size)

    # likelihood values are correct
    assert generator.likelihood("dog") == params["dog"], \
        "likelihood of \"dog\" wrong. expected " + str(params["dog"]) + ". got: " + str(generator.likelihood("dog"))
    assert generator.likelihood("bear") == params["bear"], \
        "likelihood of \"bear\" wrong. expected " + str(params["bear"]) + ". got: " + str(generator.likelihood("bear"))
    assert generator.likelihood("rat") == params["rat"], \
        "likelihood of \"rat\" wrong. expected " + str(params["rat"]) + ". got: " + str(generator.likelihood("rat"))
    assert generator.likelihood("frog") == params["frog"], \
        "likelihood of \"frog\" wrong. expected " + str(params["frog"]) + ". got: " + str(generator.likelihood("frog"))
    assert generator.likelihood("squirrel") == params["squirrel"], "likelihood of \"squirrel\" wrong. expected " + str(
        params["squirrel"]) + ". got: " + str(generator.likelihood("squirrel"))
    assert generator.likelihood("deer") == params["deer"], \
        "likelihood of \"deer\" wrong. expected " + str(params["deer"]) + ". got: " + str(generator.likelihood("deer"))
    assert generator.likelihood("human") == 0, \
        "likelihood of \"human\" wrong. expected " + str(0) + ". got: " + str(generator.likelihood("human"))


@stochastic(max_runs=4, min_passes=1)
def test_2D_multi_category(seed):
    """
    constructor:
        categorical
        -number of components c: c > 1
        -dimensions d: d > 1
        -number of categories n: n == 1, n > 1
        -form: list, dictionary
    simulate:
        size s: s > 1
    likelihood:
        value: in distribution, not in distribution
    """
    seed = hash_32_unsigned(seed)
    comp_1 = [{"dog": 0.5, "cat": 0.5}, [0.25, 0.75]]
    comp_2 = [{"yes": 0.3, "no": 0.7}, [1.0]]
    weights = [0.4, 0.6]
    generator = CategoricalGenerator([comp_1, comp_2], weights)

    # simulate averages correctly
    samples = generator.simulate(simulate_sample_size, seed)
    hist = {"dog0": 0, "dog1": 0, "cat0": 0, "cat1": 0, "yes0": 0, "yes1": 0, "no0": 0, "no1": 0}
    # string to just represent both dimensions happening
    for s in samples:
        hist[str(s[0]) + str(s[1])] += 1

    assert abs(hist["dog0"] - simulate_sample_size * weights[0] * comp_1[0]["dog"] * comp_1[1][0]) \
           <= simulate_sample_size * precision, "simulate did not reflect actual distribution close enough"
    assert abs(hist["dog1"] - simulate_sample_size * weights[0] * comp_1[0]["dog"] * comp_1[1][1]) \
           <= simulate_sample_size * precision, "simulate did not reflect actual distribution close enough"
    assert abs(hist["cat0"] - simulate_sample_size * weights[0] * comp_1[0]["cat"] * comp_1[1][0]) \
           <= simulate_sample_size * precision, "simulate did not reflect actual distribution close enough"
    assert abs(hist["cat1"] - simulate_sample_size * weights[0] * comp_1[0]["cat"] * comp_1[1][1]) \
           <= simulate_sample_size * precision, "simulate did not reflect actual distribution close enough"
    assert abs(hist["yes0"] - simulate_sample_size * weights[1] * comp_2[0]["yes"]) \
           <= simulate_sample_size * precision, "simulate did not reflect actual distribution close enough"
    assert abs(hist["yes1"] - 0) <= simulate_sample_size * precision, \
        "simulate did not reflect actual distribution close enough"
    assert abs(hist["no0"] - simulate_sample_size * weights[1] * comp_2[0]["no"]) \
           <= simulate_sample_size * precision, "simulate did not reflect actual distribution close enough"
    assert abs(hist["no1"] - 0) <= simulate_sample_size * precision, \
        "simulate did not reflect actual distribution close enough"


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
    cat = CategoricalGenerator([1.0])

    # simulate one
    assert cat.simulate(1, seed)[0] == 0, "simulated value is supposed to have a 0 probability"

    # simulate multiple
    samples = cat.simulate(simulate_sample_size, seed)
    assert len(samples) == simulate_sample_size, \
        "number of samples incorrect. expected: " + str(simulate_sample_size) + ". got: " + str(
            len(samples))
    samples = set(samples)
    assert samples == {0}, "single length categorical error. expected: {0}. got: " + str(samples)

    # likelihood values are correct
    assert cat.likelihood(0) == 1.0, "likelihood of 0 wrong. expected 1.0. got: " + str(cat.likelihood(0))
    assert cat.likelihood(5) == 0.0, \
        "likelihood of 5 wrong (not in distribution). expected 0.0. got: " + str(cat.likelihood(5))

# test_standard_1D_1COMP_multi_category()
# test_dict_form_1D_1COMP_multi_category()
# test_2D_multi_category()
# test_single()
