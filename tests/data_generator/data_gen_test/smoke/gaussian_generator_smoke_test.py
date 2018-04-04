import numpy as np
import scipy as sp

from tests.data_generator.data_gen_source.gaussian.gaussian_generator import GaussianGenerator
from tests.data_generator.data_gen_test.seed_string_to_int import hash_32_unsigned
from tests.stochastic import stochastic

simulate_sample_size = 10
simulate_precision = 0.5
likelihood_precision = 0.01


@stochastic(max_runs=4, min_passes=1)
def test_gaussian_generator_smoke(seed):
    seed = hash_32_unsigned(seed)
    mean = 1
    standard_deviation = 1

    generator = GaussianGenerator([mean, standard_deviation])

    # simulate
    samples = generator.simulate(simulate_sample_size, seed)

    assert abs(np.mean(samples) - mean) <= simulate_precision, "simulated mean not close enough to actual mean"

    # likelihood
    choice = samples[0]
    true_likelihood = np.log(sp.stats.norm.pdf(choice, mean, standard_deviation))
    assert generator.likelihood(choice) <= likelihood_precision, "likelihood of " + str(
        choice) + " incorrect. Expected: " + str(true_likelihood) + ". Got: " + str(generator.likelihood(choice))


    # test_gaussian_generator_smoke()
