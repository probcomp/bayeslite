import numpy as np
import pandas as pd
import sys
from matplotlib import pyplot as plt
import os
from timeit import default_timer as timer

from tests.data_generator.data_gen_source.csv_to_likelihood import data_to_likelihood, data_frame_to_likelihood
from tests.data_generator.data_gen_source.generate_data_CSV import generate_gaussian
from tests.data_generator.data_gen_test.seed_string_to_int import hash_32_unsigned
from tests.stochastic import stochastic

import bayeslite


@stochastic(max_runs=1, min_passes=1)
def test_cgpm_likelihood(seed):
    likelihood_helper(seed, backend_type="cgpm", iterations=1000)


@stochastic(max_runs=1, min_passes=1)
def test_loom_likelihood(seed):
    likelihood_helper(seed, backend_type="loom", iterations=1000)


def likelihood_helper(seed, backend_type="cgpm", iterations=1000):
    seed = hash_32_unsigned(seed)

    # generate data
    parameters = [[[0, 5], [0.4, 0.4]], [[10, 15], [0.1, 0.1]]]
    generate_gaussian(gaussian_parameters=parameters,
                      rows=1000,
                      out_path=backend_type + "_gaussian.csv", seed=seed)
    likelihood = data_to_likelihood(in_path=backend_type + "_gaussian.csv", data_type="gaussian",
                                    generator_parameters=parameters,
                                    out_path=backend_type + "_gaussian_likelihood.csv")
    true_complete_likelihood = sum(likelihood["probability"])

    # delete old bdb if it exists
    try:
        os.remove(backend_type + "_gaussian.bdb")
    except OSError:
        pass
    bdb = bayeslite.bayesdb_open(pathname=backend_type + "_gaussian.bdb")

    # start timing, defensive programming, not sure where inference starts
    times = []
    scores = []
    start_time = timer()

    # load tables
    bdb.execute("CREATE TABLE gaussian_full FROM '" + backend_type + "_gaussian.csv'")
    bdb.sql_execute("CREATE TABLE rowids_train AS SELECT rowid FROM gaussian_full ORDER BY random() LIMIT 800")
    bdb.sql_execute("CREATE TABLE rowids_test AS SELECT rowid FROM gaussian_full WHERE rowid NOT IN rowids_train")
    bdb.sql_execute("CREATE TABLE gaussian_train AS SELECT rowid, * FROM gaussian_full WHERE rowid IN rowids_train")
    bdb.sql_execute("CREATE TABLE gaussian_test AS SELECT rowid, * FROM gaussian_full WHERE rowid IN rowids_test")

    # create population and generator
    bdb.execute("CREATE POPULATION FOR gaussian_train (GUESS STATTYPES OF (*););")
    bdb.execute("CREATE GENERATOR FOR gaussian_train;")
    bdb.execute("INITIALIZE 10 MODELS FOR gaussian_train;")

    # analyze data

    for i in range(iterations):
        if backend_type != "loom":  # loom always has multiprocess on
            backend = bdb.backends[backend_type]
            backend.set_multiprocess(True)
        bdb.execute("ANALYZE gaussian_train FOR 1 ITERATIONS (OPTIMIZED);")
        if backend_type != "loom":
            backend.set_multiprocess(False)

        # simulate data at current level of analysis
        simulate = pd.DataFrame(bdb.execute("SIMULATE \"0\", \"1\" FROM gaussian_train LIMIT 1000").fetchall())
        predicted_likelihood = data_frame_to_likelihood(simulate, data_type="gaussian", generator_parameters=parameters)
        predicted_complete_likelihood = sum(predicted_likelihood["probability"])
        times.append(timer() - start_time)
        scores.append(predicted_complete_likelihood)

    # plot probabilities
    probability_fig, probability_ax = plt.subplots()

    probability_ax.plot([0, times[-1]], [true_complete_likelihood, true_complete_likelihood],
                        'r-', label='True Likelihood', linewidth=2)

    probability_ax.scatter(times, scores)
    probability_ax.set_title(backend_type + " Simulate Performance over Time")
    probability_ax.set_xlabel("Time (s)")
    probability_ax.set_ylabel("Likelihood (log PDF) of Simulated Values in True Model")

    plt.savefig(backend_type + "_simulate_performance_over_time.png", bbox_inches='tight')
