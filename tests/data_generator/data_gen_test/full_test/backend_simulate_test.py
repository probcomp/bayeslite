import numpy as np
import pandas as pd
import sys
from matplotlib import pyplot as plt
import os
from timeit import default_timer as timer
import math

from tests.data_generator.data_gen_source.csv_to_likelihood import data_to_likelihood, data_frame_to_likelihood
from tests.data_generator.data_gen_source.generate_data_CSV import generate_gaussian
from tests.data_generator.data_gen_test.seed_string_to_int import hash_32_unsigned
from tests.stochastic import stochastic

import bayeslite


"""
Tests for backend performance in simulating values

1. Data is sampled from a separate Gaussian mixture model
2. BayesDB analyzes the sampled data and simulates its own values
3. The likelihood of the simulated values is compared to the likelihood of the sampled data

Parameters:
iterations: number of analysis iterations the backend will complete
sample_size: number of values to sample and simulate
threshold: maximum allowed difference in likelihood (log pdf) between simulated and sampled data
measure_start_time: time (s) to verify likelihood is in threshold i.e. when BayesDB's simulate 
                    performance plateaus and further analysis doesn't improve it
"""


@stochastic(max_runs=1, min_passes=1)
def test_cgpm_simulate(seed):
    simulate_helper(seed, backend_type="cgpm", iterations=10, sample_size=1000,
                    threshold=700, measure_start_time=500)


@stochastic(max_runs=1, min_passes=1)
def test_loom_simulate(seed):
    simulate_helper(seed, backend_type="loom", iterations=10, sample_size=1000,
                    threshold=700, measure_start_time=500)


def simulate_helper(seed, backend_type, iterations, sample_size, threshold, measure_start_time):
    seed = hash_32_unsigned(seed)

    if not os.path.exists("simulate_data"):
        os.makedirs("simulate_data")

    # generate data
    parameters = [[[0, 5], [0.4, 0.4]], [[10, 15], [0.1, 0.1]]]
    generate_gaussian(gaussian_parameters=parameters,
                      rows=sample_size,
                      out_path="simulate_data/" + backend_type + "_gaussian_test_simulate.csv", seed=seed)
    likelihood = data_to_likelihood(in_path="simulate_data/" + backend_type + "_gaussian_test_simulate.csv",
                                    data_type="gaussian",
                                    generator_parameters=parameters,
                                    out_path="simulate_data/" + backend_type +
                                             "_gaussian_likelihood_test_simulate.csv")
    true_complete_likelihood = sum(likelihood["probability"])

    # delete old bdb if it exists
    try:
        os.remove("simulate_data/" + backend_type + "_gaussian_test_simulate.bdb")
    except OSError:
        pass
    bdb = bayeslite.bayesdb_open(pathname="simulate_data/" + backend_type + "_gaussian_test_simulate.bdb")

    # start timing, defensive programming, not sure where inference starts
    times = []
    scores = []
    start_time = timer()

    # load tables
    bdb.execute("CREATE TABLE gaussian_full FROM '" + "simulate_data/" + backend_type +
                "_gaussian_test_simulate.csv'")

    # create population and generator
    bdb.execute("CREATE POPULATION FOR gaussian_full (GUESS STATTYPES OF (*););")
    bdb.execute("CREATE GENERATOR FOR gaussian_full;")
    bdb.execute("INITIALIZE 10 MODELS FOR gaussian_full;")

    # analyze data

    for i in range(iterations):
        print("iteration: " + str(i))
        if backend_type != "loom":  # loom always has multiprocess on
            backend = bdb.backends[backend_type]
            backend.set_multiprocess(True)
        bdb.execute("ANALYZE gaussian_full FOR 1 ITERATIONS (OPTIMIZED; QUIET);")
        if backend_type != "loom":
            backend.set_multiprocess(False)

        # simulate data at current level of analysis
        simulate = pd.DataFrame(bdb.execute("SIMULATE \"0\", \"1\" FROM gaussian_full "
                                            "LIMIT " + str(sample_size)).fetchall())
        simulate_likelihood = data_frame_to_likelihood(simulate, data_type="gaussian",
                                                       generator_parameters=parameters)
        simulate_complete_likelihood = sum(simulate_likelihood["probability"])
        times.append(timer() - start_time)
        scores.append(simulate_complete_likelihood)

    # plot probabilities
    probability_fig, probability_ax = plt.subplots()

    probability_ax.plot([0, times[-1]], [true_complete_likelihood, true_complete_likelihood],
                        'r-', label='True Likelihood', linewidth=2)

    probability_ax.scatter(times, scores)
    probability_ax.set_title(backend_type + " Simulate Performance over Time")
    probability_ax.set_xlabel("Time (s)")
    probability_ax.set_ylabel("Likelihood (log PDF) of Simulated Values in True Model")

    plt.savefig("simulate_data/" + backend_type + "_simulate_performance_over_time.png", bbox_inches='tight')

    results = pd.DataFrame(columns=["time (s)", "simulated values likelihood (log pdf)",
                                    "true likelihood (log pdf)"])
    results["time (s)"] = times
    results["simulated values likelihood (log pdf)"] = scores
    results["true likelihood (log pdf)"] = [true_complete_likelihood] * len(times)

    results.to_csv("simulate_data/" + backend_type + "_simulate_results.csv", index=False)

    verify_results(results, backend_type, threshold, measure_start_time)


def verify_results(results, backend_type, threshold, measure_start_time):
    segment_size = 10

    true_likelihood = results["true likelihood (log pdf)"][0]

    start_index = search_time(measure_start_time, results)
    exact_start_time = results["time (s)"].tolist()[start_index]
    segmented_likelihoods = segmented_average_likelihood(start_index, segment_size, results)
    segmented_likelihoods = [seg for seg in segmented_likelihoods if not math.isnan(seg)]
    print(segmented_likelihoods)

    for segment in segmented_likelihoods:
        if abs(abs(true_likelihood) - abs(segment)) > threshold:
            raise Exception("simulate performance worse than baseline. Backend: " + backend_type +
                            ". Simulated Data Likelihood Average (log pdf) after " + str(exact_start_time) +
                            "s: " + str(segment) + ". True Likelihood (log pdf): " +
                            str(true_likelihood))


def search_time(measure_start_time, results):
    """
    Finds the index of the lowest time within the data that is above a certain minimum
    :param measure_start_time: minimum
    :param results: data
    :return: index of the lowest time in data above minimum e.g.
                measure_start_time = 3, results = [2.9, 3.3, 4.5] gives 1 (index of 3.3)
    """
    time_data = results["time (s)"].tolist()

    # longest time is longer than what data has
    if time_data[-1] < measure_start_time:
        return len(time_data)-1

    shift = int(len(time_data) // 2)
    i = int(len(time_data) // 2)

    while not (time_data[i-1] < measure_start_time < time_data[i]) and (0 < i < len(time_data)):
        shift = max(shift / 2, 1)
        if time_data[i] > measure_start_time:
            shift = -shift
        i += shift

    return i


def segmented_average_likelihood(start_index, segment_size, results):
    """
    Splits the data from an index to the end into segments of a certain size,
    and takes the average of each segments likelihood

    Note: drops the last segmented segment if its length !== segment_size

    :param start_index: to start segmentation
    :param segment_size: of each segment
    :param results: data
    :return: list of the average likelihood for each segment
    """
    out = []

    likelihood = results["simulated values likelihood (log pdf)"].tolist()

    while start_index + segment_size < len(likelihood):
        segment = likelihood[start_index:start_index+segment_size]
        segment = [value for value in segment if value != float("-inf")]

        out.append(np.average(segment))
        start_index += segment_size

    return out
