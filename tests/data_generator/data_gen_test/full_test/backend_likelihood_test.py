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


"""
Tests for backend performance in calculating likelihood

1. Data is sampled from a separate Gaussian mixture model
2. BayesDB analyzes the sampled data and gets likelihood of the data
3. The likelihood of BayesDB is compared with the true likelihood from the Gaussian mixture model

Parameters:
iterations: number of analysis iterations the backend will complete
sample_size: number of values to sample and simulate
threshold: maximum allowed difference in likelihood (log pdf) between simulated and sampled data
measure_start_time: time (s) to verify likelihood is in threshold i.e. when BayesDB's simulate 
                    performance plateaus and further analysis doesn't improve it
"""


@stochastic(max_runs=1, min_passes=1)
def test_cgpm_likelihood(seed):
    likelihood_helper(seed, backend_type="cgpm", iterations=300, sample_size=200,
                      threshold=700, measure_start_time=200)


@stochastic(max_runs=1, min_passes=1)
def test_loom_likelihood(seed):
    likelihood_helper(seed, backend_type="loom", iterations=300, sample_size=200,
                      threshold=700, measure_start_time=200)


def likelihood_helper(seed, backend_type, iterations, sample_size, threshold, measure_start_time):
    seed = hash_32_unsigned(seed)

    if not os.path.exists("likelihood_data"):
        os.makedirs("likelihood_data")

    # generate data
    parameters = [[[0, 5], [0.4, 0.4]], [[10, 15], [0.1, 0.1]]]
    generate_gaussian(gaussian_parameters=parameters,
                      rows=sample_size,
                      out_path="likelihood_data/" + backend_type + "_gaussian_test_likelihood.csv", seed=seed)
    likelihood = data_to_likelihood(in_path="likelihood_data/" + backend_type + "_gaussian_test_likelihood.csv",
                                    data_type="gaussian",
                                    generator_parameters=parameters,
                                    out_path="likelihood_data/" + backend_type +
                                             "_gaussian_likelihood_test_likelihood.csv")
    true_complete_likelihood = sum(likelihood["probability"])

    # delete old bdb if it exists
    try:
        os.remove("likelihood_data/" + backend_type + "_gaussian_test_likelihood.bdb")
    except OSError:
        pass
    bdb = bayeslite.bayesdb_open(pathname="likelihood_data/" + backend_type + "_gaussian_test_likelihood.bdb")

    # start timing, defensive programming, not sure where inference starts
    times = []
    scores = []
    start_time = timer()

    # load tables
    bdb.execute("CREATE TABLE gaussian_full FROM '" + "likelihood_data/" + backend_type +
                "_gaussian_test_likelihood.csv'")

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

        predict_time = timer()
        predicted_likelihood = pd.DataFrame(bdb.execute("ESTIMATE PREDICTIVE PROBABILITY OF * " +
                                                        "FROM \"gaussian_full\"").fetchall())
        end_predict = timer()
        print(end_predict - predict_time)

        # BayesDB gives regular likelihood, not log
        predicted_likelihood[0] = predicted_likelihood[0].apply(np.log)
        predicted_complete_likelihood = sum(predicted_likelihood[0])

        times.append(timer() - start_time)
        scores.append(predicted_complete_likelihood)

    # plot probabilities
    probability_fig, probability_ax = plt.subplots()

    probability_ax.plot([0, times[-1]], [true_complete_likelihood, true_complete_likelihood],
                        'r-', label='True Likelihood', linewidth=2)

    probability_ax.scatter(times, scores)
    probability_ax.set_title(backend_type + " Likelihood Performance over Time")
    probability_ax.set_xlabel("Time (s)")
    probability_ax.set_ylabel("Predicted Likelihood (log PDF) of Samples")

    plt.savefig("likelihood_data/" + backend_type + "_likelihood_performance_over_time.png", bbox_inches='tight')

    results = pd.DataFrame(columns=["time (s)", "predicted likelihood (log pdf)", "true likelihood (log pdf)"])
    results["time (s)"] = times
    results["predicted likelihood (log pdf)"] = scores
    results["true likelihood (log pdf)"] = [true_complete_likelihood] * len(times)

    results.to_csv("likelihood_data/" + backend_type + "_likelihood_results.csv", index=False)

    verify_results(results, backend_type, threshold, measure_start_time)


def verify_results(results, backend_type, threshold, measure_start_time):
    segment_size = 1

    true_likelihood = results["true likelihood (log pdf)"][0]

    start_index = search_time(measure_start_time, results)
    exact_start_time = results["time (s)"].tolist()[start_index]
    segmented_likelihoods = segmented_average_likelihood(start_index, segment_size, results)

    for segment in segmented_likelihoods:
        if abs(abs(true_likelihood) - abs(segment)) > threshold:
            raise Exception("likelihood performance worse than baseline. Backend: " + backend_type +
                            ". Predicted Likelihood Average (log pdf) after " + str(exact_start_time) +
                            "s:" + str(segment) + ". True Likelihood (log pdf): " +
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

    likelihood = results["predicted likelihood (log pdf)"].tolist()

    while start_index + segment_size < len(likelihood):
        segment = likelihood[start_index:start_index+segment_size]
        segment = [value for value in segment if value != float("-inf")]

        out.append(np.average(segment))
        start_index += segment_size

    return out
