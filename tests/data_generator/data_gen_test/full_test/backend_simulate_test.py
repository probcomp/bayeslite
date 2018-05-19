import shutil

import numpy as np
import pandas as pd
import sys
from matplotlib import pyplot as plt
import os
from timeit import default_timer as timer
import math

from tests.data_generator.data_gen_source.csv_to_likelihood import data_to_likelihood, data_frame_to_likelihood
from tests.data_generator.data_gen_source.generate_data_CSV import generate_gaussian, generate_categorical
from tests.data_generator.data_gen_test.seed_string_to_int import hash_32_unsigned
from tests.stochastic import stochastic

import bayeslite


"""
Tests for backend performance in simulating values

1. Data is sampled from a separate Gaussian or Categorical mixture model
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
def test_cgpm_gaussian_simulate(seed):
    simulate_helper(seed, data_type="gaussian", backend_type="cgpm", iterations=700, sample_size=1000,
                    threshold=700, measure_start_time=500)


@stochastic(max_runs=1, min_passes=1)
def test_loom_gaussian_simulate(seed):
    simulate_helper(seed, data_type="gaussian", backend_type="loom", iterations=700, sample_size=1000,
                    threshold=700, measure_start_time=500)


# @stochastic(max_runs=1, min_passes=1)
# def test_cgpm_categorical_simulate(seed):
#     simulate_helper(seed, data_type="categorical", backend_type="cgpm", iterations=700, sample_size=700,
#                     threshold=700, measure_start_time=500)
#
#
# @stochastic(max_runs=1, min_passes=1)
# def test_loom_categorical_simulate(seed):
#     simulate_helper(seed, data_type="categorical", backend_type="loom", iterations=700, sample_size=700,
#                     threshold=700, measure_start_time=500)


def simulate_helper(seed, data_type, backend_type, iterations, sample_size, threshold, measure_start_time):
    seed = hash_32_unsigned(seed)

    if not os.path.exists("simulate_data"):
        os.makedirs("simulate_data")

    if os.path.exists("simulate_data/inf_data"):
        shutil.rmtree("simulate_data/inf_data")  # asynchronous
        while os.path.exists("simulate_data/inf_data"):
            pass

    os.makedirs("simulate_data/inf_data")

    # generate data
    if data_type == "categorical":
        comp_1 = [[0.1] * 10, [0.005] * 50 + [0.5] + [0.005] * 50]
        comp_2 = [[0.002] * 500, [0, 0, 0, 0.9, 0.1]]
        parameters = [comp_1, comp_2]
        data = generate_categorical(categorical_parameters=parameters,
                                    rows=sample_size,
                                    out_path="simulate_data/" + backend_type + "_categorical_test_simulate.csv", seed=seed)
    else:
        parameters = [[[0, 5], [0.4, 0.4]], [[10, 15], [0.1, 0.1]]]
        data = generate_gaussian(gaussian_parameters=parameters,
                                 rows=sample_size,
                                 out_path="simulate_data/" + backend_type + "_gaussian_test_simulate.csv", seed=seed)

    likelihood = data_to_likelihood(in_path="simulate_data/" + backend_type +
                                            "_" + data_type + "_test_simulate.csv", data_type=data_type,
                                    generator_parameters=parameters,
                                    out_path="simulate_data/" + backend_type +
                                             "_" + data_type + "_likelihood_test_simulate.csv")

    true_complete_likelihood = sum(likelihood["probability"])

    # delete old bdb if it exists
    try:
        os.remove("simulate_data/" + backend_type + "_" + data_type + "_test_simulate.bdb")
    except OSError:
        pass
    bdb = bayeslite.bayesdb_open(pathname="simulate_data/" + backend_type + "_" + data_type + "_test_simulate.bdb")

    # start timing, defensive programming, not sure where inference starts
    times = []
    scores = []
    start_time = timer()

    # load tables
    bdb.execute("CREATE TABLE data_full FROM '" + "simulate_data/" + backend_type +
                "_" + data_type + "_test_simulate.csv'")

    # create population and generator
    numerical_nominal = "NUMERICAL" if data_type == "gaussian" else "NOMINAL"
    bdb.execute("CREATE POPULATION FOR data_full (GUESS STATTYPES OF (*);"
                "SET STATTYPE OF \"0\" TO " + numerical_nominal + ";"
                "SET STATTYPE OF \"1\" TO " + numerical_nominal + ");")
    bdb.execute("CREATE GENERATOR FOR data_full;")
    bdb.execute("INITIALIZE 10 MODELS FOR data_full;")

    # analyze data
    first_recorded = False  # for recording first simulate after measure_start_time seconds
    for i in range(iterations):
        print("iteration: " + str(i+1))
        if backend_type != "loom":  # loom always has multiprocess on
            backend = bdb.backends[backend_type]
            backend.set_multiprocess(True)
        bdb.execute("ANALYZE data_full FOR 1 ITERATIONS (OPTIMIZED; QUIET);")
        if backend_type != "loom":
            backend.set_multiprocess(False)

        # simulate data at current level of analysis
        simulate = pd.DataFrame(bdb.execute("SIMULATE \"0\", \"1\" FROM data_full "
                                            "LIMIT " + str(sample_size)).fetchall())
        simulate_likelihood = data_frame_to_likelihood(simulate, data_type=data_type,
                                                       generator_parameters=parameters)
        simulate_complete_likelihood = sum(simulate_likelihood["probability"])

        time = timer() - start_time
        # record first result in simulate vs select scatter plot
        # also if negative infinity result after convergence
        if time > measure_start_time and (not first_recorded or simulate_complete_likelihood == float("-inf")):
            first_recorded = True
            select_x = data.iloc[:, 0].tolist()
            select_y = data.iloc[:, 1].tolist()

            scatter_fig, scatter_ax = plt.subplots(1, 2, sharey=True, sharex=True)
            scatter_fig.suptitle("Select vs Simulate at Convergence")
            select_ax = scatter_ax[0]
            select_ax.scatter(select_x, select_y)
            select_ax.set_title("Select")
            select_ax.set_xlabel("x")
            select_ax.set_ylabel("y")

            simulate_ax = scatter_ax[1]
            simulate_ax.scatter(simulate.iloc[:, 0].tolist(), simulate.iloc[:, 1].tolist())
            simulate_ax.set_title("Simulate")
            simulate_ax.set_xlabel("x")
            simulate_ax.set_ylabel("y")

            if simulate_complete_likelihood == float("-inf"):
                simulate.to_csv("simulate_data/inf_data/" + backend_type + "_" +
                                data_type + "_" + str(i+1) + "_data.csv",header=True, index=False)
                simulate_likelihood.to_csv("simulate_data/inf_data/" + backend_type + "_" +
                                           data_type + "_" + str(i+1) + "_likelihood.csv", header=True, index=False)
                scatter_fig.savefig("simulate_data/inf_data/" + backend_type + "_" + str(i+1) + ".png")
            else:
                scatter_fig.savefig("simulate_data/" + backend_type + "_select_vs_simulate.png")

        times.append(time)
        scores.append(simulate_complete_likelihood)

    # save results
    results = pd.DataFrame(columns=["time (s)", "simulated values likelihood (log pdf)",
                                    "true likelihood (log pdf)"])
    results["time (s)"] = times
    results["simulated values likelihood (log pdf)"] = scores
    results["true likelihood (log pdf)"] = [true_complete_likelihood] * len(times)

    results.to_csv("simulate_data/" + backend_type + "_simulate_results.csv", index=False)

    # plot probabilities
    scores_with_true = scores[:]
    scores_with_true.append(true_complete_likelihood)
    probability_fig, probability_ax = plt.subplots()
    if data_type == "gaussian":
        probability_ax.set_xlim(0, 700)
        probability_ax.set_ylim(-100000, 0)

    probability_ax.plot([0, 700], [true_complete_likelihood, true_complete_likelihood],
                        'r-', label='True Likelihood', linewidth=2)

    probability_ax.scatter(times, scores)
    probability_ax.set_title(backend_type + " Simulate Performance over Time")
    probability_ax.set_xlabel("Time (s)")
    probability_ax.set_ylabel("Likelihood (log PDF) of Simulated Values in True Model")

    plt.savefig("simulate_data/" + backend_type + "_" + data_type +
                "_simulate_performance_over_time.png", bbox_inches='tight')

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
