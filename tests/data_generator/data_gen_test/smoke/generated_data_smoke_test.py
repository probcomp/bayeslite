import os

import bayeslite
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from tests.data_generator.data_gen_source.csv_to_likelihood import data_to_likelihood
from tests.data_generator.data_gen_source.generate_data_CSV import generate_gaussian
from tests.data_generator.data_gen_test.seed_string_to_int import hash_32_unsigned
from tests.stochastic import stochastic

@stochastic(max_runs=1, min_passes=1)
def test_generated_data_smoke(seed):
    seed = hash_32_unsigned(seed)

    # generate data
    parameters = [[[0, 5], [0.4, 0.4]], [[10, 15], [0.1, 0.1]]]
    generate_gaussian(gaussian_parameters=parameters,
                      rows=1000,
                      out_path="smoke_gaussian.csv", seed=seed)
    data_to_likelihood(in_path="smoke_gaussian.csv", data_type="gaussian",
                       generator_parameters=parameters,
                       out_path="smoke_gaussian_likelihood.csv")

    # delete old bdb if it exists
    try:
        os.remove("smoke_gaussian.bdb")
    except OSError:
        pass
    bdb = bayeslite.bayesdb_open(pathname='smoke_gaussian.bdb')

    # load tables
    bdb.execute("CREATE TABLE gaussian_full FROM 'smoke_gaussian.csv'")
    bdb.sql_execute("CREATE TABLE rowids_train AS SELECT rowid FROM gaussian_full ORDER BY random() LIMIT 800")
    bdb.sql_execute("CREATE TABLE rowids_test AS SELECT rowid FROM gaussian_full WHERE rowid NOT IN rowids_train")
    bdb.sql_execute("CREATE TABLE gaussian_train AS SELECT rowid, * FROM gaussian_full WHERE rowid IN rowids_train")
    bdb.sql_execute("CREATE TABLE gaussian_test AS SELECT rowid, * FROM gaussian_full WHERE rowid IN rowids_test")

    # create population and generator
    bdb.execute("CREATE POPULATION FOR gaussian_train (GUESS STATTYPES OF (*););")
    bdb.execute("CREATE GENERATOR FOR gaussian_train;")
    bdb.execute("INITIALIZE 5 MODELS FOR gaussian_train;")

    # analyze data
    backend = bdb.backends["cgpm"]  # loom always has multiprocess on
    backend.set_multiprocess(True)
    bdb.execute("ANALYZE gaussian_train FOR 200 ITERATIONS (OPTIMIZED);")
    backend.set_multiprocess(False)

    # insert testing data
    bdb.sql_execute("INSERT INTO gaussian_train SELECT * FROM gaussian_test;")

    # get columns with dependence >= 0.8
    features = bdb.execute("SELECT * FROM (ESTIMATE \"name\", DEPENDENCE PROBABILITY WITH \"0\" "
                           "AS \"dependence with 0\" FROM VARIABLES OF \"gaussian_train\" "
                           "ORDER BY \"dependence with 0\" DESC) WHERE \"dependence with 0\" >= 0.8")
    data = pd.DataFrame(features.fetchall())

    # nullify feature columns
    first = True
    for _, f in data.iterrows():
        if first:
            first = False
            continue
        name = f[0]
        bdb.sql_execute("UPDATE gaussian_train SET \"" + name + "\" = NULL WHERE rowid IN rowids_test")

    # prediction with nullified columns
    bdb.execute("CREATE TABLE predictions_without_features AS INFER EXPLICIT \"0\" AS \"True 0\", PREDICT \"0\" "
                "AS \"Predicted 0\" USING 100 SAMPLES FROM gaussian_train WHERE rowid IN (SELECT * FROM rowids_test)")

    # reinsert nullified feature columns
    for _, f in data.iterrows():
        name = f[0]
        bdb.sql_execute("UPDATE gaussian_train SET \"" + name + "\" = "
                        "(SELECT \"" + name + "\" FROM gaussian_test "
                        "WHERE gaussian_test.rowid = gaussian_train.rowid)"
                        " WHERE rowid IN rowids_test")

    # prediction with feature columns
    bdb.execute("CREATE TABLE predictions_with_features AS INFER EXPLICIT \"0\" AS \"True 0\", PREDICT \"0\" "
                "AS \"Predicted 0\" USING 100 SAMPLES FROM gaussian_train WHERE rowid IN (SELECT * FROM rowids_test)")

    # get real data for side-by-side comparison with simulated data
    select_x = pd.DataFrame(bdb.sql_execute("SELECT \"0\" FROM gaussian_full").fetchall())[0].tolist()
    select_y = pd.DataFrame(bdb.sql_execute("SELECT \"1\" FROM gaussian_full").fetchall())[0].tolist()
    simulate = pd.DataFrame(bdb.execute("SIMULATE \"0\", \"1\" FROM gaussian_train LIMIT 1000").fetchall())

    # graph set up
    combined_fig, combined_ax = plt.subplots(1, 2, sharey=True, sharex=True)

    select_ax = combined_ax[0]
    select_ax.scatter(select_x, select_y)
    select_ax.set_title("Select")
    select_ax.set_xlabel("x")
    select_ax.set_ylabel("y")

    simulate_ax = combined_ax[1]
    simulate_ax.scatter(simulate[0].tolist(), simulate[1].tolist())
    simulate_ax.set_title("Simulate")
    simulate_ax.set_xlabel("x")
    simulate_ax.set_ylabel("y")

    plt.savefig("smoke_simulate_comparison.png")

    # true likelihood
    bdb.execute("CREATE TABLE true_probability_rows FROM \'smoke_gaussian_likelihood.csv\'")
    true_likelihood = pd.DataFrame(
        bdb.sql_execute("SELECT * FROM true_probability_rows WHERE rowid in rowids_test").fetchall())

    # predicted likelihood
    bdb.execute("CREATE TABLE predictive_probability_rows AS ESTIMATE rowid, PREDICTIVE PROBABILITY "
                "OF * AS probability FROM \"gaussian_train\"")
    predicted_likelihood = pd.DataFrame(bdb.sql_execute(
        "SELECT * FROM predictive_probability_rows WHERE rowid in rowids_test").fetchall())
    del predicted_likelihood[0]  # delete row id column
    predicted_likelihood.columns = [0]

    predicted_likelihood[0] = predicted_likelihood[0].apply(np.log) # BayesDB gives regular likelihood, not log

    # plot probabilities
    probability_fig, probability_ax = plt.subplots(1, 2, sharey=True, sharex=True)

    true_ax = probability_ax[0]
    true_ax.plot(true_likelihood[0])
    true_ax.set_title("True Likelihood")
    true_ax.set_xlabel("x")
    true_ax.set_ylabel("y")

    predicted_ax = probability_ax[1]
    predicted_ax.plot(predicted_likelihood[0])
    predicted_ax.set_title("Predicted Likelihood")
    predicted_ax.set_xlabel("x")
    predicted_ax.set_ylabel("y")

    plt.savefig("smoke_likelihood_comparison.png")
