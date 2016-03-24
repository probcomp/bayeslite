# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2016, MIT Probabilistic Computing Project
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

"""Utilities for Geweke-style testing of Bayeslite metamodels.

This module implements an end-to-end test in the style introduced by
[1], that can be used to look for evidence of bugs in Bayesian
metamodels.

Briefly, the idea is this.  A Bayesian metamodel contains a prior
p(theta) on some latent structure theta, which can be elicited by
creating a generator and initializing models against an empty data
table.  Such a metamodel also contains a data synthesizing
distribution p(d|theta), elicited by invoking SIMULATE, and a
data-dependent learning operator T_d(theta'|theta), which can be
elicited by ANALYZE.

This means we can sample from the model's joint distribution on
synthetic data in two different ways:

A. Draw theta ~ p(theta)
   Draw d     ~ p(d|theta)

B. Draw theta_0 ~ p(theta)
   K times:
     Draw d_i     ~ p(d|theta_i-1)
     Draw theta_i ~ T_d_i(theta|theta_i-1)
   Draw d ~ p(d|theta_K)

If the samplers for p(theta), p(d|theta), and T_d(theta'|theta) are
all implemented correctly, and if T_d(.|theta) is a transition
operator for a Markov chain that leaves p(theta|d) stationary,
distributions A and B should be the same for all K.  For a full
discussion, see [1], and for more intuition, see [2].  In the
documentation of this module, we will refer to B as a Geweke chain.

[1] J. Geweke. Getting it right: joint distribution tests of posterior
simulators. JASA, 2004.
http://qed.econ.queensu.ca/pub/faculty/ferrall/quant/papers/04_04_29_geweke.pdf

[2] https://hips.seas.harvard.edu/blog/2013/06/10/testing-mcmc-code-part-2-integration-tests/
"""

import math

import bayeslite.bql as bql
import bayeslite.core as core
import bayeslite.stats as stats

from bayeslite.sqlite3_util import sqlite3_quote_name

def create_empty_table(bdb, column_names):
    """Create a fresh empty table with the given column names.

    Give all the columns a NUMERIC data type in the underlying SQL.
    Return the name of the new table.
    """
    table = bdb.temp_table_name()
    qt = sqlite3_quote_name(table)
    qcns = map(sqlite3_quote_name, column_names)
    schema = ','.join('%s NUMERIC' % (qcn,) for qcn in qcns)
    bdb.sql_execute('CREATE TABLE %s(%s)' % (qt, schema))
    core.bayesdb_table_guarantee_columns(bdb, table)
    return table

def create_temp_gen(bdb, table, target_metamodel, schema):
    """Create a generator.

    :param BayesDB bdb: The BayesDB instance.
    :param string table: Name of table for generator.
    :param IBayesDBMetamodel target_metamodel: Metamodel for generator.
    :param list schema: A valid schema for that metamodel.
    :return: A :class:`Generator` representing the resulting
        generator.
    """
    gen_name = bdb.temp_table_name()
    gen_id_box = [None]
    def instantiate(columns):
        gen_id, column_list = bql.instantiate_generator(bdb, gen_name, table,
            target_metamodel, columns,
            default=True)
        gen_id_box[0] = gen_id
        return gen_id, column_list
    with bdb.savepoint():
        target_metamodel.create_generator(bdb, table, schema, instantiate)
    return Generator(bdb, target_metamodel, gen_id_box[0], gen_name)

class Generator(object):
    """A representation of a BayesDB generator.

    Knows its Bayeslite handle, its metamodel, its generator_id, and
    its name, and forwards methods to its metamodel, supplying the
    former two as additional arguments.
    """
    def __init__(self, bdb, metamodel, generator_id, name):
        self.bdb = bdb
        self.metamodel = metamodel
        self.generator_id = generator_id
        self.name = name

    def __getattr__(self, name):
        mm_attr = getattr(self.metamodel, name)
        def f(*args, **kwargs):
            return mm_attr(self.bdb, self.generator_id, *args, **kwargs)
        return f

def create_prior_gen(bdb, target_metamodel, schema, column_names,
                     prior_samples):
    table = create_empty_table(bdb, column_names)
    prior_gen = create_temp_gen(bdb, table, target_metamodel, schema)
    bdb.execute('INITIALIZE %s MODELS FOR %s' %
        (prior_samples, sqlite3_quote_name(prior_gen.name)))
    return prior_gen

def create_geweke_chain_gen(bdb, target_metamodel, schema, column_names,
                            target_cells, geweke_samples, geweke_iterates):
    table = create_empty_table(bdb, column_names)
    geweke_chain_gen = create_temp_gen(bdb, table, target_metamodel, schema)
    bdb.execute('INITIALIZE %s MODELS FOR %s' %
        (geweke_samples, sqlite3_quote_name(geweke_chain_gen.name)))
    for _ in range(geweke_iterates):
        for modelno in range(geweke_samples):
            # Need each Geweke chain to hallucinate its own data.
            # Doing it by model-controlled simulation and inference in
            # one generator.  This does rely on insert-remove
            # invariance for the models that are not analyzed.
            # An alternative would have been to create N generators,
            # each with 1 model.  As of this writing, that feels
            # gottier, because I would need to adjust the KL
            # computation to aggregate them.
            [data] = geweke_chain_gen.simulate_joint(
                target_cells, [], modelno=modelno, num_predictions=1)
            for ((i, j), datum) in zip(target_cells, data):
                geweke_chain_gen.insert((i, j, datum))
            geweke_chain_gen.analyze_models(modelnos=[modelno])
            for ((i, j), datum) in zip(target_cells, data):
                geweke_chain_gen.remove((i, j, datum))
    return geweke_chain_gen

def kl_est_sample(from_gen, of_gen, target_cells, constraints):
    """Estimate Kullback-Liebler divergence of `of_gen` from `from_gen`.

    Specifically, let P be the distribution over the given target
    cells induced by the generator `from_gen` conditioned on the
    constraints, and let Q be same induced by the `of_gen` generator.
    This function computes and returns a one-point Monte-Carlo
    estimate of the K-L of Q from P.
    """
    [data] = from_gen.simulate_joint(target_cells, constraints)
    targeted_data = [(i, j, x) for ((i, j), x) in zip(target_cells, data)]
    from_assessment = from_gen.logpdf_joint(targeted_data, constraints)
    of_assessment   =   of_gen.logpdf_joint(targeted_data, constraints)
    return from_assessment - of_assessment

def estimate_mean(samples):
    """Estimate the mean of a distribution from samples.

    Return the triple (count, mean, error).

    `count` is the number of input samples.

    `mean` is the mean of the samples, which estimates the true mean
    of the distribution.

    `error` is an estimate of the standard deviation of the returned
    `mean`.  This is computed from the variance of the input samples,
    on the assumption that the Central Limit Theorem applies.  This is
    will be so if the underlying distribution has a finite variance,
    and enough samples were drawn.
    """
    (n, mean, stddev) = stats.gauss_suff_stats(samples)
    return (n, mean, stddev / math.sqrt(n))

def geweke_kl(bdb, metamodel_name, schema, column_names, target_cells,
              prior_samples, geweke_samples, geweke_iterates, kl_samples):
    """The Kullback-Leibler divergence of a Geweke chain from the prior.

    :param BayesDB bdb: BayesDB instance.
    :param string metamodel_name: Name of the metamodel to test.  Must
        already be registered with `bdb`.
    :param list schema: A valid parsed schema for the metamodel to
        test.  This will be used as the schema with which test
        generators are instantiated.
    :param list column_names: A list of the names to give to the
        columns of the test data table.  This is somewhat redundant
        with the schema, but cannot actually be derived from it in
        general.
    :param list target_cells: A list of (row_id, col_id) pairs, which
        are the cells to jointly synthesize during the test.
    :param int prior_samples: The number of models to instantiate for
        the prior distribution.
    :param int geweke_samples: The number of independent Geweke chains
        to instantiate.
    :param int geweke_iterates: The number of times to generate
        synthetic data and learn from it.  This is K from the main
        exposition.
    :param int kl_samples: The number of samples to use for the Monte
        Carlo estimate of the K-L divergence.
    :return: A 3-tuple giving information about the Monte Carlo
        estimate of the K-L divergence: The number of samples used to
        form the estimate, the estimate, and the predicted standard
        deviation of the estimate.  See :func:`estimate_mean`.

    `metamodel_name`, `schema`, `column_names`, and `target_cells`
    specify an exact probability distribution that should satisfy the
    Geweke invariant if the metamodel under test is Bayesian and
    correctly implemented.

    `prior_samples`, `geweke_samples`, `geweke_iterates`, and
    `kl_samples` specify cost-accuracy tradeoffs in approximating the
    true K-L divergence between the true test distributions.

    Operates inside a savepoint, which it rolls back before returning
    to avoid changing the database state.  If you want the intermediate
    quantities persisted, use :func:`geweke_kl_persist`.

    What should you expect from calling this?  Raising `kl_samples`
    should make the returned K-L estimates more accurate, but should
    not drive them to zero, because of approximation error from finite
    values of `prior_samples` and `geweke_samples`.  For the same
    reason, the K-L estimated by repeated runs should vary more than
    the reported error estimate, because that error estimate only
    takes into account Monte Carlo integration error, not the actual
    variation in K-Ls of different approximations to the same ideal
    distributions.

    Raising `prior_samples` and `geweke_samples` should drive the
    reported K-L divergence toward zero, if the metamodel under test
    is implemented correctly.

    Raising `geweke_iterates` should not affect the reported K-L
    divergences if the metamodel under test is implemented correctly,
    but is likely to increase them if there is a bug that is amplified
    by repeated data synthesis.

    In general, we advise looking at a tableau of multiple runs
    varying the `prior_samples`, `geweke_samples`, `geweke_iterates`,
    and `kl_samples` parameters to judge whether a problem is
    indicated.  Particularly, it's a good idea to include runs with 0
    `geweke_iterates` in that tableau, as an estimate of the
    approximation error induced by having finite `prior_samples` and
    `geweke_samples`.  We also advise testing a metamodel in multiple
    different regimes (little data, much data, various schemas).

    Final word of caution: This is a diagnostic tool, not a debugging
    aid.  If a problem is indicated, do not try to divine what it is
    from the pattern of reported K-L divergences.  Instrument your
    model, plot quantities of interest, turn off various parts, etc.

    """
    with bdb.savepoint_rollback():
        return geweke_kl_persist(bdb, metamodel_name, schema, column_names,
            target_cells, prior_samples, geweke_samples, geweke_iterates,
            kl_samples)

def geweke_kl_persist(bdb, metamodel_name, schema, column_names, target_cells,
        prior_samples, geweke_samples, geweke_iterates, kl_samples):
    """As geweke_kl, but leave the intermediate products in the database.

    This can be useful for initial exploration of casues of a test
    failure.
    """
    ests = geweke_kl_samples(bdb, metamodel_name, schema, column_names,
        target_cells, prior_samples, geweke_samples, geweke_iterates,
        kl_samples)
    return estimate_mean(ests)

def geweke_kl_samples(bdb, metamodel_name, schema, column_names, target_cells,
        prior_samples, geweke_samples, geweke_iterates, kl_samples):
    """The raw samples for a Geweke K-L estimate.

    See :func:`geweke_kl`.  This is useful for testing whether the
    Central Limit Theorem dominates the error of Monte Carlo
    estimation of the K-L.

    Note: Intermediate database state is not automaticallly cleaned up.
    """
    target_metamodel = bdb.metamodels[metamodel_name]
    prior_gen = create_prior_gen(bdb, target_metamodel, schema, column_names, \
        prior_samples)
    geweke_chain_gen = create_geweke_chain_gen(bdb, target_metamodel, schema, \
        column_names, target_cells, geweke_samples, geweke_iterates)
    return [kl_est_sample(prior_gen, geweke_chain_gen, target_cells, [])
            for _ in range(kl_samples)]
