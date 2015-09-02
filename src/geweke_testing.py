# -*- coding: utf-8 -*-

#   Copyright (c) 2015, MIT Probabilistic Computing Project
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

[1] J. Geweke. Getting it right: joint distribution tests of posterior simulators. JASA, 2004.
http://qed.econ.queensu.ca/pub/faculty/ferrall/quant/papers/04_04_29_geweke.pdf

[2] https://hips.seas.harvard.edu/blog/2013/06/10/testing-mcmc-code-part-2-integration-tests/

"""

import math

import bayeslite.core as core
import bayeslite.ast as ast
import bayeslite.bql as bql
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

def create_generator(bdb, table, target_metamodel, schema):
    """Programmatically create a generator.

    :param BayesDB bdb: The Bayeslite handle where to do this.

    :param string table: The name (not quoted) of the table wherewith
        this generator should be associated.

    :param IBayesDBMetamodel target_metamodel: The metamodel object
        for which to create a generator.

    :param list schema: A valid schema for that metamodel.

    :return: A :class:`Generator` representing the resulting
        generator.

    """
    gen_name = bdb.temp_table_name()
    phrase = ast.CreateGen(default = True,
                           name = gen_name,
                           ifnotexists = False,
                           table = table,
                           metamodel = target_metamodel.name(),
                           schema = schema)
    instantiate = bql.mk_instantiate(bdb, target_metamodel, phrase)
    gen_id_box = [None]
    def new_instantiate(*args, **kwargs):
        # Because there is no other way to capture the generator id
        (new_gen_id, other) = instantiate(*args, **kwargs)
        gen_id_box[0] = new_gen_id
        return (new_gen_id, other)
    with bdb.savepoint():
        target_metamodel.create_generator(bdb, phrase.table, phrase.schema,
            new_instantiate)
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

def create_prior_gen(bdb, target_metamodel, schema, column_names, prior_samples):
    table = create_empty_table(bdb, column_names)
    prior_gen = create_generator(bdb, table, target_metamodel, schema)
    init_models_bql = '''
    INITIALIZE %s MODELS FOR %s
    ''' % (prior_samples, sqlite3_quote_name(prior_gen.name))
    bdb.execute(init_models_bql)
    return prior_gen

def create_geweke_chain_generator(bdb, target_metamodel, schema, column_names,
                                  target_cells, geweke_samples, geweke_iterates):
    table = create_empty_table(bdb, column_names)
    geweke_chain_gen = create_generator(bdb, table, target_metamodel, schema)
    init_models_bql = '''
    INITIALIZE %s MODELS FOR %s
    ''' % (geweke_samples, sqlite3_quote_name(geweke_chain_gen.name))
    bdb.execute(init_models_bql)
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
            data = geweke_chain_gen.simulate_joint(target_cells, [], modelnos=[modelno])
            for ((i, j), datum) in zip(target_cells, data):
                geweke_chain_gen.insert((i, j, datum))
            geweke_chain_gen.analyze_models(modelnos=[modelno])
            for ((i, j), datum) in zip(target_cells, data):
                geweke_chain_gen.remove((i, j, datum))
    return geweke_chain_gen

def kl_est_sample(from_gen, of_gen, target_cells, constraints):
    data = from_gen.simulate_joint(target_cells, constraints)
    targeted_data = [(i, j, x) for ((i, j), x) in zip(target_cells, data)]
    from_assessment = from_gen.logpdf(targeted_data, constraints)
    of_assessment   =   of_gen.logpdf(targeted_data, constraints)
    return from_assessment - of_assessment

def gauss_suff_stats(data):
    """From https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance

    This is the "Online algorithm" by Knuth."""
    n = 0
    mean = 0.0
    total_deviance = 0.0 # n * sigma^2

    for x in data:
        n = n + 1
        delta = x - mean
        mean = mean + delta/n
        total_deviance = total_deviance + delta*(x - mean)

    if n < 1:
        return (n, mean, 0.0)
    else:
        return (n, mean, math.sqrt(total_deviance / float(n)))

def estimate_kl(from_gen, of_gen, target_cells, constraints, kl_samples, self_check=None):
    """Estimate the Kullback-Liebler divergence from the first generator to the second.

    Specifically, let P be the distribution over the given target
    cells induced by the generator ``from_gen`` conditioned on the
    constraints, and let Q be same induced by the ``of_gen`` generator.
    This function computes and returns a ``kl_samples``-point
    Monte-Carlo estimate of the K-L of Q from P, in the form of a
    triple: (num_samples, estimate, estimated error of estimate).  The
    error estimate is computed from the variance of the individual
    point estimates of K-L, on the assumtion that ``kl_samples`` are
    high enough that the distribution on the retuned ``estimate`` is
    Gaussian (as it must become, by the Central Limit Theorem)
    (provided the appropriate moments exist, which we assume is so).

    The ``self_check`` parameter, if supplied, requests a self-check
    report, as follows: Break the ``kl_samples`` samples into
    ``self_check`` independent batches (of size
    ``kl_samples/self_check``), and compute and print the count, mean,
    and error estimate for each batch.  If the resulting means differ
    by significantly more than 2-3x their error estimates, the Central
    Limit Theorem does not dominate yet, and more samples may be in
    order.

    """

    estimates = [kl_est_sample(from_gen, of_gen, target_cells, constraints)
                 for _ in range(kl_samples)]
    (n, mean, stddev) = gauss_suff_stats(estimates)
    if self_check is not None:
        for i in range(self_check):
            start = i * kl_samples / self_check
            stop = (i+1) * kl_samples / self_check
            (ni, meani, stddevi) = gauss_suff_stats(estimates[start:stop])
            print "Monte Carlo self check: %4d samples estimate %9.5f with error %9.5f" % \
                (ni, meani, stddevi / math.sqrt(ni))
    return (n, mean, stddev / math.sqrt(n))

def geweke_kl(bdb, metamodel_name, schema, column_names, target_cells, prior_samples, geweke_samples, geweke_iterates, kl_samples, kl_self_check=None):
    """Estimate the Kullback-Leibler divergence of a Geweke chain from the prior.

    :param BayesDB bdb: Bayeslite database handle where to do the
        test.

    :param string metamodel_name: Name of the metamodel to test.  Must
        already be registered with ``bdb``.

    :param list schema: A valid parsed schema for the metamodel to
        test.  This will be used as the schema with which test
        generators are instantiated.

    :param list column_names: A list of the names to give to the
        columns of the test data table.  This is somewhat redundant
        with the schema, but cannot actually be derived from it in
        general.

    :param list target_cells: A list of (row_id, col_id) pairs, which
        are the cells to synthesize during the test.  You might want
        to specify more than one row to test the joint distribution
        across rows, and to test consistency of inference in the
        presence of larger amounts of (still synthetic) data.

    :param int prior_samples: The number of models to instantiate for
        the prior distribution.

    :param int geweke_samples: The number of independent Geweke chains
        to instantiate.

    :param int geweke_iterates: The number of times to generate
        synthetic data and learn from it.  This is K from the main
        exposition.

    :param int kl_samples: The number of samples to use for the Monte
        Carlo estimate of the K-L divergence.

    :param int kl_self_check: Granularity of self-checking of the
        Monte Carlo estimate of the K-L divergence.  See
        :func:`estimate_kl`.  Skip the self-check if None.

    :return: A 3-tuple giving information about the Monte Carlo
        estimate of the K-L divergence: The number of samples used to
        form the estimate, the estimate, and the predicted standard
        deviation of the estimate.  See :func:`estimate_kl`.

    The ``metamodel_name``, ``schema``, ``column_names``, and
    ``target_cells`` parameters define an exact probability
    distribution that should satisfy the Geweke invariant if the
    metamodel under test is Bayesian and correctly implemented.

    The ``prior_samples``, ``geweke_samples``, ``geweke_iterates``,
    and ``kl_samples`` parameters specify cost-accuracy tradeoffs in
    approximating the true K-L divergence between the true test
    distributions.

    What should you expect from calling this?  Raising the
    ``kl_samples`` should make the returned K-L estimates more
    accurate, but should not drive them to zero, because of
    approximation error from finite values of ``prior_samples`` and
    ``geweke_samples``.  For the same reason, the K-L estimated by
    repeated runs should vary more than the reported error estimate,
    because that error estimate only takes into account Monte Carlo
    integration error, not the actual variation in K-Ls of different
    approximations to the same ideal distributions.

    Raising ``prior_samples`` and ``geweke_samples`` should drive the
    reported K-L divergence toward zero, if the metamodel under test
    is implemented correctly.

    Raising ``geweke_iterates`` should not affect the reported K-L
    divergences if the metamodel under test is implemented correctly,
    but is likely to increase them if there is a bug that is amplified
    by repeated data synthesis.

    In general, we advise looking at a tableau of multiple runs
    varying the ``prior_samples``, ``geweke_samples``,
    ``geweke_iterates``, and ``kl_samples`` parameters to judge
    whether a problem is indicated.  Particularly, it's a good idea to
    include runs with 0 ``geweke_iterates`` in that tableau, as an
    estimate of the approximation error induced by having finite
    ``prior_samples`` and ``geweke_samples``.  We also advise testing
    a metamodel in multiple different regimes (little data, much data,
    various schemas).

    Final word of caution: This is a diagnostic tool, not a debugging
    aid.  If a problem is indicated, do not try to divine what it is
    from the pattern of reported K-L divergences.  Instrument your
    model, plot quantities of interest, turn off various parts, etc.

    """
    target_metamodel = bdb.metamodels[metamodel_name]
    prior_gen = create_prior_gen(bdb, target_metamodel, schema, column_names, prior_samples)
    geweke_chain_gen = create_geweke_chain_generator(bdb, target_metamodel, schema, column_names, target_cells, geweke_samples, geweke_iterates)
    return estimate_kl(prior_gen, geweke_chain_gen, target_cells, [], kl_samples, self_check=kl_self_check)
