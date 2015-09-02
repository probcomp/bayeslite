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

import math

import bayeslite.core as core
import bayeslite.ast as ast
import bayeslite.bql as bql
from bayeslite.sqlite3_util import sqlite3_quote_name

def create_empty_table(bdb, column_names):
    table = bdb.temp_table_name()
    qt = sqlite3_quote_name(table)
    qcns = map(sqlite3_quote_name, column_names)
    schema = ','.join('%s NUMERIC' % (qcn,) for qcn in qcns)
    bdb.sql_execute('CREATE TABLE %s(%s)' % (qt, schema))
    core.bayesdb_table_guarantee_columns(bdb, table)
    return table

def create_generator(bdb, table, target_metamodel, schema):
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
        data = geweke_chain_gen.simulate_joint(target_cells, [])
        for ((i, j), datum) in zip(target_cells, data):
            geweke_chain_gen.insert((i, j, datum))
        geweke_chain_gen.analyze_models()
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
    """Estimate the K-L divergence from the first generator to the second.

    Specifically, let P be the distribution over the given target
    cells induced by the `from` generator conditioned on the
    constraints, and let Q be same induced by the `of` generator.
    This function computes and returns a `kl_samples`-point
    Monte-Carlo estimate of the KL of Q from P, in the form of a
    triple: (num_samples, estimate, estimated error of estimate).  The
    error estimate is computed from the variance of the individual
    point estimates of K-L, on the assumtion that `kl_samples` are
    high enough that the distribution on the retuned `estimate` is
    Gaussian (as it must become, by the Central Limit Theorem)
    (provided the appropriate moments exist, which we hope is so)."""

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
    target_metamodel = bdb.metamodels[metamodel_name]
    prior_gen = create_prior_gen(bdb, target_metamodel, schema, column_names, prior_samples)
    geweke_chain_gen = create_geweke_chain_generator(bdb, target_metamodel, schema, column_names, target_cells, geweke_samples, geweke_iterates)
    return estimate_kl(prior_gen, geweke_chain_gen, target_cells, [], kl_samples, self_check=kl_self_check)
