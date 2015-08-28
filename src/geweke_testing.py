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
            geweke_chain_gen.insert(i, j, datum)
        geweke_chain_gen.analyze_models()
        for (i, j) in target_cells:
            geweke_chain_gen.remove(i, j)
    return geweke_chain_gen

def estimate_kl(from_gen, of_gen, target_cells, constraints, kl_samples):
    total = 0
    for _ in range(kl_samples):
        data = from_gen.simulate_joint(target_cells, constraints)
        targeted_data = [(i, j, x) for ((i, j), x) in zip(target_cells, data)]
        from_assessment = from_gen.logpdf(targeted_data, constraints)
        of_assessment   =   of_gen.logpdf(targeted_data, constraints)
        total += from_assessment - of_assessment
    return total

def geweke_kl(bdb, metamodel_name, schema, column_names, target_cells, prior_samples, geweke_samples, geweke_iterates, kl_samples):
    target_metamodel = bdb.metamodels[metamodel_name]
    prior_gen = create_prior_gen(bdb, target_metamodel, schema, column_names, prior_samples)
    geweke_chain_gen = create_geweke_chain_generator(bdb, target_metamodel, schema, column_names, target_cells, geweke_samples, geweke_iterates)
    return estimate_kl(prior_gen, geweke_chain_gen, target_cells, [], kl_samples)
