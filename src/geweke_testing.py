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

def create_empty_table(bdb, columns):
    ...

def create_generator(bdb, table, target_metamodel, columns):
    ...

def create_prior_gen(bdb, target_metamodel, columns, prior_samples):
    prior_gen = create_generator(bdb, table, target_metamodel, columns)
    prior_gen.initialize_models(range(prior_samples))
    return prior_gen

def create_geweke_chain_generator(bdb, target_metamodel, columns, target_cells, geweke_samples, geweke_iterates):
    geweke_chain_gen = create_generator(bdb, table, target_metamodel, columns)
    geweke_chain_gen.initialize_models(range(geweke_samples))
    for _ in geweke_iterates:
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
        from_assessment = from_gen.logpdf(zip(target_cells, data))
          of_assessment =   of_gen.logpdf(zip(target_cells, data))
        total += from_assessment - of_assessment
    return total

def geweke_kl(bdb, target_metamodel, columns, target_cells, prior_samples, geweke_samples, geweke_iterates, kl_samples):
    table = create_empty_table(bdb, columns)
    prior_gen = create_prior_gen(bdb, table, target_metamodel, columns, prior_samples)
    geweke_chain_gen = create_geweke_chain_generator(bdb, target_metamodel, columns, target_cells, geweke_samples, geweke_iterates)
    return estimate_kl(prior_gen, geweke_chain_gen, target_cells, [], kl_samples)
