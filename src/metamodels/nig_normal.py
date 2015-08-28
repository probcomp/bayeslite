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

"""A model that posts that all columns are independently Gaussian with
unknown parameters.

The parameters are taken from the normal and inverse-gamma conjuate
prior.

This module implements the :class:`bayeslite.IBayesDBMetamodel`
interface for the NIG-Normal model.
"""

import math
import random

import bayeslite.metamodel as metamodel

class NIGNormalMetamodel(metamodel.IBayesDBMetamodel):
    """Normal-Inverse-Gamma-Normal metamodel for BayesDB.

    The metamodel is named ``nig_normal`` in BQL::

        CREATE GENERATOR t_nig FOR t USING nig_normal(..)
    """

    def __init__(self, seed=0):
        self.prng = random.Random(seed)
    def name(self): return 'nig_normal'
    def register(self, bdb):
        bdb.sql_execute("INSERT INTO bayesdb_metamodel (name, version) VALUES ('nig_normal', 1)")
    def create_generator(self, bdb, table, schema, instantiate):
        instantiate(schema)
    def drop_generator(self, *args): pass
    def rename_column(self, *args): pass
    def initialize_models(self, *args): pass
    def drop_models(self, *args): pass
    def analyze_models(self, *args): pass
    def simulate_joint(self, _bdb, _generator_id, targets, _constraints):
        return [self.prng.gauss(0, 1) for _ in targets]
    def logpdf(self, _bdb, _generator_id, targets, _constraints):
        return sum(logpdfOne(value, 0, 1) for (_, _, value) in targets)
    def insert(self, *args): pass
    def remove(self, *args): pass
    def infer(self, *args): pass

HALF_LOG2PI = 0.5 * math.log(2 * math.pi)

def logpdfOne(x, mu, sigma):
    deviation = x - mu
    return - math.log(sigma) - HALF_LOG2PI \
        - (0.5 * deviation * deviation / (sigma * sigma))
