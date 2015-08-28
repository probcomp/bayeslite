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

"""The IID Gaussian Model posits that all data values are independently a standard Gaussian.

This is an example of the simplest possible population model that's
actually stochastic.

This module implements the :class:`bayeslite.IBayesDBMetamodel`
interface for the IID Gaussian Model.

"""

import scipy.stats as stats

import bayeslite.metamodel as metamodel

class StdNormalMetamodel(metamodel.IBayesDBMetamodel):
    """IID Gaussian metamodel for BayesDB.

    The metamodel is named ``std_normal`` in BQL::

        CREATE GENERATOR t_sn FOR t USING std_normal(..)
    """

    def __init__(self): pass
    def name(self): return 'std_normal'
    def register(self, bdb):
        bdb.sql_execute("INSERT INTO bayesdb_metamodel (name, version) VALUES ('std_normal', 1)")
    def create_generator(self, bdb, table, schema, instantiate):
        instantiate(schema)
    def drop_generator(self, *args): pass
    def rename_column(self, *args): pass
    def initialize_models(self, *args): pass
    def drop_models(self, *args): pass
    def analyze_models(self, *args): pass
    def simulate_joint(self, _bdb, _generator_id, targets, _constraints):
        return stats.norm(loc=0, scale=1).rvs(len(targets))
    def logpdf(self, _bdb, _generator_id, targets, _constraints):
        vals = [value for (_, _, value) in targets]
        anss = stats.norm(loc=0, scale=1).logpdf(vals)
        return sum(anss)
    def insert(self, *args): pass
    def remove(self, *args): pass
    def infer(self, *args): pass
