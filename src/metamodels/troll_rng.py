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

"""The Troll Model posits that all data values are equal to 9.

Reference: http://dilbert.com/strip/2001-10-25

This is an example of the simplest possible population model.

This module implements the :class:`bayeslite.IBayesDBMetamodel`
interface for the Troll Model.
"""

import bayeslite.metamodel as metamodel

class TrollMetamodel(metamodel.IBayesDBMetamodel):
    """Troll metamodel for BayesDB.

    The metamodel is named ``troll_rng`` in BQL::

        CREATE GENERATOR t_troll FOR t USING troll_rng(..)
    """

    def __init__(self): pass
    def name(self): return 'troll_rng'
    def register(self, bdb):
        bdb.sql_execute('''
            INSERT INTO bayesdb_metamodel (name, version)
                VALUES (?, 1)
        ''', (self.name(),))
    def create_generator(self, bdb, table, schema, instantiate):
        instantiate(schema)
    def drop_generator(self, *args, **kwargs): pass
    def rename_column(self, *args, **kwargs): pass
    def initialize_models(self, *args, **kwargs): pass
    def drop_models(self, *args, **kwargs): pass
    def analyze_models(self, *args, **kwargs): pass
    def simulate_joint(self, _bdb, _generator_id, targets, _constraints,
            modelno=None, num_predictions=1):
        return [[9 for _ in targets]] * num_predictions
    def logpdf_joint(self, _bdb, _generator_id, targets, constraints,
            modelno=None):
        for (_, _, value) in constraints:
            if not value == 9:
                return float("nan")
        for (_, _, value) in targets:
            if not value == 9:
                return float("-inf")
        # TODO This is only correct wrt counting measure.  What's the
        # base measure of numericals?
        return 0
    def insert(self, *args, **kwargs): pass
    def remove(self, *args, **kwargs): pass
    def infer(self, *args, **kwargs): pass
