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

This module implements the :class:`bayeslite.BayesDB_Backend`
interface for the Troll Model.
"""

import bayeslite.backend

class TrollBackend(bayeslite.backend.BayesDB_Backend):
    """Troll backend for BayesDB.

    The backend is named ``troll_rng`` in BQL::

        CREATE GENERATOR t_troll FOR t USING troll_rng(..)
    """

    def __init__(self): pass
    def name(self): return 'troll_rng'
    def register(self, bdb):
        bdb.sql_execute('''
            INSERT INTO bayesdb_backend (name, version)
                VALUES (?, 1)
        ''', (self.name(),))
    def create_generator(self, bdb, generator_id, schema, **kwargs):
        pass
    def drop_generator(self, *args, **kwargs): pass
    def rename_column(self, *args, **kwargs): pass
    def initialize_models(self, *args, **kwargs): pass
    def drop_models(self, *args, **kwargs): pass
    def analyze_models(self, *args, **kwargs): pass
    def simulate_joint(self, _bdb, _generator_id, _modelnos, rowid, targets,
            _constraints, num_samples=1):
        return [[9 for _ in targets]] * num_samples
    def logpdf_joint(self, _bdb, _generator_id, _modelnos, rowid, targets,
            constraints):
        for (_, value) in constraints:
            if not value == 9:
                return float("nan")
        for (_, value) in targets:
            if not value == 9:
                return float("-inf")
        # TODO This is only correct wrt counting measure.  What's the
        # base measure of numericals?
        return 0
    def infer(self, *args, **kwargs): pass
