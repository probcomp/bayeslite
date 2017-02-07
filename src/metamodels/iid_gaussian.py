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

"""The IID Gaussian Model posits that all data are independently Gaussian.

This is an example of the simplest possible population model that's
actually stochastic.  The Gaussian has mean 0 and standard deviation
1.

This module implements the :class:`bayeslite.IBayesDBMetamodel`
interface for the IID Gaussian Model.

"""

import math
import random

import bayeslite.metamodel as metamodel

from bayeslite.exception import BQLError

std_normal_schema_1 = '''
INSERT INTO bayesdb_metamodel (name, version) VALUES ('std_normal', 1);
'''

class StdNormalMetamodel(metamodel.IBayesDBMetamodel):
    """IID Gaussian metamodel for BayesDB.

    The metamodel is named ``std_normal`` in BQL::

        CREATE GENERATOR t_sn FOR t USING std_normal(..)
    """

    def __init__(self, seed=0):
        self.prng = random.Random(seed)
    def name(self): return 'std_normal'
    def register(self, bdb):
        with bdb.savepoint():
            schema_sql = 'SELECT version FROM bayesdb_metamodel WHERE name = ?'
            cursor = bdb.sql_execute(schema_sql, (self.name(),))
            version = None
            try:
                row = cursor.next()
            except StopIteration:
                version = 0
            else:
                version = row[0]
            assert version is not None
            if version == 0:
                # XXX WHATTAKLUDGE!
                for stmt in std_normal_schema_1.split(';'):
                    bdb.sql_execute(stmt)
                version = 1
            if version != 1:
                raise BQLError(bdb, 'IID-Gaussian already installed'
                    ' with unknown schema version: %d' % (version,))
    def create_generator(self, bdb, generator_id, schema, **kwargs):
        pass
    def drop_generator(self, *args, **kwargs): pass
    def rename_column(self, *args, **kwargs): pass
    def initialize_models(self, *args, **kwargs): pass
    def drop_models(self, *args, **kwargs): pass
    def analyze_models(self, *args, **kwargs): pass
    def simulate_joint(self, _bdb, _generator_id, rowid, targets, _constraints,
            modelno=None, num_samples=1, accuracy=None):
        return [[self.prng.gauss(0, 1) for _ in targets]
                for _ in range(num_samples)]
    def logpdf_joint(self, _bdb, _generator_id, rowid, targets, _constraints,
            modelno=None):
        return sum(logpdf_gaussian(value, 0, 1) for (_, value) in targets)
    def infer(self, *args, **kwargs): pass

HALF_LOG2PI = 0.5 * math.log(2 * math.pi)

def logpdf_gaussian(x, mu, sigma):
    deviation = x - mu
    return - math.log(sigma) - HALF_LOG2PI \
        - (0.5 * deviation * deviation / (sigma * sigma))
