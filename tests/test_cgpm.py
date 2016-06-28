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

import math
import numpy as np
import random                   # XXX

#from cgpm.regressions.forest import RandomForest
from cgpm.regressions.linreg import LinearRegression

from bayeslite import bayesdb_open
from bayeslite import bayesdb_register_metamodel
from bayeslite.metamodels.cgpm_metamodel import CGPM_Metamodel

# XXX KLUDGE TAKEN FROM cgpm/tests/test_gpmcc_simple_composite.py
from cgpm.cgpm import CGpm
class FourWay(CGpm):
    """Generates categorical(4) output on R2 valued input."""

    def __init__(self, outputs, inputs, rng, quagga=None, distargs=None):
        self.rng = rng
        self.probabilities =[
            [.7, .1, .05, .05],
            [.1, .8, .1, .1],
            [.1, .15, .65, .1],
            [.1, .05, .1, .75]]
        assert len(outputs) == 1
        assert len(inputs) == 2
        self.outputs = list(outputs)
        self.inputs = list(inputs)

    def simulate(self, rowid, query, evidence=None, N=None):
        if N is not None:
            return [self.simulate(rowid, query, evidence) for i in xrange(N)]
        regime = self.lookup_quadrant(
            evidence[self.inputs[0]], evidence[self.inputs[1]])
        x = gu.pflip(self.probabilities[regime], rng=self.rng)
        return {self.outputs[0]: x}

    def logpdf(self, rowid, query, evidence=None):
        x = query[self.outputs[0]]
        if int(x) != x: return -float('inf')
        if not (0 <= x <= 3): return -float('inf')
        regime = self.lookup_quadrant(
            evidence[self.inputs[0]] + 1e-5, evidence[self.inputs[1]] + 1e-5)
        return np.log(self.probabilities[regime][int(x)])

    def incorporate(self, rowid, observations, evidence=None):
        pass
    def unincorporate(self, rowid, observations, evidence=None):
        pass

    @staticmethod
    def lookup_quadrant(y0, y1):
        if y0 > 0 and y1 > 0: return 0
        if y0 < 0 and y1 > 0: return 1
        if y0 > 0 and y1 < 0: return 2
        if y0 < 0 and y1 < 0: return 3
        raise ValueError('Invalid value: %s' % str((y0, y1)))

    @staticmethod
    def retrieve_y_for_x(x):
        if x == 0: return [2, 2]
        if x == 1: return [-2, 2]
        if x == 2: return [2, -2]
        if x == 3: return [-2, -2]
        raise ValueError('Invalid value: %s' % str(x))

    def to_metadata(self):
        metadata = {}
        metadata['outputs'] = self.outputs
        metadata['inputs'] = self.inputs
        metadata['factory'] = (__name__, 'FourWay')
        return metadata

    @classmethod
    def from_metadata(cls, metadata, rng):
        return cls(metadata['outputs'], metadata['inputs'], rng)

RandomForest = FourWay
# XXX END KLUDGE

# XXX KLUDGE UNTIL WE HAVE A KEPLER'S LAWS
Kepler = RandomForest
# XXX END KLUDGE

def test_cgpm():
    with bayesdb_open(':memory:') as bdb:
        bdb.sql_execute('''
            CREATE TABLE satellites_ucs (
                apogee,
                class_of_orbit,
                country_of_operator,
                launch_mass,
                perigee,
                period
        )''')
        for l, f in [
            ('geo', lambda x, y: x + y**2),
            ('leo', lambda x, y: math.sin(x + y)),
            (None, lambda x, y: x + y**2),
            (None, lambda x, y: math.sin(x + y)),
        ]:
            for x in xrange(5):
                for y in xrange(5):
                    countries = ['US', 'Russia', 'China', 'Bulgaria']
                    country = countries[random.randrange(len(countries))]
                    mass = random.gauss(1000, 50)
                    bdb.sql_execute('''
                        INSERT INTO satellites_ucs
                            (country_of_operator, launch_mass, class_of_orbit,
                                apogee, perigee, period)
                            VALUES (?,?,?,?,?,?)
                    ''', (country, mass, l, x, y, f(x, y)))
        bdb.execute('''
            CREATE POPULATION satellites FOR satellites_ucs (
                apogee NUMERICAL,
                class_of_orbit CATEGORICAL,
                country_of_operator CATEGORICAL,
                launch_mass NUMERICAL,
                perigee NUMERICAL,
                period NUMERICAL
            )
        ''')
        bdb.execute('''
            estimate correlation from pairwise columns of satellites
        ''').fetchall()
        XXX = bdb.sql_execute('SELECT * FROM satellites_ucs').fetchall()
        registry = {
            'kepler': Kepler,
            'linreg': LinearRegression,
        }
        bayesdb_register_metamodel(bdb, CGPM_Metamodel(registry))
        bdb.execute('''
            CREATE GENERATOR g0 FOR satellites USING cgpm (
                MODEL period GIVEN apogee, perigee
                    USING linreg
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR g0')
        # Another generator: exponential launch mass instead of normal.
        bdb.execute('''
            CREATE GENERATOR g1 FOR satellites USING cgpm (
                launch_mass EXPONENTIAL,
                MODEL period GIVEN apogee, perigee
                    USING kepler (quagga = eland)
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL IF NOT EXISTS FOR g1')
        bdb.execute('ANALYZE g0 FOR 1 ITERATION WAIT')
        bdb.execute('ANALYZE g1 FOR 1 ITERATION WAIT')
        bdb.execute('''
            ESTIMATE DEPENDENCE PROBABILITY
                FROM PAIRWISE VARIABLES OF satellites
        ''').fetchall()
        bdb.execute('''
            ESTIMATE PREDICTIVE PROBABILITY OF period FROM satellites
        ''').fetchall()
        bdb.execute('''
            ESTIMATE PROBABILITY OF period = 42
                    GIVEN (apogee = 8 AND perigee = 7)
                BY satellites
        ''').fetchall()
        bdb.execute('''
            SIMULATE apogee, perigee, period FROM satellites LIMIT 100
        ''').fetchall()
        bdb.execute('''
            INFER EXPLICIT PREDICT apogee
                CONFIDENCE apogee_confidence FROM satellites LIMIT 2
        ''').fetchall()
        results = bdb.execute('''
            INFER EXPLICIT PREDICT class_of_orbit
                CONFIDENCE class_of_orbit_confidence FROM satellites LIMIT 2
        ''').fetchall()
        assert isinstance(results[0][0], unicode)
        bdb.execute('DROP MODELS FROM g0')
        bdb.execute('DROP GENERATOR g0')
        bdb.execute('DROP GENERATOR g1')
