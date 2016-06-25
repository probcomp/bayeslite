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
import random                   # XXX

from cgpm.crosscat.engine import Engine
#from cgpm.regressions.forest import RandomForest

from bayeslite import bayesdb_open
from bayeslite import bayesdb_register_metamodel
from bayeslite.metamodels.cgpm_metamodel import CGPM_Metamodel

# XXX KLUDGE TAKEN FROM cgpm/tests/test_gpmcc_simple_composite.py
from cgpm.cgpm import CGpm
class FourWay(CGpm):
    """Generates categorical(4) output on R2 valued input."""

    def __init__(self, outputs, inputs, rng):
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
        if not (0 <= x <= 3): return -float('inf')
        regime = self.lookup_quadrant(
            evidence[self.inputs[0]], evidence[self.inputs[1]])
        return np.log(self.probabilities[regime][x])

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
RandomForest = FourWay
# XXX END KLUDGE

# XXX KLUDGE UNTIL WE HAVE A KEPLER'S LAWS
Kepler = RandomForest
# XXX END KLUDGE

def test_cgpm():
    with bayesdb_open(':memory:') as bdb:
        bdb.sql_execute('''
            CREATE TABLE satellites(
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
        ]:
            for x in xrange(10):
                for y in xrange(10):
                    countries = ['US', 'Russia', 'China', 'Bulgaria']
                    country = countries[random.randrange(len(countries))]
                    mass = random.gauss(1000, 50)
                    bdb.sql_execute('''
                        INSERT INTO satellites
                            (country_of_operator, launch_mass, class_of_orbit,
                                apogee, perigee, period)
                            VALUES (?,?,?,?,?,?)
                    ''', (country, mass, l, x, y, f(x, y)))
        XXX = bdb.sql_execute('SELECT * FROM satellites').fetchall()
        engine = Engine(XXX, num_states=0, multithread=False)
        registry = {
            'kepler': Kepler,
        }
        bayesdb_register_metamodel(bdb, CGPM_Metamodel(engine, registry))
        bdb.execute('''
            CREATE GENERATOR g FOR satellites USING cgpm(
                apogee NUMERICAL,
                class_of_orbit CATEGORICAL,
                country_of_operator CATEGORICAL,
                launch_mass NUMERICAL,
                perigee NUMERICAL,
                period NUMERICAL
            )
        ''')
        bdb.execute('''
            INITIALIZE 1 MODEL FOR g (<
                "variables"~ (
                    ("apogee", "numerical", "normal", < >),
                    ("class_of_orbit", "categorical", "categorical", <"k"~ 3>),
                    ("country_of_operator", "categorical", "categorical",
                     <"k"~ 4>),
                    ("launch_mass", "numerical", "normal", < >),
                    ("perigee", "numerical", "normal", < >),
                    ("period", "numerical", "normal", < >)
                ),
                "categoricals"~ <
                    "1"~ <
                        "geo"~ 0,
                        "leo"~ 1,
                        "meo"~ 2
                    >,
                    "2"~ <
                        "US"~ 0,
                        "Russia"~ 1,
                        "China"~ 2,
                        "Bulgaria"~ 3
                    >
                >,
                "cgpm_composition"~ (
                    <
                        "name"~ "kepler",
                        "outputs"~ ("apogee", "perigee"),
                        "inputs"~ ("period")
                        -- "kwds"~ <"noise"~ 1.0>
                    >
                )
            >)
            -- USING (period ~ kepler(apogee, perigee))
        ''')
        # Another model schema: exponential launch mass instead of
        # normal.
        #
        # XXX For the moment, we have to use INITIALIZE 2 MODELS IF
        # NOT EXISTS in order to get *one* model with this schema.  To
        # be remedied once we name model schemas.
        bdb.execute('''
            INITIALIZE 2 MODELS IF NOT EXISTS FOR g (<
                "variables"~ (
                    ("apogee", "numerical", "normal", < >),
                    ("class_of_orbit", "categorical", "categorical", <"k"~ 3>),
                    ("country_of_operator", "categorical", "categorical",
                     <"k"~ 4>),
                    ("launch_mass", "numerical", "exponential", < >),
                    ("perigee", "numerical", "normal", < >),
                    ("period", "numerical", "normal", < >)
                ),
                "categoricals"~ <
                    "1"~ <
                        "geo"~ 0,
                        "leo"~ 1,
                        "meo"~ 2
                    >,
                    "2"~ <
                        "US"~ 0,
                        "Russia"~ 1,
                        "China"~ 2,
                        "Bulgaria"~ 3
                    >
                >,
                "cgpm_composition"~ (
                    <
                        "name"~ "kepler",
                        "outputs"~ ("apogee", "perigee"),
                        "inputs"~ ("period")
                        -- "kwds"~ <"noise"~ 1.0>
                    >
                )
            >)
            -- USING (period ~ kepler(apogee, perigee))
        ''')
        # Another model -- via a model schema.
        bdb.execute('''
            CREATE MODEL SCHEMA g0 FOR g (<
                "variables"~ (
                    ("apogee", "numerical", "normal", < >),
                    ("class_of_orbit", "categorical", "categorical", <"k"~ 3>),
                    ("country_of_operator", "categorical", "categorical",
                     <"k"~ 4>),
                    ("launch_mass", "numerical", "normal_trunc",
                     <"l"~ 0, "h"~ 4000>),
                    ("perigee", "numerical", "normal", < >),
                    ("period", "numerical", "normal", < >)
                ),
                "categoricals"~ <
                    "1"~ <
                        "geo"~ 0,
                        "leo"~ 1,
                        "meo"~ 2
                    >,
                    "2"~ <
                        "US"~ 0,
                        "Russia"~ 1,
                        "China"~ 2,
                        "Bulgaria"~ 3
                    >
                >,
                "cgpm_composition"~ (
                    <
                        "name"~ "kepler",
                        "outputs"~ ("apogee", "perigee"),
                        "inputs"~ ("period")
                        -- "kwds"~ <"noise"~ 1.0>
                    >
                )
            >)
        ''')
        bdb.execute('INITIALIZE 3 MODELS IF NOT EXISTS FOR g USING g0')
        bdb.execute('DROP MODELSCHEMA g0')
        bdb.execute('ANALYZE g FOR 1 ITERATION WAIT')
        bdb.execute('''
            ESTIMATE DEPENDENCE PROBABILITY
                FROM PAIRWISE COLUMNS OF g
        ''').fetchall()
        bdb.execute('DROP MODELS FROM g')
        bdb.execute('DROP GENERATOR g')
