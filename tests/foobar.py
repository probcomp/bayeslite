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
from cgpm.venturescript.vscgpm import VsCGpm
from cgpm.utils import general as gu

from bayeslite import bayesdb_open
from bayeslite import bayesdb_register_metamodel
from bayeslite.metamodels.cgpm_metamodel import CGPM_Metamodel

# ------------------------------------------------------------------------------
# XXX Get some venturescript integration going.

bdb = bayesdb_open(':memory:')

bdb.sql_execute('''
    CREATE TABLE satellites_ucs (
        apogee,
        class_of_orbit,
        country_of_operator,
        launch_mass,
        perigee,
        period,
        -- Exposed variables.
        kepler_noise,
        kepler_cluster_id
        )
    ''')

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
                INSERT INTO satellites_ucs
                    (country_of_operator, launch_mass, class_of_orbit,
                        apogee, perigee, period)
                    VALUES (?,?,?,?,?,?)
            ''', (country, mass, l, x, y, f(x, y)))

D = bdb.sql_execute('SELECT * FROM satellites_ucs').fetchall()

bdb.execute('''
    CREATE POPULATION satellites FOR satellites_ucs (
        apogee NUMERICAL,
        class_of_orbit CATEGORICAL,
        country_of_operator CATEGORICAL,
        launch_mass NUMERICAL,
        perigee NUMERICAL,
        period NUMERICAL,
        kepler_noise NUMERICAL,
        kepler_cluster_id CATEGORICAL
        )
    ''')

bdb.execute('''
    ESTIMATE CORRELATION FROM PAIRWISE COLUMNS OF satellites
    ''').fetchall()

cgpmt = CGPM_Metamodel(
    cgpm_registry={
        'venturescript': VsCGpm,
        'linreg': LinearRegression,
        })
bayesdb_register_metamodel(bdb, cgpmt)

bdb.execute('''
    CREATE GENERATOR g0 FOR satellites USING cgpm (
        MODEL kepler_cluster_id, kepler_noise, period GIVEN apogee, perigee
            USING venturescript
        )
    ''')

assert False

bdb.execute('INITIALIZE 1 MODEL FOR g0')

# Another generator: exponential launch mass instead of normal.
bdb.execute('''
    CREATE GENERATOR g1 FOR satellites USING cgpm (
        launch_mass EXPONENTIAL,
        MODEL period GIVEN apogee, perigee
            USING kepler
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

bdb.execute('DROP MODELS FROM g0')
bdb.execute('DROP GENERATOR g0')
bdb.execute('DROP GENERATOR g1')
