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
import pytest

from cgpm.cgpm import CGpm
from cgpm.dummy.fourway import FourWay
from cgpm.utils import general as gu

from bayeslite import bayesdb_open
from bayeslite import bayesdb_register_metamodel
from bayeslite.exception import BQLError
from bayeslite.metamodels.cgpm_metamodel import CGPM_Metamodel
from bayeslite.util import cursor_value

# Use dummy, quick version of Kepler's laws.
Kepler = FourWay

def test_cgpm():
    try:
        from cgpm.regressions.linreg import LinearRegression
    except ImportError:
        pytest.skip('no sklearn or venturescript')
        return
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
                    country = countries[bdb._np_prng.randint(0, len(countries))]
                    mass = bdb._np_prng.normal(1000, 50)
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
        bayesdb_register_metamodel(
            bdb, CGPM_Metamodel(registry, multiprocess=0))
        bdb.execute('''
            CREATE GENERATOR g0 FOR satellites USING cgpm (
                MODEL period GIVEN apogee, perigee
                    USING linreg
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR g0')
        c = bdb.execute('SELECT COUNT(*) FROM bayesdb_cgpm_individual')
        n = c.fetchvalue()
        # Another generator: exponential launch mass instead of normal.
        bdb.execute('''
            CREATE GENERATOR g1 FOR satellites USING cgpm (
                launch_mass EXPONENTIAL,
                MODEL period GIVEN apogee, perigee
                    USING kepler,
                SUBSAMPLE 20
            )
        ''')
        c_ = bdb.execute('SELECT COUNT(*) FROM bayesdb_cgpm_individual')
        n_ = c_.fetchvalue()
        assert n_ - n == 20
        bdb.execute('INITIALIZE 1 MODEL IF NOT EXISTS FOR g1')
        bdb.execute('ANALYZE g0 FOR 1 ITERATION WAIT')
        bdb.execute('ANALYZE g1 FOR 1 ITERATION WAIT')
        bdb.execute('''
            ESTIMATE DEPENDENCE PROBABILITY
                FROM PAIRWISE VARIABLES OF satellites
        ''').fetchall()
        with pytest.raises(AssertionError):
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

def test_unknown_stattype():
    try:
        from cgpm.regressions.linreg import LinearRegression
    except ImportError:
        pytest.skip('no sklearn or venturescript')
        return
    with bayesdb_open(':memory:') as bdb:
        bdb.sql_execute('''
            CREATE TABLE satellites_ucs (
                apogee,
                class_of_orbit,
                country_of_operator,
                launch_mass,
                perigee,
                period,
                relaunches
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
                    country = countries[bdb._np_prng.randint(0, len(countries))]
                    mass = bdb._np_prng.normal(1000, 50)
                    bdb.sql_execute('''
                        INSERT INTO satellites_ucs
                            (country_of_operator, launch_mass, class_of_orbit,
                                apogee, perigee, period, relaunches)
                            VALUES (?,?,?,?,?,?,?)
                    ''', (country, mass, l, x, y, f(x, y), x + y))
        # Nobody will ever create a QUAGGA statistical type!
        with pytest.raises(BQLError):
            # No such statistical type at the moment.
            bdb.execute('''
                CREATE POPULATION satellites FOR satellites_ucs (
                    apogee NUMERICAL,
                    class_of_orbit CATEGORICAL,
                    country_of_operator CATEGORICAL,
                    launch_mass NUMERICAL,
                    perigee NUMERICAL,
                    period NUMERICAL,
                    relaunches QUAGGA
                )
            ''')
        # Invent the statistical type.
        bdb.sql_execute('INSERT INTO bayesdb_stattype VALUES (?)', ('quagga',))
        bdb.execute('''
            CREATE POPULATION satellites FOR satellites_ucs (
                apogee NUMERICAL,
                class_of_orbit CATEGORICAL,
                country_of_operator CATEGORICAL,
                launch_mass NUMERICAL,
                perigee NUMERICAL,
                period NUMERICAL,
                relaunches QUAGGA
            )
        ''')
        registry = {
            'kepler': Kepler,
            'linreg': LinearRegression,
        }
        bayesdb_register_metamodel(bdb, CGPM_Metamodel(registry))
        with pytest.raises(BQLError):
            # Can't model QUAGGA by default.
            bdb.execute('CREATE GENERATOR g0 FOR satellites USING cgpm')
        with pytest.raises(BQLError):
            # Can't model QUAGGA as input.
            bdb.execute('''
                CREATE GENERATOR g0 FOR satellites USING cgpm (
                    MODEL relaunches GIVEN apogee USING linreg,
                    MODEL period GIVEN relaunches USING linreg
                )
            ''')
        # Can model QUAGGA with an explicit distribution family.
        bdb.execute('''
            CREATE GENERATOR g0 FOR satellites USING cgpm (
                relaunches POISSON
            )
        ''')
        bdb.execute('''
            CREATE GENERATOR g1 FOR satellites USING cgpm (
                relaunches POISSON,
                MODEL period GIVEN relaunches USING linreg
            )
        ''')

def test_bad_analyze_vars():
    try:
        from cgpm.regressions.linreg import LinearRegression
    except ImportError:
        pytest.skip('no sklearn or venturescript')
        return
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
                    country = countries[bdb._np_prng.randint(0, len(countries))]
                    mass = bdb._np_prng.normal(1000, 50)
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
        registry = {
            'kepler': Kepler,
            'linreg': LinearRegression,
        }
        bayesdb_register_metamodel(bdb, CGPM_Metamodel(registry))
        bdb.execute('''
            CREATE GENERATOR satellites_cgpm FOR satellites USING cgpm
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR satellites_cgpm')
        bdb.execute('ANALYZE satellites_cgpm FOR 1 ITERATION WAIT ()')
        bdb.execute('ANALYZE satellites_cgpm FOR 1 ITERATION WAIT')
        with pytest.raises(BQLError):
            # Unknown variable `perige'.
            bdb.execute('''
                ANALYZE satellites_cgpm FOR 1 ITERATION WAIT (
                    VARIABLES period, perige
                )
            ''')
        with pytest.raises(BQLError):
            # Unknown variable `perige'.
            bdb.execute('''
                ANALYZE satellites_cgpm FOR 1 ITERATION WAIT (
                    SKIP period, perige
                )
            ''')
