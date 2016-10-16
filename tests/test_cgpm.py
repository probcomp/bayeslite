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

from __future__ import division

import contextlib
import math
import numpy as np
import pytest

from cgpm.cgpm import CGpm
from cgpm.dummy.fourway import FourWay
from cgpm.dummy.piecewise import PieceWise
from cgpm.utils import general as gu

from bayeslite import bayesdb_open
from bayeslite import bayesdb_register_metamodel
from bayeslite.exception import BQLError
from bayeslite.metamodels.cgpm_metamodel import CGPM_Metamodel
from bayeslite.util import cursor_value

@contextlib.contextmanager
def cgpm_smoke_bdb():
    with bayesdb_open(':memory:', builtin_metamodels=False) as bdb:
        registry = {
            'piecewise': PieceWise,
        }
        bayesdb_register_metamodel(
            bdb, CGPM_Metamodel(registry, multiprocess=0))

        bdb.sql_execute('CREATE TABLE t (output, cat, input)')
        for i in xrange(3):
            for j in xrange(3):
                for k in xrange(3):
                    output = i + j/(k + 1)
                    cat = -1 if (i + j*k) % 2 else +1
                    input = (i*j - k)**2
                    if i % 2:
                        output = None
                    if j % 2:
                        cat = None
                    if k % 2:
                        input = None
                    bdb.sql_execute('''
                        INSERT INTO t (output, cat, input) VALUES (?, ?, ?)
                    ''', (output, cat, input))

        bdb.execute('''
            CREATE POPULATION p FOR t WITH SCHEMA(
                MODEL output, input AS NUMERICAL;
                MODEL cat AS CATEGORICAL
            )
        ''')

        yield bdb

@contextlib.contextmanager
def cgpm_dummy_satellites_bdb():
    with bayesdb_open(':memory:', builtin_metamodels=False) as bdb:
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
        yield bdb

def cgpm_smoke_tests(bdb, gen, vars):
    modelledby = 'MODELLED BY %s' % (gen,) if gen else ''
    for var in vars:
        bdb.execute('''
            ESTIMATE PROBABILITY OF %s = 1 WITHIN p %s
        ''' % (var, modelledby)).fetchall()
        bdb.execute('''
            SIMULATE %s FROM p %s LIMIT 1
        ''' % (var, modelledby)).fetchall()
        bdb.execute('''
            INFER %s FROM p %s LIMIT 1
        ''' % (var, modelledby)).fetchall()

def test_cgpm_smoke():
    with cgpm_smoke_bdb() as bdb:

        # Default model.
        bdb.execute('CREATE METAMODEL g_default FOR p USING cgpm')
        bdb.execute('INITIALIZE 1 MODEL FOR g_default')
        bdb.execute('ANALYZE g_default FOR 1 ITERATION WAIT')
        cgpm_smoke_tests(bdb, 'g_default', ['output', 'cat', 'input'])

        # Custom model for output and cat.
        bdb.execute('''
            CREATE METAMODEL g_manifest FOR p USING cgpm (
                MODEL output, cat GIVEN input USING piecewise
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR g_manifest')
        bdb.execute('ANALYZE g_manifest FOR 1 ITERATION WAIT')
        cgpm_smoke_tests(bdb, 'g_manifest', ['output', 'cat', 'input'])

        # Custom model for latent output, manifest output.
        bdb.execute('''
            CREATE METAMODEL g_latout FOR p USING cgpm (
                LATENT output_ NUMERICAL,
                MODEL output_, cat GIVEN input USING piecewise
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR g_latout')
        bdb.execute('ANALYZE g_latout FOR 1 ITERATION WAIT')
        cgpm_smoke_tests(bdb, 'g_latout',
            ['output', 'output_', 'cat', 'input'])

        # Custom model for manifest out, latent cat.
        bdb.execute('''
            CREATE METAMODEL g_latcat FOR p USING cgpm (
                LATENT cat_ CATEGORICAL,
                MODEL output, cat_ GIVEN input USING piecewise
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR g_latcat')
        bdb.execute('ANALYZE g_latcat FOR 1 ITERATION WAIT')
        cgpm_smoke_tests(bdb, 'g_latcat', ['output', 'cat', 'cat_', 'input'])

        # Custom chained model.
        bdb.execute('''
            CREATE METAMODEL g_chain FOR p USING cgpm (
                LATENT midput NUMERICAL,
                LATENT excat NUMERICAL,
                MODEL midput, cat GIVEN input USING piecewise,
                MODEL output, excat GIVEN midput USING piecewise
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR g_chain')
        bdb.execute('ANALYZE g_chain FOR 1 ITERATION WAIT')
        cgpm_smoke_tests(bdb, 'g_chain',
            ['output', 'excat', 'midput', 'cat', 'input'])

        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE METAMODEL g_error_typo FOR p USING cgpm (uot NORMAL)
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE METAMODEL g_error_typo_manifest FOR p USING cgpm (
                    MODEL output, cat GIVEN ni USING piecewise
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE METAMODEL g_error_typo_output FOR p USING cgpm (
                    MODEL output, dog GIVEN input USING piecewise
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE METAMODEL g_error_dup_manifest FOR p USING cgpm (
                    input NORMAL,
                    input LOGNORMAL
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE METAMODEL g_error_dup_latent FOR p USING cgpm (
                    LATENT output_error NUMERICAL,
                    LATENT output_error CATEGORICAL,
                    MODEL output_error, cat GIVEN input USING piecewise
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE METAMODEL g_error_latent_exists FOR p USING cgpm (
                    LATENT output_ NUMERICAL,
                    MODEL output_, cat GIVEN input USING piecewise
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE METAMODEL g_error_latent_manifest FOR p USING cgpm (
                    LATENT output NUMERICAL,
                    MODEL output, cat GIVEN input USING piecewise
                )
            ''')

        cgpm_smoke_tests(bdb, None, ['output', 'cat', 'input'])

        # XXX Check each operation independently: simulate, logpdf, impute.
        for var in ['output_', 'cat_', 'midput', 'excat']:
            with pytest.raises(BQLError):
                cgpm_smoke_tests(bdb, None, [var])

# Use dummy, quick version of Kepler's laws.  Allow an extra
# distribution argument to make sure it gets passed through.
class Kepler(FourWay):
    def __init__(self, outputs, inputs, quagga=None, *args, **kwargs):
        assert quagga == 'eland'
        return super(Kepler, self).__init__(outputs, inputs, *args, **kwargs)

def test_cgpm_kepler():
    try:
        from cgpm.regressions.linreg import LinearRegression
    except ImportError:
        pytest.skip('no sklearn')
        return
    with cgpm_dummy_satellites_bdb() as bdb:
        bdb.execute('''
            CREATE POPULATION satellites FOR satellites_ucs WITH SCHEMA(
                MODEL apogee AS NUMERICAL;
                MODEL class_of_orbit AS CATEGORICAL;
                MODEL country_of_operator AS CATEGORICAL;
                MODEL launch_mass AS NUMERICAL;
                MODEL perigee AS NUMERICAL;
                MODEL period AS NUMERICAL
            )
        ''')
        bdb.execute('''
            estimate correlation from pairwise columns of satellites
        ''').fetchall()
        registry = {
            'kepler': Kepler,
            'linreg': LinearRegression,
        }
        bayesdb_register_metamodel(
            bdb, CGPM_Metamodel(registry, multiprocess=0))
        bdb.execute('''
            CREATE METAMODEL g0 FOR satellites USING cgpm (
                MODEL period GIVEN apogee, perigee
                    USING linreg
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR g0')
        c = bdb.execute('SELECT COUNT(*) FROM bayesdb_cgpm_individual')
        n = c.fetchvalue()
        # Another generator: exponential launch mass instead of normal.
        bdb.execute('''
            CREATE METAMODEL g1 FOR satellites USING cgpm (
                launch_mass EXPONENTIAL,
                MODEL period GIVEN apogee, perigee
                    USING kepler(quagga = eland),
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
        pytest.skip('no sklearn')
        return
    with cgpm_dummy_satellites_bdb() as bdb:
        # Add a column called relaunches, sum of apogee and perigee.
        bdb.sql_execute('ALTER TABLE satellites_ucs ADD COLUMN relaunches')
        n_rows = bdb.sql_execute('''
            SELECT COUNT(*) FROM satellites_ucs
        ''').next()[0]
        for rowid in xrange(n_rows):
            bdb.sql_execute('''
                UPDATE satellites_ucs
                    SET relaunches = (SELECT apogee + perigee)
                    WHERE _rowid_ = ?
            ''', (rowid+1,))
        # Nobody will ever create a QUAGGA statistical type!
        with pytest.raises(BQLError):
            # No such statistical type at the moment.
            bdb.execute('''
                CREATE POPULATION satellites FOR satellites_ucs WITH SCHEMA(
                    MODEL apogee, perigee, launch_mass, period
                    AS NUMERICAL;

                    MODEL class_of_orbit, country_of_operator
                    AS NOMINAL;

                    MODEL relaunches
                    AS QUAGGA
                )
            ''')
        # Invent the statistical type.
        bdb.sql_execute('INSERT INTO bayesdb_stattype VALUES (?)', ('quagga',))
        bdb.execute('''
            CREATE POPULATION satellites FOR satellites_ucs WITH SCHEMA(
                MODEL apogee, perigee, launch_mass, period
                AS NUMERICAL;

                MODEL class_of_orbit, country_of_operator
                AS NOMINAL;

                MODEL relaunches
                AS QUAGGA
            )
        ''')
        registry = {
            'kepler': Kepler,
            'linreg': LinearRegression,
        }
        bayesdb_register_metamodel(bdb, CGPM_Metamodel(registry))
        with pytest.raises(BQLError):
            # Can't model QUAGGA by default.
            bdb.execute('CREATE METAMODEL g0 FOR satellites USING cgpm')
        with pytest.raises(BQLError):
            # Can't model QUAGGA as input.
            bdb.execute('''
                CREATE METAMODEL g0 FOR satellites USING cgpm (
                    MODEL relaunches GIVEN apogee USING linreg,
                    MODEL period GIVEN relaunches USING linreg
                )
            ''')
        # Can model QUAGGA with an explicit distribution family.
        bdb.execute('''
            CREATE METAMODEL g0 FOR satellites USING cgpm (
                relaunches POISSON
            )
        ''')
        bdb.execute('''
            CREATE METAMODEL g1 FOR satellites USING cgpm (
                relaunches POISSON,
                MODEL period GIVEN relaunches USING linreg
            )
        ''')

def test_bad_analyze_vars():
    try:
        from cgpm.regressions.linreg import LinearRegression
    except ImportError:
        pytest.skip('no sklearn')
        return
    with cgpm_dummy_satellites_bdb() as bdb:
        bdb.execute('''
            CREATE POPULATION satellites FOR satellites_ucs WITH SCHEMA(
                MODEL apogee AS NUMERICAL;
                MODEL class_of_orbit AS CATEGORICAL;
                MODEL country_of_operator AS CATEGORICAL;
                MODEL launch_mass AS NUMERICAL;
                MODEL perigee AS NUMERICAL;
                MODEL period AS NUMERICAL
            )
        ''')
        registry = {
            'kepler': Kepler,
            'linreg': LinearRegression,
        }
        bayesdb_register_metamodel(bdb, CGPM_Metamodel(registry))
        bdb.execute('''
            CREATE METAMODEL satellites_cgpm FOR satellites USING cgpm
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

def test_output_stattypes():
    try:
        from cgpm.factor.factor import FactorAnalysis
    except ImportError:
        pytest.skip('no sklearn')
        return
    with cgpm_dummy_satellites_bdb() as bdb:
        # Missing policy for class_of_orbit, perigee, period
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE POPULATION satellites FOR satellites_ucs WITH SCHEMA(
                    MODEL apogee, launch_mass AS NUMERICAL;
                    MODEL country_of_operator AS CATEGORICAL
                )
            ''')
        bdb.execute('''
            CREATE POPULATION satellites FOR satellites_ucs WITH SCHEMA(
                IGNORE class_of_orbit, perigee, period;
                MODEL apogee, launch_mass AS NUMERICAL;
                MODEL country_of_operator AS CATEGORICAL
            )
        ''')
        registry = {
            'factor_analysis': FactorAnalysis,
        }
        bayesdb_register_metamodel(bdb, CGPM_Metamodel(registry))
        # Creating factor analysis with categorical manifest should crash.
        bdb.execute('''
            CREATE METAMODEL satellites_g0 FOR satellites USING cgpm(
                LATENT pc_1 NUMERICAL,
                MODEL apogee, country_of_operator, pc_1
                    USING factor_analysis(L=1)
            )
        ''')
        with pytest.raises(ValueError):
            bdb.execute('INITIALIZE 1 MODEL FOR satellites_g0')
        # Creating factor analysis with categorical latent should crash.
        bdb.execute('''
            CREATE METAMODEL satellites_g1 FOR satellites USING cgpm(
                LATENT pc_2 CATEGORICAL,
                MODEL apogee, launch_mass, pc_2
                    USING factor_analysis(L=1)
            )
        ''')
        with pytest.raises(ValueError):
            bdb.execute('INITIALIZE 1 MODEL FOR satellites_g1')
        # Creating factor analysis with all numerical should be ok.
        bdb.execute('''
            CREATE METAMODEL satellites_g2 FOR satellites USING cgpm(
                LATENT pc_3 NUMERICAL,
                MODEL apogee, launch_mass, pc_3
                    USING factor_analysis(L=1)
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR satellites_g2')
        bdb.execute('ANALYZE satellites_g2 FOR 2 ITERATION WAIT;')
