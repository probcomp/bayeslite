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

import StringIO
import contextlib
import math
import time

import numpy as np
import pytest

from cgpm.cgpm import CGpm
from cgpm.dummy.barebones import BareBonesCGpm
from cgpm.dummy.piecewise import PieceWise
from cgpm.dummy.trollnormal import TrollNormal
from cgpm.utils import general as gu

from bayeslite import bayesdb_open
from bayeslite import bayesdb_read_csv
from bayeslite import bayesdb_register_metamodel
from bayeslite.exception import BQLError
from bayeslite.metamodels.cgpm_metamodel import CGPM_Metamodel
from bayeslite.util import cursor_value

import test_csv

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

def test_cgpm_no_empty_categories():
    with cgpm_smoke_bdb() as bdb:
        bdb.sql_execute('CREATE TABLE f (a, b, c);')
        rows = [['', '\'\'', 'nan'], [1.1, 3, ''], ['""""', 1, 1]]
        for row in rows:
            bdb.sql_execute('INSERT INTO f (a, b, c) VALUES (?,?,?)', row)
        bdb.execute('''
            CREATE POPULATION q FOR f WITH SCHEMA (
                MODEL a, b, c AS NOMINAL
            );
        ''')
        bdb.execute('CREATE METAMODEL h IF NOT EXISTS FOR q USING cgpm;')
        bdb.execute('INITIALIZE 1 MODEL FOR h')
        category_rows = bdb.sql_execute('''
            SELECT colno, value FROM bayesdb_cgpm_category;
        ''')
        # Assert that none of the categories are empty strings or NULL.
        expected = {
            0 : ['1.1'],       # categories for a
            1 : ['1', '3'],    # categories for b
            2 : ['nan', '1'],  # categories for c
        }
        seen = {
            0: [],
            1: [],
            2: [],
        }
        for row in category_rows:
            colno, value = row
            seen[colno].append(value)
        assert all(set(expected[c])==set(seen[c]) for c in expected)

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
                OVERRIDE MODEL FOR output, cat
                GIVEN input
                USING piecewise
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR g_manifest')
        bdb.execute('ANALYZE g_manifest FOR 1 ITERATION WAIT')
        cgpm_smoke_tests(bdb, 'g_manifest', ['output', 'cat', 'input'])

        # Custom model for latent output, manifest output.
        bdb.execute('''
            CREATE METAMODEL g_latout FOR p USING cgpm (
                LATENT output_ NUMERICAL;
                OVERRIDE MODEL FOR output_, cat GIVEN input USING piecewise;
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR g_latout')
        bdb.execute('ANALYZE g_latout FOR 1 ITERATION WAIT')
        cgpm_smoke_tests(bdb, 'g_latout',
            ['output', 'output_', 'cat', 'input'])

        # Custom model for manifest out, latent cat.
        bdb.execute('''
            CREATE METAMODEL g_latcat FOR p USING cgpm (
                LATENT cat_ CATEGORICAL;
                OVERRIDE MODEL FOR output, cat_ GIVEN input USING piecewise
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR g_latcat')
        bdb.execute('ANALYZE g_latcat FOR 1 ITERATION WAIT')
        cgpm_smoke_tests(bdb, 'g_latcat', ['output', 'cat', 'cat_', 'input'])

        # Custom chained model.
        bdb.execute('''
            CREATE METAMODEL g_chain FOR p USING cgpm (
                LATENT midput NUMERICAL;
                LATENT excat NUMERICAL;
                OVERRIDE MODEL FOR midput, cat GIVEN input USING piecewise;
                OVERRIDE MODEL FOR output, excat GIVEN midput USING piecewise
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR g_chain')
        bdb.execute('ANALYZE g_chain FOR 1 ITERATION WAIT')
        cgpm_smoke_tests(bdb, 'g_chain',
            ['output', 'excat', 'midput', 'cat', 'input'])

        # Override the crosscat category model.
        bdb.execute('''
            CREATE METAMODEL g_category_model FOR p USING cgpm (
                SET CATEGORY MODEL FOR output TO NORMAL;
                OVERRIDE MODEL FOR input, cat GIVEN output USING piecewise;
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR g_category_model')
        bdb.execute('ANALYZE g_category_model FOR 1 ITERATION WAIT')
        cgpm_smoke_tests(bdb, 'g_category_model',
            ['output', 'cat', 'input'])

        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE METAMODEL g_error_typo FOR p USING cgpm (
                    SET CATEGORY MODEL FOR uot TO NORMAL
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE METAMODEL g_error_typo_manifest FOR p USING cgpm (
                    OVERRIDE MODEL FOR output, cat GIVEN ni USING piecewise
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE METAMODEL g_error_typo_output FOR p USING cgpm (
                    OVERRIDE MODEL FOR output, dog GIVEN input USING piecewise;
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE METAMODEL g_error_dup_manifest FOR p USING cgpm (
                    SET CATEGORY MODEL FOR input TO NORMAL;
                    SET CATEGORY MODEL FOR input TO LOGNORMAL
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE METAMODEL g_error_dup_latent FOR p USING cgpm (
                    LATENT output_error NUMERICAL;
                    LATENT output_error CATEGORICAL;

                    OVERRIDE MODEL FOR output_error, cat
                    GIVEN input USING piecewise;
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE METAMODEL g_error_latent_exists FOR p USING cgpm (
                    LATENT output_ NUMERICAL;
                    OVERRIDE MODEL FOR output_, cat GIVEN input USING piecewise;
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE METAMODEL g_error_latent_manifest FOR p USING cgpm (
                    LATENT output NUMERICAL;
                    OVERRIDE MODEL FOR output, cat GIVEN input USING piecewise;
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE METAMODEL g_category_override_dupe FOR p USING cgpm (
                    SET CATEGORY MODEL FOR output TO LOGNORMAL;
                    OVERRIDE MODEL FOR output, cat GIVEN input USING piecewise;
                )
            ''')

        cgpm_smoke_tests(bdb, None, ['output', 'cat', 'input'])

        # XXX Check each operation independently: simulate, logpdf, impute.
        for var in ['output_', 'cat_', 'midput', 'excat']:
            with pytest.raises(BQLError):
                cgpm_smoke_tests(bdb, None, [var])

def test_cgpm_analysis_iteration_timed__ci_slow():
    # Test that the minimum of iterations and wall clock are used for ANALYZE.
    # The point of these tests is not for fine-grained timing control (those
    # tests exists in the python interface, there is additional bayesdb bql
    # which makes the time less predictable), but ensuring that clealry
    # extreme amounts of analysis are quickly short-circuited.
    with cgpm_smoke_bdb() as bdb:

        bdb.execute('CREATE METAMODEL g2 FOR p USING cgpm')
        bdb.execute('INITIALIZE 2 MODELS FOR g2')

        start0 = time.time()
        bdb.execute('''
            ANALYZE g2 FOR 10000 ITERATION OR 5 SECONDS
                CHECKPOINT 1 ITERATION WAIT (QUIET);
        ''')
        assert 5 < time.time() - start0 < 15

        start1 = time.time()
        bdb.execute('''
            ANALYZE g2 FOR 10000 ITERATION OR 5 SECONDS WAIT
                (OPTIMIZED; QUIET)
        ''')
        assert 5 < time.time() - start1 < 15

        start2 = time.time()
        bdb.execute('''
            ANALYZE g2 FOR 1 ITERATION OR 100 MINUTES WAIT;
        ''')
        assert 0 < time.time() - start2 < 15

        start3 = time.time()
        bdb.execute('''
            ANALYZE g2 FOR 10 ITERATION OR 100 SECONDS
            CHECKPOINT 1 ITERATION WAIT (OPTIMIZED)
        ''')
        assert 0 < time.time() - start3 < 15


# Use dummy, quick version of Kepler's laws.  Allow an extra
# distribution argument to make sure it gets passed through.
class Kepler(TrollNormal):
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
            ESTIMATE CORRELATION from PAIRWISE VARIABLES OF satellites
        ''').fetchall()
        registry = {
            'kepler': Kepler,
            'linreg': LinearRegression,
        }
        bayesdb_register_metamodel(
            bdb, CGPM_Metamodel(registry, multiprocess=0))
        bdb.execute('''
            CREATE METAMODEL g0 FOR satellites USING cgpm (
                OVERRIDE GENERATIVE MODEL FOR period
                GIVEN apogee, perigee
                USING linreg
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR g0')
        c = bdb.execute('SELECT COUNT(*) FROM bayesdb_cgpm_individual')
        n = c.fetchvalue()
        # Another generator: exponential launch mass instead of normal.
        bdb.execute('''
            CREATE METAMODEL g1 FOR satellites USING cgpm (
                SET CATEGORY MODEL FOR launch_mass TO EXPONENTIAL;
                OVERRIDE MODEL FOR period GIVEN apogee, perigee
                    USING kepler(quagga = eland);
                SUBSAMPLE 20
            )
        ''')
        c_ = bdb.execute('SELECT COUNT(*) FROM bayesdb_cgpm_individual')
        n_ = c_.fetchvalue()
        assert n_ - n == 20
        bdb.execute('INITIALIZE 1 MODEL IF NOT EXISTS FOR g1')
        bdb.execute('ANALYZE g0 FOR 1 ITERATION WAIT')
        bdb.execute('ANALYZE g0 FOR 1 ITERATION WAIT (VARIABLES period)')
        bdb.execute('ANALYZE g1 FOR 1 ITERATION WAIT')
        bdb.execute('ANALYZE g1 FOR 1 ITERATION WAIT (VARIABLES period)')
        # OPTIMIZED is ignored because period is a foreign variable.
        bdb.execute('''
            ANALYZE g1 FOR 1 ITERATION WAIT (OPTIMIZED; VARIABLES period)
        ''')
        # This should fail since we have a SET CATEGORY MODEL which is not
        # compatible with lovecat. The ValueError is from cgpm not bayeslite.
        with pytest.raises(ValueError):
            bdb.execute('''
                ANALYZE g1 FOR 1 ITERATION WAIT
                    (OPTIMIZED; VARIABLES launch_mass)
            ''')
        # Cannot use timed analysis with mixed variables.
        with pytest.raises(BQLError):
            bdb.execute('''
                ANALYZE g1 FOR 5 SECONDS WAIT (VARIABLES period, apogee)
            ''')
        # Cannot use timed analysis with mixed variables (period by SKIP).
        with pytest.raises(BQLError):
            bdb.execute('''
                ANALYZE g1 FOR 5 SECONDS WAIT (SKIP apogee)
            ''')
        # OK to use iteration analysis with mixed values.
        bdb.execute('''
                ANALYZE g1 FOR 1 ITERATION WAIT (VARIABLES period, apogee)
            ''')
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
            INFER EXPLICIT
                PREDICT apogee
                    CONFIDENCE apogee_confidence
                    USING 5 SAMPLES
            FROM satellites LIMIT 2
        ''').fetchall()
        results = bdb.execute('''
            INFER EXPLICIT
                PREDICT class_of_orbit
                    CONFIDENCE class_of_orbit_confidence
            FROM satellites LIMIT 2
        ''').fetchall()
        assert len(results[0]) == 2
        assert isinstance(results[0][0], unicode)
        assert isinstance(results[0][1], float)
        # No CONFIDENCE specified.
        results = bdb.execute('''
            INFER EXPLICIT PREDICT class_of_orbit USING 2 SAMPLES
            FROM satellites LIMIT 2
        ''').fetchall()
        assert len(results[0]) == 1
        assert isinstance(results[0][0], unicode)
        bdb.execute('DROP MODELS FROM g0')
        bdb.execute('DROP METAMODEL g0')
        bdb.execute('DROP METAMODEL g1')

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
                    OVERRIDE MODEL FOR relaunches GIVEN apogee USING linreg;
                    OVERRIDE MODEL FOR period GIVEN relaunches USING linreg
                )
            ''')
        # Can model QUAGGA with an explicit distribution family.
        bdb.execute('''
            CREATE METAMODEL g0 FOR satellites USING cgpm (
                SET CATEGORY MODEL FOR relaunches TO POISSON
            )
        ''')
        bdb.execute('''
            CREATE METAMODEL g1 FOR satellites USING cgpm (
                SET CATEGORY MODEL FOR relaunches TO POISSON;
                OVERRIDE MODEL FOR period GIVEN relaunches USING linreg
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
            CREATE METAMODEL satellites_g0 FOR satellites(
                OVERRIDE MODEL FOR apogee, country_of_operator
                AND EXPOSE pc_1 NUMERICAL
                USING factor_analysis(L=1)
            )
        ''')
        with pytest.raises(ValueError):
            bdb.execute('INITIALIZE 1 MODEL FOR satellites_g0')
        with pytest.raises(BQLError):
            # Duplicate pc_2 in LATENT and EXPOSE.
            bdb.execute('''
                CREATE METAMODEL satellites_g1 FOR satellites(
                    LATENT pc_2 CATEGORICAL,
                    OVERRIDE GENERATIVE MODEL FOR
                        apogee, launch_mass
                    AND EXPOSE pc_2 CATEGORICAL
                    USING factor_analysis(L=1)
                )
            ''')
        # Creating factor analysis with categorical latent should crash.
        bdb.execute('''
            CREATE METAMODEL satellites_g1 FOR satellites(
                OVERRIDE GENERATIVE MODEL FOR
                    apogee, launch_mass
                AND EXPOSE pc_2 CATEGORICAL
                USING factor_analysis(L=1)
            )
        ''')
        with pytest.raises(ValueError):
            bdb.execute('INITIALIZE 1 MODEL FOR satellites_g1')
        # Creating factor analysis with all numerical should be ok.
        bdb.execute('''
            CREATE METAMODEL satellites_g2 FOR satellites USING cgpm(
                LATENT pc_3 NUMERICAL;

                OVERRIDE MODEL FOR apogee, launch_mass, pc_3, pc_4
                USING factor_analysis(L=2);

                LATENT pc_4 NUMERICAL
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR satellites_g2')
        bdb.execute('ANALYZE satellites_g2 FOR 2 ITERATION WAIT;')
        # Cannot transitioned baseline and foreign using timed analyis.
        with pytest.raises(BQLError):
            bdb.execute('''
                ANALYZE satellites_g2 FOR 2 SECONDS WAIT (
                    VARIABLES country_of_operator, apogee, launch_mass, pc_3);
            ''')
        bdb.execute('''
            ANALYZE satellites_g2 FOR 1 ITERATION WAIT (
                VARIABLES apogee, launch_mass);
        ''')
        # Dependence probability of manifest with latent.
        cursor = bdb.execute('''
            ESTIMATE DEPENDENCE PROBABILITY OF apogee WITH pc_3
            BY satellites MODELED BY satellites_g2;
        ''').fetchall()
        assert cursor[0][0] == 1.
        # Dependence probability of latent with latent.
        cursor = bdb.execute('''
            ESTIMATE DEPENDENCE PROBABILITY OF pc_3 WITH pc_4
            BY satellites MODELED BY satellites_g2;
        ''').fetchall()
        assert cursor[0][0] == 1.
        # Mutual information of latent with manifest.
        cursor = bdb.execute('''
            ESTIMATE MUTUAL INFORMATION OF apogee WITH pc_4 USING 1 SAMPLES
            BY satellites MODELED BY satellites_g2;
        ''').fetchall()
        # Mutual information of latent with latent.
        cursor = bdb.execute('''
            ESTIMATE MUTUAL INFORMATION OF pc_3 WITH pc_4 USING 1 SAMPLES
            BY satellites MODELED BY satellites_g2;
        ''').fetchall()

def test_initialize_with_all_nulls():
    # This test ensures that trying to initialize a CGPM metamodel with any
    # (manifest) column of all null variables will crash.
    # Initializing an overriden column with all null variables should not
    # be a problem in general, so we test this case as well.

    with bayesdb_open(':memory:', builtin_metamodels=False) as bdb:
        registry = {
            'barebones': BareBonesCGpm,
        }
        bayesdb_register_metamodel(
            bdb, CGPM_Metamodel(registry, multiprocess=0))
        # Create table with all missing values for a.
        bdb.sql_execute('''
            CREATE TABLE t (a REAL, b REAL, c REAL);
        ''')
        bdb.sql_execute('INSERT INTO t VALUES (?,?,?)', (None, None, 3))
        bdb.sql_execute('INSERT INTO t VALUES (?,?,?)', (None, None, 1))
        bdb.sql_execute('INSERT INTO t VALUES (?,?,?)', (None, None, 1))
        bdb.sql_execute('INSERT INTO t VALUES (?,?,?)', (None, -2, 1))
        bdb.sql_execute('INSERT INTO t VALUES (?,?,?)', (None, -5, 1))
        bdb.sql_execute('INSERT INTO t VALUES (?,?,?)', (None, 2, 3))

        # Fail when a is numerical and modeled by crosscat.
        bdb.execute('''
            CREATE POPULATION p FOR t WITH SCHEMA(
                MODEL a, b, c AS NUMERICAL
            )
        ''')
        bdb.execute('''
            CREATE METAMODEL m FOR p WITH BASELINE crosscat;
        ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                INITIALIZE 2 MODELS FOR m;
            ''')

        # Fail when a is nominal and modeled by crosscat.
        bdb.execute('''
            CREATE POPULATION p2 FOR t WITH SCHEMA(
                MODEL a AS NOMINAL;
                MODEL b, c AS NUMERICAL
            )
        ''')
        bdb.execute('CREATE METAMODEL m2 FOR p2 WITH BASELINE crosscat;')
        with pytest.raises(BQLError):
            bdb.execute('INITIALIZE 2 MODELS FOR m2;')

        # Succeed when a is ignored.
        bdb.execute('''
            CREATE POPULATION p3 FOR t WITH SCHEMA(
                IGNORE a;
                MODEL b, c AS NUMERICAL
            )
        ''')
        bdb.execute('CREATE METAMODEL m3 FOR p3 WITH BASELINE crosscat;')
        bdb.execute('INITIALIZE 2 MODELS FOR m3;')


        # Succeed when a is numerical overriden using a dummy CGPM.
        bdb.execute('''
            CREATE METAMODEL m4 FOR p WITH BASELINE crosscat(
                OVERRIDE MODEL FOR a GIVEN b USING barebones
            )
        ''')
        bdb.execute('INITIALIZE 2 MODELS FOR m4;')
        bdb.execute('ANALYZE m4 FOR 1 ITERATION WAIT;')

def test_add_variable():
    with bayesdb_open() as bdb:
        bayesdb_read_csv(
            bdb, 't', StringIO.StringIO(test_csv.csv_data),
            header=True, create=True)
        bdb.execute('''
            CREATE POPULATION p FOR t WITH SCHEMA(
                age         numerical;
                gender      nominal;
                salary      numerical;
                height      ignore;
                division    ignore;
                rank        ignore;
            )
        ''')
        bdb.metamodels['cgpm'].set_multiprocess(False)
        bdb.execute('CREATE METAMODEL m0 FOR p WITH BASELINE crosscat;')
        bdb.execute('INITIALIZE 1 MODELS FOR m0;')
        bdb.execute('ANALYZE m0 FOR 5 ITERATION WAIT;')
        # Run some queries on the new variable in a metamodel or aggregated.
        def run_queries(target, m):
            extra = 'MODELED BY %s' % (m,) if m is not None else ''
            bdb.execute('''
                ESTIMATE PROBABILITY OF %s = 1 BY p %s
            ''' % (target, extra,)).fetchall()
            for other in ['age', 'gender', 'salary']:
                cursor = bdb.execute('''
                    ESTIMATE DEPENDENCE PROBABILITY OF %s WITH %s
                    BY p %s
                ''' % (target, other, extra))
                assert cursor_value(cursor) >= 0
            bdb.execute('''
                ESTIMATE SIMILARITY WITH RESPECT TO %s
                FROM PAIRWISE p %s;
            ''' % (target, extra,)).fetchall()
        # Fail to run quieres on height, does not exist yet.
        with pytest.raises(BQLError):
            run_queries('height', 'm0')
        # Add the height variable
        bdb.execute('ALTER POPULATION p ADD VARIABLE height numerical;')
        # Run targeted analysis on the newly included height variable.
        bdb.execute('ANALYZE m0 FOR 5 ITERATION WAIT;')
        bdb.execute('ANALYZE m0 FOR 5 ITERATION WAIT (VARIABLES height);')
        # Queries should now be successful.
        run_queries('height', 'm0')
        # Create a new metamodel, and create a custom cateogry model for
        # the new variable `height`.
        bdb.execute('''
            CREATE METAMODEL m1 FOR p WITH BASELINE crosscat(
                SET CATEGORY MODEL FOR age TO exponential;
                SET CATEGORY MODEL FOR height TO lognormal;
            )
        ''')
        bdb.execute('INITIALIZE 2 MODELS FOR m1')
        bdb.execute('ANALYZE m1 FOR 2 ITERATION WAIT;')
        # Run height queries on m1.
        run_queries('height', 'm1')
        # Run height queries on population, aggregating m0 and m1.
        run_queries('height', None)
        # Add a third variable rank.
        bdb.execute('ALTER POPULATION p ADD VARIABLE rank numerical;')
        # Analyze rank on m0.
        bdb.execute('''
            ANALYZE m0 FOR 2 ITERATION WAIT (OPTIMIZED; VARIABLES rank);
        ''')
        # Analyze all except rank on m0.
        bdb.execute('''
            ANALYZE m0 FOR 2 ITERATION WAIT (OPTIMIZED; SKIP rank);
        ''')
        # Fail on m1 with OPTIMIZED, since non-standard category models.
        with pytest.raises(ValueError):
            bdb.execute('''
                ANALYZE m1 FOR 2 ITERATION WAIT (OPTIMIZED; VARIABLES rank);
            ''')
        # Succeed analysis on non-optimized analysis.
        bdb.execute('ANALYZE m1 FOR 2 ITERATION WAIT;')
        # Run queries on the new variable.
        run_queries('rank', 'm0')
        run_queries('rank', 'm1')
        run_queries('rank', None)
