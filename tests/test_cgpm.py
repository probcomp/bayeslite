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
from cgpm.factor.factor import FactorAnalysis
from cgpm.regressions.linreg import LinearRegression
from cgpm.utils import general as gu

from bayeslite import bayesdb_nullify
from bayeslite import bayesdb_open
from bayeslite import bayesdb_read_csv
from bayeslite import bayesdb_register_backend
from bayeslite.core import bayesdb_get_generator
from bayeslite.core import bayesdb_get_population
from bayeslite.exception import BQLError
from bayeslite.backends.cgpm_backend import CGPM_Backend
from bayeslite.util import cursor_value

import test_csv

@contextlib.contextmanager
def cgpm_smoke_bdb():
    with bayesdb_open(':memory:', builtin_backends=False) as bdb:
        registry = {
            'piecewise': PieceWise,
        }
        bayesdb_register_backend(
            bdb, CGPM_Backend(registry, multiprocess=0))

        bdb.sql_execute('CREATE TABLE t (Output, cat, Input)')
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
                output  NUMERICAL;
                input   NUMERICAL;
                cat     NOMINAL;
            )
        ''')

        yield bdb

@contextlib.contextmanager
def cgpm_dummy_satellites_bdb():
    with bayesdb_open(':memory:', builtin_backends=False) as bdb:
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
        bayesdb_nullify(bdb, 'f', "''")
        bayesdb_nullify(bdb, 'f', '""""')
        bayesdb_nullify(bdb, 'f', '')
        bdb.execute('''
            CREATE POPULATION q FOR f WITH SCHEMA (
                SET STATTYPES OF a, b, c TO NOMINAL
            );
        ''')
        bdb.execute('CREATE GENERATOR IF NOT EXISTS h FOR q USING cgpm;')
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
    modeledby = 'MODELED BY %s' % (gen,) if gen else ''
    for var in vars:
        bdb.execute('''
            ESTIMATE PROBABILITY DENSITY OF %s = 1 WITHIN p %s
        ''' % (var, modeledby)).fetchall()
        bdb.execute('''
            SIMULATE %s FROM p %s LIMIT 1
        ''' % (var, modeledby)).fetchall()
        bdb.execute('''
            INFER %s FROM p %s LIMIT 1
        ''' % (var, modeledby)).fetchall()
        nvars = len(bdb.execute('''
            ESTIMATE * FROM VARIABLES OF p %(modeledby)s
                ORDER BY PROBABILITY OF
                    (MUTUAL INFORMATION WITH %(var)s USING 1 SAMPLES > 0.1)
        ''' % {'var': var, 'modeledby': modeledby}).fetchall())
        if 0 < nvars:
            c = bdb.execute('''
                SIMULATE p.(ESTIMATE * FROM VARIABLES OF p %(modeledby)s
                                ORDER BY PROBABILITY OF
                                    (MUTUAL INFORMATION WITH %(var)s
                                        USING 1 SAMPLES > 0.1))
                    FROM p
                    %(modeledby)s
                    LIMIT 1
            ''' % {'var': var, 'modeledby': modeledby}).fetchall()
            assert len(c) == 1
            assert len(c[0]) == nvars

def test_cgpm_smoke():
    with cgpm_smoke_bdb() as bdb:

        # Default model.
        bdb.execute('CREATE GENERATOR g_default FOR p USING cgpm')
        bdb.execute('INITIALIZE 1 MODEL FOR g_default')
        bdb.execute('ANALYZE g_default FOR 1 ITERATION')
        cgpm_smoke_tests(bdb, 'g_default', ['output', 'cat', 'input'])

        # Custom model for output and cat.
        bdb.execute('''
            CREATE GENERATOR g_manifest FOR p USING cgpm (
                OVERRIDE MODEL FOR Output, Cat
                GIVEN Input
                USING piecewise
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR g_manifest')
        bdb.execute('ANALYZE g_manifest FOR 1 ITERATION')
        cgpm_smoke_tests(bdb, 'g_manifest', ['output', 'cat', 'input'])

        # Custom model for latent output, manifest output.
        bdb.execute('''
            CREATE GENERATOR g_latout FOR p USING cgpm (
                LATENT output_ NUMERICAL;
                OVERRIDE MODEL FOR output_, cat GIVEN input USING piecewise;
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR g_latout')
        bdb.execute('ANALYZE g_latout FOR 1 ITERATION')
        cgpm_smoke_tests(bdb, 'g_latout',
            ['output', 'output_', 'cat', 'input'])

        # Custom model for manifest out, latent cat.
        bdb.execute('''
            CREATE GENERATOR g_latcat FOR p USING cgpm (
                LATENT cat_ NOMINAL;
                OVERRIDE MODEL FOR output, cat_ GIVEN input USING piecewise
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR g_latcat')
        bdb.execute('ANALYZE g_latcat FOR 1 ITERATION')
        cgpm_smoke_tests(bdb, 'g_latcat', ['output', 'cat', 'cat_', 'input'])

        # Custom chained model.
        bdb.execute('''
            CREATE GENERATOR g_chain FOR p USING cgpm (
                LATENT midput NUMERICAL;
                LATENT excat NUMERICAL;
                OVERRIDE MODEL FOR midput, cat GIVEN input USING piecewise;
                OVERRIDE MODEL FOR output, excat GIVEN midput USING piecewise
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR g_chain')
        bdb.execute('ANALYZE g_chain FOR 1 ITERATION')
        cgpm_smoke_tests(bdb, 'g_chain',
            ['output', 'excat', 'midput', 'cat', 'input'])

        # Override the crosscat category model.
        bdb.execute('''
            CREATE GENERATOR g_category_model FOR p USING cgpm (
                SET CATEGORY MODEL FOR output TO NORMAL;
                OVERRIDE MODEL FOR input, cat GIVEN output USING piecewise;
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR g_category_model')
        bdb.execute('ANALYZE g_category_model FOR 1 ITERATION')
        cgpm_smoke_tests(bdb, 'g_category_model',
            ['output', 'cat', 'input'])

        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE GENERATOR g_error_typo FOR p USING cgpm (
                    SET CATEGORY MODEL FOR uot TO NORMAL
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE GENERATOR g_error_typo_manifest FOR p USING cgpm (
                    OVERRIDE MODEL FOR output, cat GIVEN ni USING piecewise
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE GENERATOR g_error_typo_output FOR p USING cgpm (
                    OVERRIDE MODEL FOR output, dog GIVEN input USING piecewise;
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE GENERATOR g_error_dup_manifest FOR p USING cgpm (
                    SET CATEGORY MODEL FOR input TO NORMAL;
                    SET CATEGORY MODEL FOR input TO LOGNORMAL
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE GENERATOR g_error_dup_latent FOR p USING cgpm (
                    LATENT output_error NUMERICAL;

                    LATENT output_error NOMINAL;
                    OVERRIDE MODEL FOR output_error, cat
                    GIVEN input USING piecewise;
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE GENERATOR g_error_latent_exists FOR p USING cgpm (
                    LATENT output_ NUMERICAL;
                    OVERRIDE MODEL FOR output_, cat GIVEN input USING piecewise;
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE GENERATOR g_error_latent_manifest FOR p USING cgpm (
                    LATENT output NUMERICAL;
                    OVERRIDE MODEL FOR output, cat GIVEN input USING piecewise;
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE GENERATOR g_category_override_dupe FOR p USING cgpm (
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

        bdb.execute('CREATE GENERATOR g2 FOR p USING cgpm')
        bdb.execute('INITIALIZE 2 MODELS FOR g2')

        start0 = time.time()
        bdb.execute('''
            ANALYZE g2 FOR 10000 ITERATION OR 5 SECONDS
                CHECKPOINT 1 ITERATION (QUIET);
        ''')
        assert 5 < time.time() - start0 < 15

        start1 = time.time()
        bdb.execute('''
            ANALYZE g2 FOR 10000 ITERATION OR 5 SECONDS (
                OPTIMIZED; QUIET
            )
        ''')
        assert 5 < time.time() - start1 < 15

        start2 = time.time()
        bdb.execute('''
            ANALYZE g2 FOR 1 ITERATION OR 100 MINUTES
        ''')
        assert 0 < time.time() - start2 < 15

        start3 = time.time()
        bdb.execute('''
            ANALYZE g2 FOR 10 ITERATION OR 100 SECONDS
            CHECKPOINT 1 ITERATION (OPTIMIZED)
        ''')
        assert 0 < time.time() - start3 < 15


# Use dummy, quick version of Kepler's laws.  Allow an extra
# distribution argument to make sure it gets passed through.
class Kepler(TrollNormal):
    def __init__(self, outputs, inputs, quagga=None, *args, **kwargs):
        assert quagga == 'eland'
        return super(Kepler, self).__init__(outputs, inputs, *args, **kwargs)

def test_cgpm_kepler():
    with cgpm_dummy_satellites_bdb() as bdb:
        bdb.execute('''
            CREATE POPULATION satellites FOR satellites_ucs WITH SCHEMA(
                apogee                  NUMERICAL;
                launch_mass             NUMERICAL;
                class_of_orbit          NOMINAL;
                country_of_operator     NOMINAL;
                perigee                 NUMERICAL;
                period                  NUMERICAL
            )
        ''')
        bdb.execute('''
            ESTIMATE CORRELATION from PAIRWISE VARIABLES OF satellites
        ''').fetchall()
        registry = {
            'kepler': Kepler,
            'linreg': LinearRegression,
        }
        bayesdb_register_backend(
            bdb, CGPM_Backend(registry, multiprocess=0))
        bdb.execute('''
            CREATE GENERATOR g0 FOR satellites USING cgpm (
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
            CREATE GENERATOR g1 FOR satellites USING cgpm (
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
        bdb.execute('ANALYZE g0 FOR 1 ITERATION')
        bdb.execute('ANALYZE g0 FOR 1 ITERATION (VARIABLES period)')
        bdb.execute('ANALYZE g1 FOR 1 ITERATION')
        bdb.execute('ANALYZE g1 FOR 1 ITERATION (VARIABLES period)')
        # OPTIMIZED is ignored because period is a foreign variable.
        bdb.execute('''
            ANALYZE g1 FOR 1 ITERATION (OPTIMIZED; VARIABLES period)
        ''')
        # This should fail since we have a SET CATEGORY MODEL which is not
        # compatible with lovecat. The ValueError is from cgpm not bayeslite.
        with pytest.raises(ValueError):
            bdb.execute('''
                ANALYZE g1 FOR 1 ITERATION
                    (OPTIMIZED; VARIABLES launch_mass)
            ''')
        # Cannot use timed analysis with mixed variables.
        with pytest.raises(BQLError):
            bdb.execute('''
                ANALYZE g1 FOR 5 SECONDS (VARIABLES period, apogee)
            ''')
        # Cannot use timed analysis with mixed variables (period by SKIP).
        with pytest.raises(BQLError):
            bdb.execute('''
                ANALYZE g1 FOR 5 SECONDS (SKIP apogee)
            ''')
        # OK to use iteration analysis with mixed values.
        bdb.execute('''
                ANALYZE g1 FOR 1 ITERATION (VARIABLES period, apogee)
            ''')
        bdb.execute('''
            ESTIMATE DEPENDENCE PROBABILITY
                FROM PAIRWISE VARIABLES OF satellites
        ''').fetchall()
        bdb.execute('''
            ESTIMATE PREDICTIVE PROBABILITY OF period FROM satellites
        ''').fetchall()
        bdb.execute('''
            ESTIMATE PROBABILITY DENSITY OF period = 42
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
        bdb.execute('DROP GENERATOR g0')
        bdb.execute('DROP GENERATOR g1')

def test_unknown_stattype():
    from cgpm.regressions.linreg import LinearRegression
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
                    SET STATTYPES OF apogee, perigee, launch_mass, period
                        TO NUMERICAL;

                    SET STATTYPE OF class_of_orbit, country_of_operator
                        TO NOMINAL;

                    SET STATTYPE OF relaunches
                        TO QUAGGA
                )
            ''')
        # Invent the statistical type.
        bdb.sql_execute('INSERT INTO bayesdb_stattype VALUES (?)', ('quagga',))
        bdb.execute('''
            CREATE POPULATION satellites FOR satellites_ucs WITH SCHEMA(
                SET STATTYPES OF apogee, perigee, launch_mass, period
                    TO NUMERICAL;

                SET STATTYPES OF class_of_orbit, country_of_operator
                TO NOMINAL;

                SET STATTYPES OF relaunches
                TO QUAGGA
            )
        ''')
        registry = {
            'kepler': Kepler,
            'linreg': LinearRegression,
        }
        bayesdb_register_backend(bdb, CGPM_Backend(registry))
        with pytest.raises(BQLError):
            # Can't model QUAGGA by default.
            bdb.execute('CREATE GENERATOR g0 FOR satellites USING cgpm')
        with pytest.raises(BQLError):
            # Can't model QUAGGA as input.
            bdb.execute('''
                CREATE GENERATOR g0 FOR satellites USING cgpm (
                    OVERRIDE MODEL FOR relaunches GIVEN apogee USING linreg;
                    OVERRIDE MODEL FOR period GIVEN relaunches USING linreg
                )
            ''')
        # Can model QUAGGA with an explicit distribution family.
        bdb.execute('''
            CREATE GENERATOR g0 FOR satellites USING cgpm (
                SET CATEGORY MODEL FOR relaunches TO POISSON
            )
        ''')
        bdb.execute('''
            CREATE GENERATOR g1 FOR satellites USING cgpm (
                SET CATEGORY MODEL FOR relaunches TO POISSON;
                OVERRIDE MODEL FOR period GIVEN relaunches USING linreg
            )
        ''')

def test_bad_analyze_vars():
    with cgpm_dummy_satellites_bdb() as bdb:
        bdb.execute('''
            CREATE POPULATION satellites FOR satellites_ucs WITH SCHEMA(
                SET STATTYPE OF apogee TO NUMERICAL;
                SET STATTYPE OF class_of_orbit TO NOMINAL;
                SET STATTYPE OF country_of_operator TO NOMINAL;
                SET STATTYPE OF launch_mass TO NUMERICAL;
                SET STATTYPE OF perigee TO NUMERICAL;
                SET STATTYPE OF period TO NUMERICAL
            )
        ''')
        registry = {
            'kepler': Kepler,
            'linreg': LinearRegression,
        }
        bayesdb_register_backend(bdb, CGPM_Backend(registry))
        bdb.execute('''
            CREATE GENERATOR satellites_cgpm FOR satellites USING cgpm
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR satellites_cgpm')
        bdb.execute('ANALYZE satellites_cgpm FOR 1 ITERATION ()')
        bdb.execute('ANALYZE satellites_cgpm FOR 1 ITERATION')
        with pytest.raises(BQLError):
            # Unknown variable `perige'.
            bdb.execute('''
                ANALYZE satellites_cgpm FOR 1 ITERATION (
                    VARIABLES period, perige
                )
            ''')
        with pytest.raises(BQLError):
            # Unknown variable `perige'.
            bdb.execute('''
                ANALYZE satellites_cgpm FOR 1 ITERATION (
                    SKIP period, perige
                )
            ''')

def test_output_stattypes():
    with cgpm_dummy_satellites_bdb() as bdb:
        # Missing policy for class_of_orbit, perigee, period
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE POPULATION satellites FOR satellites_ucs WITH SCHEMA(
                    SET STATTYPES OF apogee, launch_mass TO NUMERICAL;
                    SET STATTYPES OF country_of_operator TO NOMINAL
                )
            ''')
        bdb.execute('''
            CREATE POPULATION satellites FOR satellites_ucs WITH SCHEMA(
                IGNORE class_of_orbit, perigee, period;
                SET STATTYPES OF apogee, launch_mass TO NUMERICAL;
                SET STATTYPES OF country_of_operator TO NOMINAL
            )
        ''')
        registry = {
            'factor_analysis': FactorAnalysis,
        }
        bayesdb_register_backend(bdb, CGPM_Backend(registry))
        # Creating factor analysis with nominal manifest should crash.
        bdb.execute('''
            CREATE GENERATOR satellites_g0 FOR satellites(
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
                CREATE GENERATOR satellites_g1 FOR satellites(
                    LATENT pc_2 NOMINAL,
                    OVERRIDE GENERATIVE MODEL FOR
                        apogee, launch_mass
                    AND EXPOSE pc_2 NOMINAL
                    USING factor_analysis(L=1)
                )
            ''')
        # Creating factor analysis with nominal latent should crash.
        bdb.execute('''
            CREATE GENERATOR satellites_g1 FOR satellites(
                OVERRIDE GENERATIVE MODEL FOR
                    apogee, launch_mass
                AND EXPOSE pc_2 NOMINAL
                USING factor_analysis(L=1)
            )
        ''')
        with pytest.raises(ValueError):
            bdb.execute('INITIALIZE 1 MODEL FOR satellites_g1')
        # Creating factor analysis with all numerical should be ok.
        bdb.execute('''
            CREATE GENERATOR satellites_g2 FOR satellites USING cgpm(
                LATENT pc_3 NUMERICAL;

                OVERRIDE MODEL FOR apogee, launch_mass, pc_3, pc_4
                USING factor_analysis(L=2);

                LATENT pc_4 NUMERICAL
            )
        ''')
        bdb.execute('INITIALIZE 1 MODEL FOR satellites_g2')
        bdb.execute('ANALYZE satellites_g2 FOR 2 ITERATION')
        # Cannot transition baseline and foreign using timed analysis.
        with pytest.raises(BQLError):
            bdb.execute('''
                ANALYZE satellites_g2 FOR 2 SECONDS (
                    VARIABLES country_of_operator, apogee, launch_mass, pc_3);
            ''')
        bdb.execute('''
            ANALYZE satellites_g2 FOR 1 ITERATION (
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
    # This test ensures that trying to initialize a generator with any
    # (manifest) column of all null variables will crash.
    # Initializing an overriden column with all null variables should not
    # be a problem in general, so we test this case as well.

    with bayesdb_open(':memory:', builtin_backends=False) as bdb:
        registry = {
            'barebones': BareBonesCGpm,
        }
        bayesdb_register_backend(
            bdb, CGPM_Backend(registry, multiprocess=0))
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
                SET STATTYPES OF a, b, c TO NUMERICAL
            )
        ''')
        bdb.execute('''
            CREATE GENERATOR m FOR p;
        ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                INITIALIZE 2 MODELS FOR m;
            ''')

        # Fail when a is nominal and modeled by crosscat.
        bdb.execute('''
            CREATE POPULATION p2 FOR t WITH SCHEMA(
                SET STATTYPES OF a TO NOMINAL;
                SET STATTYPES OF b, c TO NUMERICAL
            )
        ''')
        bdb.execute('CREATE GENERATOR m2 FOR p2;')
        with pytest.raises(BQLError):
            bdb.execute('INITIALIZE 2 MODELS FOR m2;')

        # Succeed when a is ignored.
        bdb.execute('''
            CREATE POPULATION p3 FOR t WITH SCHEMA(
                IGNORE a;
                SET STATTYPES OF b, c TO NUMERICAL
            )
        ''')
        bdb.execute('CREATE GENERATOR m3 FOR p3;')
        bdb.execute('INITIALIZE 2 MODELS FOR m3;')


        # Succeed when a is numerical overriden using a dummy CGPM.
        bdb.execute('''
            CREATE GENERATOR m4 FOR p(
                OVERRIDE MODEL FOR a GIVEN b USING barebones
            )
        ''')
        bdb.execute('INITIALIZE 2 MODELS FOR m4;')
        bdb.execute('ANALYZE m4 FOR 1 ITERATION')

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
        bdb.backends['cgpm'].set_multiprocess(False)
        bdb.execute('CREATE GENERATOR m0 FOR p;')
        bdb.execute('INITIALIZE 1 MODELS FOR m0;')
        bdb.execute('ANALYZE m0 FOR 5 ITERATION')
        # Run some queries on the new variable in the generator or aggregated.
        def run_queries(target, m):
            extra = 'MODELED BY %s' % (m,) if m is not None else ''
            bdb.execute('''
                ESTIMATE PROBABILITY DENSITY OF %s = 1 BY p %s
            ''' % (target, extra,)).fetchall()
            for other in ['age', 'gender', 'salary']:
                cursor = bdb.execute('''
                    ESTIMATE DEPENDENCE PROBABILITY OF %s WITH %s
                    BY p %s
                ''' % (target, other, extra))
                assert cursor_value(cursor) >= 0
            bdb.execute('''
                ESTIMATE SIMILARITY IN THE CONTEXT OF %s
                FROM PAIRWISE p %s;
            ''' % (target, extra,)).fetchall()
        # Fail to run quieres on height, does not exist yet.
        with pytest.raises(BQLError):
            run_queries('height', 'm0')
        # Add the height variable
        bdb.execute('ALTER POPULATION p ADD VARIABLE height numerical;')
        # Run targeted analysis on the newly included height variable.
        bdb.execute('ANALYZE m0 FOR 5 ITERATION')
        bdb.execute('ANALYZE m0 FOR 5 ITERATION (VARIABLES height);')
        # Queries should now be successful.
        run_queries('height', 'm0')
        # Create a new generator, and create a custom category model for
        # the new variable `height`.
        bdb.execute('''
            CREATE GENERATOR m1 FOR p(
                SET CATEGORY MODEL FOR age TO exponential;
                SET CATEGORY MODEL FOR height TO lognormal;
            )
        ''')
        bdb.execute('INITIALIZE 2 MODELS FOR m1')
        bdb.execute('ANALYZE m1 FOR 2 ITERATION')
        # Run height queries on m1.
        run_queries('height', 'm1')
        # Run height queries on population, aggregating m0 and m1.
        run_queries('height', None)
        # Add a third variable rank.
        bdb.execute('ALTER POPULATION p ADD VARIABLE rank numerical;')
        # Analyze rank on m0.
        bdb.execute('''
            ANALYZE m0 FOR 2 ITERATION (OPTIMIZED; VARIABLES rank);
        ''')
        # Analyze all except rank on m0.
        bdb.execute('''
            ANALYZE m0 FOR 2 ITERATION (OPTIMIZED; SKIP rank);
        ''')
        # Fail on m1 with OPTIMIZED, since non-standard category models.
        with pytest.raises(ValueError):
            bdb.execute('''
                ANALYZE m1 FOR 2 ITERATION (OPTIMIZED; VARIABLES rank);
            ''')
        # Succeed analysis on non-optimized analysis.
        bdb.execute('ANALYZE m1 FOR 2 ITERATION')
        # Run queries on the new variable.
        run_queries('rank', 'm0')
        run_queries('rank', 'm1')
        run_queries('rank', None)

def test_predictive_relevance():
    with cgpm_dummy_satellites_bdb() as bdb:
        bayesdb_register_backend(bdb, CGPM_Backend(cgpm_registry=dict()))
        bdb.execute('''
            CREATE POPULATION satellites FOR satellites_ucs WITH SCHEMA (
                apogee                  NUMERICAL;
                class_of_orbit          NOMINAL;
                country_of_operator     NOMINAL;
                launch_mass             NUMERICAL;
                perigee                 NUMERICAL;
                period                  NUMERICAL
            )
        ''')
        bdb.execute('CREATE GENERATOR m FOR satellites;')
        bdb.execute('INITIALIZE 2 MODELS FOR m;')
        bdb.execute('ANALYZE m FOR 25 ITERATION (OPTIMIZED);')

        # Check self-similarites, and also provide coverage of bindings.
        rowids = bdb.execute('SELECT OID from satellites_ucs;').fetchall()
        for rowid in rowids[:4]:
            cursor = bdb.execute('''
                ESTIMATE PREDICTIVE RELEVANCE
                    TO EXISTING ROWS (rowid = ?)
                    IN THE CONTEXT OF "period"
                FROM satellites
                WHERE rowid = ?
            ''', (1, 1,))
            assert next(cursor)[0] == 1.

        # A full extravaganza query, using FROM (as a 1-row).
        cursor = bdb.execute('''
            ESTIMATE PREDICTIVE RELEVANCE
                TO EXISTING ROWS
                    (country_of_operator = 'Russia' AND period < 0)
                AND HYPOTHETICAL ROWS WITH VALUES (
                    (perigee=1.0, launch_mass=120),
                    (country_of_operator='Bulgaria', perigee=2.0))
                IN THE CONTEXT OF "country_of_operator"
            FROM satellites
            LIMIT 5
        ''').fetchall()
        assert len(cursor) == 5
        assert all(0 <= c[0] <= 1 for c in cursor)

        # A full extravaganza query, using BY (as a constant).
        cursor = bdb.execute('''
            ESTIMATE PREDICTIVE RELEVANCE
                OF (rowid = 1)
                TO EXISTING ROWS
                    (country_of_operator = 'Russia' AND period < 0)
                AND HYPOTHETICAL ROWS WITH VALUES (
                    (country_of_operator='China', perigee=1.0),
                    (country_of_operator='Bulgaria'))
                IN THE CONTEXT OF "country_of_operator"
            BY satellites
        ''').fetchall()
        assert len(cursor) == 1
        assert all(0 <= c[0] <= 1 for c in cursor)

        # Hypothetical satellite with negative perigee should not be similar,
        # and use a binding to just ensure that they work.
        cursor = bdb.execute('''
            ESTIMATE PREDICTIVE RELEVANCE
                TO HYPOTHETICAL ROWS WITH VALUES (
                    (perigee = ?))
                IN THE CONTEXT OF "perigee"
            FROM satellites
            LIMIT 5
        ''' , (-10000,)).fetchall()
        assert len(cursor) == 5
        assert all(np.allclose(c[0], 0) for c in cursor)

        # No matching target OF row.
        with pytest.raises(BQLError):
            bdb.execute('''
                ESTIMATE PREDICTIVE RELEVANCE
                    OF (rowid < 0) TO EXISTING ROWS (rowid = 10)
                    IN THE CONTEXT OF "launch_mass"
                BY satellites
            ''')

        # Unknown CONTEXT variable "banana".
        with pytest.raises(BQLError):
            bdb.execute('''
                ESTIMATE PREDICTIVE RELEVANCE
                    OF (rowid = 1) TO EXISTING ROWS (rowid = 2)
                    IN THE CONTEXT OF "banana"
                BY satellites
            ''')

        # No matching EXISTING ROW.
        with pytest.raises(BQLError):
            bdb.execute('''
                ESTIMATE PREDICTIVE RELEVANCE
                    OF (rowid = 10) TO EXISTING ROWS (rowid < 0)
                    IN THE CONTEXT OF "launch_mass"
                BY satellites
            ''')

        # Unknown nominal values 'Mongolia' in HYPOTHETICAL ROWS.
        with pytest.raises(BQLError):
            bdb.execute('''
                ESTIMATE PREDICTIVE RELEVANCE
                    OF (rowid = 10)
                    TO HYPOTHETICAL ROWS WITH VALUES (
                        (country_of_operator='Mongolia'),
                        (country_of_operator='Bulgaria', perigee=2.0))
                    IN THE CONTEXT OF "launch_mass"
                BY satellites
            ''')

        # Create a new row.
        bdb.sql_execute('''
            INSERT INTO satellites_ucs
            (apogee, launch_mass) VALUES (12.128, 12.128)
        ''')

        # TARGET ROW not yet incorporated should return nan.
        cursor = bdb.execute('''
            ESTIMATE PREDICTIVE RELEVANCE
                OF (apogee = 12.128)
                TO HYPOTHETICAL ROWS WITH VALUES (
                    (country_of_operator='China', perigee=1.0))
                IN THE CONTEXT OF "launch_mass"
            BY satellites
        ''')
        result = cursor_value(cursor)
        assert result is None

        # EXISTING ROW not yet incorporated should return nan, since there is
        # no hypothetical.
        cursor = bdb.execute('''
            ESTIMATE PREDICTIVE RELEVANCE
                OF (rowid = 1)
                TO EXISTING ROWS (apogee = 12.128)
                IN THE CONTEXT OF "launch_mass"
            BY satellites
        ''')
        result = cursor_value(cursor)
        assert result is None

        # Although apogee = 12.128 is EXISTING but not incorporated, there are
        # other EXISTING ROWS with apogee > 0, so we should still get a result.
        cursor = bdb.execute('''
            ESTIMATE PREDICTIVE RELEVANCE
                OF (rowid = 1)
                TO EXISTING ROWS (apogee = 12.128 OR apogee > 0)
                IN THE CONTEXT OF "launch_mass"
            BY satellites
        ''')
        result = cursor_value(cursor)
        assert result is not None

        # Although apogee = 12.128 is EXISTING but not incorporated, there are
        # other HYPOTHETICAL ROWS, so we should still get a result.
        cursor = bdb.execute('''
            ESTIMATE PREDICTIVE RELEVANCE
                OF (rowid = 1)
                TO EXISTING ROWS (apogee = 12.128 OR apogee > 0)
                AND HYPOTHETICAL ROWS WITH VALUES (
                    (country_of_operator='China', perigee=1.0),
                    (country_of_operator='Bulgaria'))
                IN THE CONTEXT OF "launch_mass"
            BY satellites
        ''')
        result = cursor_value(cursor)
        assert result is not None

def test_add_drop_models():
    with cgpm_dummy_satellites_bdb() as bdb:
        bayesdb_register_backend(
            bdb, CGPM_Backend(dict(), multiprocess=0))
        bdb.execute('''
            CREATE POPULATION p FOR satellites_ucs WITH SCHEMA(
                GUESS STATTYPES OF (*);
            )
        ''')
        bdb.execute('CREATE GENERATOR m FOR p (SUBSAMPLE 10);')

        # Retrieve id for testing.
        population_id = bayesdb_get_population(bdb, 'p')
        generator_id = bayesdb_get_generator(bdb, population_id, 'm')

        def check_modelno_mapping(lookup):
            pairs = bdb.sql_execute('''
                SELECT modelno, cgpm_modelno FROM bayesdb_cgpm_modelno
                WHERE generator_id = ?
            ''', (generator_id,))
            for pair in pairs:
                assert lookup[pair[0]] == pair[1]
                del lookup[pair[0]]
            assert len(lookup) == 0

        # Initialize some models.
        bdb.execute('INITIALIZE 16 MODELS FOR m')
        # Assert identity mapping initially.
        check_modelno_mapping({i:i for i in xrange(16)})

        bdb.execute('ANALYZE m FOR 1 ITERATION (QUIET);')

        # Drop some models.
        bdb.execute('DROP MODELS 1, 8-12, 14 FROM m')
        # Assert cgpm models are contiguous while bayesdb models are not, with
        # the mapping preserving the strict order.
        check_modelno_mapping({
            0: 0,
            2: 1,
            3: 2,
            4: 3,
            5: 4,
            6: 5,
            7: 6,
            13: 7,
            15: 8,
        })

        # Run some analysis again.
        bdb.execute('ANALYZE m FOR 1 ITERATION (OPTIMIZED; QUIET);')

        # Initialize 14 models if not existing.
        bdb.execute('INITIALIZE 14 MODELS IF NOT EXISTS FOR m')
        # Assert cgpm models are 0-14, while bayesdb are 0-15 excluding 14. Note
        # that INITIALIZE 14 MODELS IF NOT EXISTS does not guarantee that 14
        # MODELS in total will exist after the query, rather it will initialize
        # any non-existing modelnos with index 0-13, and any modelnos > 14
        # (modelno 15 in this test case) are untouched.
        check_modelno_mapping({
            0: 0,
            2: 1,
            3: 2,
            4: 3,
            5: 4,
            6: 5,
            7: 6,
            13: 7,
            15: 8,
            # Recreated models.
            1: 9,
            8: 10,
            9: 11,
            10: 12,
            11: 13,
            12: 14,
        })

        # Drop some more models, add them back with some more, and confirm
        # arithmetic and ordering remains correct.
        bdb.execute('DROP MODELS 0-1 FROM m')
        check_modelno_mapping({
            2: 0,
            3: 1,
            4: 2,
            5: 3,
            6: 4,
            7: 5,
            13: 6,
            15: 7,
            # Recreated models.
            8: 8,
            9: 9,
            10: 10,
            11: 11,
            12: 12,
        })
        bdb.execute('INITIALIZE 20 MODELS IF NOT EXISTS FOR m;')
        check_modelno_mapping({
            2: 0,
            3: 1,
            4: 2,
            5: 3,
            6: 4,
            7: 5,
            13: 6,
            15: 7,
            # Recreated models.
            8: 8,
            9: 9,
            10: 10,
            11: 11,
            12: 12,
            # Re-recreated models.
            0: 13,
            1: 14,
            # New models.
            14: 15,
            16: 16,
            17: 17,
            18: 18,
            19: 19,
        })

        # No such models.
        with pytest.raises(BQLError):
            bdb.execute('DROP MODELS 20-50 FROM m')
        # Drop all models.
        bdb.execute('DROP MODELS FROM m;')
        # No such models.
        with pytest.raises(BQLError):
            bdb.execute('DROP MODEL 0 FROM m')
        # Assert cgpm mapping is cleared.
        cursor = bdb.sql_execute('''
            SELECT COUNT(*) FROM bayesdb_cgpm_modelno
            WHERE generator_id = ?
        ''', (generator_id,))
        assert cursor_value(cursor) == 0

def test_using_modelnos():
    with cgpm_dummy_satellites_bdb() as bdb:
        bdb.execute('''
            CREATE POPULATION satellites FOR satellites_ucs WITH SCHEMA(
                SET STATTYPE OF apogee              TO NUMERICAL;
                SET STATTYPE OF class_of_orbit      TO NOMINAL;
                SET STATTYPE OF country_of_operator TO NOMINAL;
                SET STATTYPE OF launch_mass         TO NUMERICAL;
                SET STATTYPE OF perigee             TO NUMERICAL;
                SET STATTYPE OF period              TO NUMERICAL
            )
        ''')
        bayesdb_register_backend(bdb, CGPM_Backend(dict(), multiprocess=0))
        bdb.execute('''
            CREATE GENERATOR g0 FOR satellites USING cgpm(
                SUBSAMPLE 10
            );
        ''')
        bdb.execute('INITIALIZE 2 MODELS FOR g0')

        # Crash test simulate.
        bdb.execute('''
            SIMULATE apogee, class_of_orbit
            FROM satellites
            MODELED BY g0
            USING MODEL 0-1
            LIMIT 10
        ''')
        # Crash test infer explicit.
        bdb.execute('''
            INFER EXPLICIT PREDICT period, perigee
            FROM satellites
            MODELED BY g0
            USING MODEL 0
            LIMIT 2
        ''')
        # Crash test dependence probability BY.
        c = bdb.execute('''
            ESTIMATE
                DEPENDENCE PROBABILITY OF launch_mass WITH period
            BY satellites
            MODELED BY g0
            USING MODEL 0
        ''')
        assert cursor_value(c) in [0, 1]
        # Crash test dependence probability pairwise.
        cursor = bdb.execute('''
            ESTIMATE
                DEPENDENCE PROBABILITY
            FROM PAIRWISE VARIABLES OF satellites
            MODELED BY g0
            USING MODEL 1
        ''')
        for d in cursor:
            assert d[0] in [0, 1]
        # Crash test mutual information 1row.
        bdb.execute('''
            ESTIMATE
                MUTUAL INFORMATION WITH (period) USING 1 SAMPLES
            FROM VARIABLES OF satellites
            USING MODEL 0
        ''').fetchall()
        # Test analyze on per-model basis.
        bdb.execute('''
            ANALYZE g0 MODEL 0 FOR 1 ITERATION CHECKPOINT 1 ITERATION
        ''')
        engine = bdb.backends['cgpm']._engine(bdb, 1)
        assert len(engine.states[0].diagnostics['logscore']) == 1
        assert len(engine.states[1].diagnostics['logscore']) == 0
        bdb.execute('''
            ANALYZE g0 MODEL 1 FOR 4 ITERATION CHECKPOINT 1 ITERATION (
                OPTIMIZED
            );
        ''')
        assert len(engine.states[0].diagnostics['logscore']) == 1
        assert len(engine.states[1].diagnostics['logscore']) == 4
        # Some errors with bad modelnos.
        with pytest.raises(BQLError):
            bdb.execute('''
                ANALYZE g0 MODEL 0-3 FOR 4 ITERATION
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                SIMULATE apogee FROM satellites USING MODEL 25 LIMIT 10;
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                ESTIMATE PREDICTIVE PROBABILITY OF period FROM satellites
                USING MODELS 0-8 LIMIT 2;
            ''')
