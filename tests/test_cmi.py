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

import contextlib
import itertools
import os
os.environ['LOOM_VERBOSITY'] = '0'
import pytest

import numpy as np

from bayeslite import bayesdb_open
from bayeslite import bayesdb_register_backend
from bayeslite.backends.loom_backend import LoomBackend
from bayeslite.exception import BQLError
from bayeslite.exception import BQLParseError
from stochastic import stochastic
from test_loom_backend import tempdir


'''This test provides coverage for ESTIMATE and SIMULATE of conditional mutual
information queries for univariate and multivariate targets and conditioning
statements.'''


@contextlib.contextmanager
def smoke_bdb():
    with bayesdb_open(':memory:') as bdb:
        bdb.sql_execute('CREATE TABLE t (a, b, c, d, e)')

        for a, b, c, d, e in itertools.product(*([range(2)]*4+[['x','y']])):
            # XXX Insert synthetic data generator here.
            bdb.sql_execute('''
                INSERT INTO t (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)
            ''', (a, b, c, d, e))

        bdb.execute('''
            CREATE POPULATION p FOR t WITH SCHEMA (
                SET STATTYPES OF a, b, c, d TO NUMERICAL;
                SET STATTYPES OF e TO NOMINAL
            )
        ''')

        bdb.execute('CREATE GENERATOR m1 FOR p;')
        bdb.execute('INITIALIZE 10 MODELS FOR m1;')

        bdb.execute('CREATE GENERATOR m2 FOR p;')
        bdb.execute('INITIALIZE 10 MODELS FOR m2;')
        yield bdb

@contextlib.contextmanager
def smoke_loom():
    with tempdir('bayeslite-loom') as loom_store_path:
        with bayesdb_open(':memory:') as bdb:
            bayesdb_register_backend(
                bdb,
                LoomBackend(loom_store_path=loom_store_path)
            )
            bdb.sql_execute('CREATE TABLE t (a, b, c, d, e)')

            for a, b, c, d, e in itertools.product(*([range(2)]*4+[['x','y']])):
                # XXX Insert synthetic data generator here.
                bdb.sql_execute('''
                    INSERT INTO t (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)
                ''', (a, b, c, d, e))

            bdb.execute('''
                CREATE POPULATION p FOR t WITH SCHEMA (
                    SET STATTYPES OF a, b, c, d TO NUMERICAL;
                    SET STATTYPES OF e TO NOMINAL
                )
            ''')

            bdb.execute('CREATE GENERATOR m FOR p using loom;')
            bdb.execute('INITIALIZE 1 MODELS FOR m;')

            yield bdb

# Define priors for parents.
P_A = 0.5
P_B = 0.5
# Define conditional probability table.
P_C_GIVEN_AB = {
    (0, 0,) : 0.1,
    (0, 1,) : 0.8,
    (1, 0,) : 0.5,
    (1, 1,) : 0.2,
}

def generate_v_structured_data(N, np_prng):
    """Generate data from v-structure graphical of binary nodes.

    a ~ binary(p_a)
    b ~ binary(p_b)
    c ~ binary(p_c | a, b)

    Graphical model:
    (a) -> (c) <- (b)
    """
    p_a = P_A
    p_b = P_B
    p_c_given_ab = P_C_GIVEN_AB


    def flip(p):
        """Sample binary data with probability p."""
        return int(np_prng.uniform() < p)
    a = [flip(p_a) for _ in range(N)]
    b = [flip(p_b) for _ in range(N)]
    c = [
        flip(p_c_given_ab[parent_config])
        for parent_config in zip(a, b)
    ]
    return zip(a, b, c)

@contextlib.contextmanager
def bdb_for_checking_cmi(backend, iterations):
    with tempdir('bayeslite-loom') as loom_store_path:
        with bayesdb_open(':memory:') as bdb:
            bayesdb_register_backend(
                bdb,
                LoomBackend(loom_store_path=loom_store_path)
            )
            bdb.sql_execute('CREATE TABLE t (a, b, c)')
            for row in generate_v_structured_data(1000, bdb.np_prng):
                bdb.sql_execute('''
                    INSERT INTO t (a, b, c) VALUES (?, ?, ?)
                ''', row)

            bdb.execute('''
                CREATE POPULATION p FOR t WITH SCHEMA (
                    SET STATTYPES OF a, b, c TO NOMINAL;
                )
            ''')
            # I am assuming that SQL formatting with `?` does only work for
            # `bdb.sql_execute` and not for `bdb.execute`.
            if backend == 'loom':
                bdb.execute('CREATE GENERATOR m FOR p using loom')
            elif backend == 'cgpm':
                bdb.execute('CREATE GENERATOR m FOR p using cgpm')
            else:
                raise ValueError('Backend %s unknown' % (backend,))
            # XXX we may want to downscale this eventually.
            bdb.execute('INITIALIZE 10 MODELS FOR m;')
            bdb.backends['cgpm'].set_multiprocess('on')
            bdb.execute('ANALYZE m FOR %d ITERATIONS;' % (iterations,))
            bdb.backends['cgpm'].set_multiprocess('off')
            yield bdb


def test_estimate_cmi_basic__ci_slow():
    with smoke_bdb() as bdb:
        bql = '''
            ESTIMATE MUTUAL INFORMATION OF a WITH b
            BY p
            MODELED BY m1
        '''
        result = bdb.execute(bql).fetchall()
        assert len(result) == 1


def test_estimate_cmi_no_condition():
    with smoke_bdb() as bdb:
        # Univariate targets.
        bql = '''
            ESTIMATE MUTUAL INFORMATION OF a WITH b USING 10 SAMPLES
            BY p
        '''
        result = bdb.execute(bql).fetchall()
        assert len(result) == 1
        # Multivariate targets.
        bql = '''
            ESTIMATE MUTUAL INFORMATION OF (a, e) WITH b USING 10 SAMPLES
            BY p
        '''
        result = bdb.execute(bql).fetchall()
        assert len(result) == 1


def test_estimate_cmi_equality_condition():
    with smoke_bdb() as bdb:
        # Univariate targets.
        bql = '''
            ESTIMATE
                MUTUAL INFORMATION OF a WITH b GIVEN
                    (c = 1, e = 'x') USING 10 SAMPLES
            BY p;
        '''
        result = bdb.execute(bql).fetchall()
        assert len(result) == 1
        # Multivariate targets.
        bql = '''
            ESTIMATE
                MUTUAL INFORMATION OF (a, d) WITH b GIVEN
                    (c = 1, e = 'x') USING 10 SAMPLES
            BY p;
        '''
        result = bdb.execute(bql).fetchall()
        assert len(result) == 1


def test_estimate_cmi_marginal_condition__ci_slow():
    with smoke_bdb() as bdb:
        # Univariate targets.
        bql = '''
            ESTIMATE
                MUTUAL INFORMATION OF a WITH b GIVEN (d) USING 10 SAMPLES
            BY p;
        '''
        result = bdb.execute(bql).fetchall()
        assert len(result) == 1
        # Multivariate targets.
        bql = '''
            ESTIMATE
                MUTUAL INFORMATION OF (a, b) WITH (c) GIVEN (d=1, e)
                USING 10 SAMPLES
            BY p;
        '''
        result = bdb.execute(bql).fetchall()
        assert len(result) == 1


def test_estimate_cmi_equality_marginal_condition__ci_slow():
    with smoke_bdb() as bdb:
        # Univariate targets.
        bql = '''
            ESTIMATE
                MUTUAL INFORMATION OF a WITH b
                    GIVEN (d, c = 1) USING 10 SAMPLES
            BY p;
        '''
        result = bdb.execute(bql).fetchall()
        assert len(result) == 1
        # Multivariate targets.
        bql = '''
            ESTIMATE
                MUTUAL INFORMATION OF a WITH (e, b)
                    GIVEN (d, c = 1) USING 10 SAMPLES
            BY p;
        '''
        result = bdb.execute(bql).fetchall()
        assert len(result) == 1


def test_simulate_cmi__ci_slow():
    with smoke_bdb() as bdb:

        # Univariate targets.
        bdb.execute('''
            CREATE TABLE f1 AS
                SIMULATE
                    MUTUAL INFORMATION OF a WITH b USING 10 SAMPLES
                        AS "mutinf(a,b)"
                FROM MODELS OF p
        ''')
        cursor = bdb.execute('SELECT * FROM f1;')
        assert cursor.description[0][0] == 'mutinf(a,b)'
        results = cursor.fetchall()
        assert len(results) == 20
        assert all(len(r) == 1 for r in results)

        # Univariate and multivariate targets.
        bdb.execute('''
            CREATE TABLE IF NOT EXISTS f2 AS
                SIMULATE
                    MUTUAL INFORMATION OF a WITH b USING 10 SAMPLES
                        AS "mutinf2(a,b)",
                    MUTUAL INFORMATION OF a WITH (e, b) GIVEN (c=1, d)
                        USING 2 SAMPLES AS "mutinf2(a,bf|c=1,d)"
                FROM MODELS OF p MODELED BY m1
        ''')
        cursor = bdb.execute('SELECT * FROM f2;')
        assert cursor.description[0][0] == 'mutinf2(a,b)'
        assert cursor.description[1][0] == 'mutinf2(a,bf|c=1,d)'
        # m1 has 10 models, so expect 10 results.
        results = cursor.fetchall()
        assert len(results) == 10
        assert all(len(r) == 2 for r in results)

        # f1 already exists
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE TABLE f1 AS
                    SIMULATE
                        MUTUAL INFORMATION OF a WITH b USING 10 SAMPLES
                            AS "mutinf(a,b)"
                    FROM MODELS OF p MODELED BY m1
            ''')

        # f1 already exists, so this query should not be run. If it was
        # run then the cursor would have 10 results instead of 20 from the
        # previous invocation.
        bdb.execute('''
            CREATE TABLE IF NOT EXISTS f1 AS
                SIMULATE
                    MUTUAL INFORMATION OF a WITH b USING 10 SAMPLES
                        AS "mutinf(a,b)"
                FROM MODELS OF p
                MODELED BY m1
        ''')
        cursor = bdb.execute('SELECT * FROM f1;')
        assert cursor.description[0][0] == 'mutinf(a,b)'
        assert len(cursor.fetchall()) == 20


def test_simulate_cmi_missing_table():
    with smoke_bdb() as bdb:
        bdb.execute('''
            SIMULATE MUTUAL INFORMATION OF a WITH b USING 10 SAMPLES
            FROM MODELS OF p;
        ''')
        bdb.execute('''
            SIMULATE MUTUAL INFORMATION OF a WITH b USING 10 SAMPLES
            FROM MODELS OF p
            MODELED BY m1;
        ''')
        bdb.execute('''
            SIMULATE 1 + MUTUAL INFORMATION OF a WITH b USING 10 SAMPLES
            FROM MODELS OF p
            MODELED BY m1;
        ''')

def test_estimate_cmi_bound():
    with smoke_bdb() as bdb:
        bdb.execute('''
            ESTIMATE PROBABILITY OF
                    (MUTUAL INFORMATION OF a WITH b USING 10 SAMPLES > 0.5)
                WITHIN p
        ''')

def test_simulate_cmi_missing_models_of():
    # SIMULATE of MUTUAL INFORMATION requires FROM MODELS OF, so specifying
    # a non population quantity should raise.
    with smoke_bdb() as bdb:
        with pytest.raises(BQLParseError):
            bdb.execute('''
                SIMULATE MUTUAL INFORMATION OF a WITH b USING 10 SAMPLES
                FROM p LIMIT 10;
            ''')

def test_simulate_models_population_variables():
    # SIMULATE FROM MODELS OF does not accept population variables.
    with smoke_bdb() as bdb:
        with pytest.raises(BQLParseError):
            bdb.execute('''
                SIMULATE a, b FROM MODELS OF p LIMIT 10;
            ''')

def test_smoke_loom_mi():
    """Smoke test MI with Loom."""
    with smoke_loom() as bdb:
        # Checking whether there is near-zero MI between parents.
        bql = '''
            ESTIMATE
                MUTUAL INFORMATION OF a WITH b USING 2 SAMPLES
            BY p
            MODELED BY m
        '''
        result = bdb.execute(bql).fetchall()
        assert len(result) == 1

def test_smoke_loom_conditional_mi():
    """Smoke test CMI with Loom."""
    with smoke_loom() as bdb:
        # Checking whether there is near-zero MI between parents.
        bql = '''
            ESTIMATE
                MUTUAL INFORMATION OF a WITH b GIVEN (c = 0)
                    USING 2 SAMPLES
            BY p;
        '''
        result = bdb.execute(bql).fetchall()
        assert len(result) == 1


def test_smoke_loom_marginalizing_conditional_mi():
    """Smoke test marginal CMI with Loom."""
    with smoke_loom() as bdb:
        # Checking whether there is near-zero MI between parents.
        bql = '''
            ESTIMATE
                MUTUAL INFORMATION OF a WITH b GIVEN (c)
                    USING 2 SAMPLES
            BY p;
        '''
        result = bdb.execute(bql).fetchall()
        assert len(result) == 1

# Define a tolerance for comparing CMI values to zero.
N_DIGITS = 2

@stochastic(max_runs=4, min_passes=3)
def test_assess_cmi_independent_columns__ci_slow(seed):
    """Assess whether the correct indepencies hold."""
    with bdb_for_checking_cmi('loom', 50) as bdb:
        # Checking whether there is near-zero MI between parents.
        bql_mi_parents = '''
            ESTIMATE
                MUTUAL INFORMATION OF a WITH b USING 100 SAMPLES
            BY p
            MODELED BY m
        '''
        result_mi_parents = bdb.execute(bql_mi_parents).fetchall()[0]
        # Test independence of parents
        assert np.isclose(result_mi_parents, 0, atol=10**-N_DIGITS)

        # Assess whether conditioning on child-value breaks independence.
        bql_cond_mi = '''
            ESTIMATE
                MUTUAL INFORMATION OF a WITH b GIVEN (c = 0)
                    USING 100 SAMPLES
            BY p;
        '''
        result_cond_mi = bdb.execute(bql_cond_mi).fetchall()[0]
        # Test conditional dependence.
        assert not np.isclose(result_cond_mi, 0, atol=10**-N_DIGITS)

        # Assess whether conditioning on the marginal child breaks independence
        bql_cond_mi_margin = '''
            ESTIMATE
                MUTUAL INFORMATION OF a WITH b GIVEN (c)
                    USING 100 SAMPLES
            BY p;
        '''
        result_cond_mi_marginal = bdb.execute(bql_cond_mi_margin).fetchall()[0]
        # Test marginal conditional dependence.
        assert not np.isclose(result_cond_mi_marginal, 0, atol=10**-N_DIGITS)
