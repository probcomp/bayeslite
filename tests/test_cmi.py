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
import pytest

from bayeslite import bayesdb_open
from bayeslite.exception import BQLError
from bayeslite.exception import BQLParseError


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
                MODEL a, b, c, d AS NUMERICAL;
                MODEL e AS NOMINAL
            )
        ''')

        bdb.execute('CREATE METAMODEL m1 FOR p;')
        bdb.execute('INITIALIZE 10 MODELS FOR m1;')

        bdb.execute('CREATE METAMODEL m2 FOR p;')
        bdb.execute('INITIALIZE 10 MODELS FOR m2;')
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


def test_estiamte_cmi_marginal_condition__ci_slow():
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


def test_estiamte_cmi_equality_marginal_condition__ci_slow():
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
    # SIMULATE FROM MODELS without CREATE TABLE currently disabled, pending
    # either virtual tables or implementation of winding/unwinding business
    # in compiler.py
    with smoke_bdb() as bdb:
        # No modeled by.
        with pytest.raises(BQLError):
            bdb.execute('''
                SIMULATE MUTUAL INFORMATION OF a WITH b USING 10 SAMPLES
                FROM MODELS OF p;
            ''')
        # With modeled by.
        with pytest.raises(BQLError):
            bdb.execute('''
                SIMULATE MUTUAL INFORMATION OF a WITH b USING 10 SAMPLES
                FROM MODELS OF p
                MODELED BY m1;
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
