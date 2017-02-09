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

import apsw
import contextlib
import itertools
import json
import pytest
import tempfile

import crosscat.LocalEngine
import crosscat.MultiprocessingEngine

import bayeslite
import bayeslite.bqlfn as bqlfn
import bayeslite.core as core
import bayeslite.guess as guess
import bayeslite.metamodel as metamodel

from bayeslite.metamodels.crosscat import CrosscatMetamodel

from bayeslite import bql_quote_name
from bayeslite.sqlite3_util import sqlite3_connection
from bayeslite.util import cursor_value

import test_csv

def powerset(s):
    s = list(s)
    combinations = (itertools.combinations(s, r) for r in range(len(s) + 1))
    return itertools.chain.from_iterable(combinations)

def local_crosscat():
    return crosscat.LocalEngine.LocalEngine(seed=0)

def multiprocessing_crosscat():
    return crosscat.MultiprocessingEngine.MultiprocessingEngine(seed=0)

@contextlib.contextmanager
def bayesdb(metamodel=None, **kwargs):
    if metamodel is None:
        crosscat = local_crosscat()
        metamodel = CrosscatMetamodel(crosscat)
    bdb = bayeslite.bayesdb_open(builtin_metamodels=False, **kwargs)
    bayeslite.bayesdb_register_metamodel(bdb, metamodel)
    try:
        yield bdb
    finally:
        bdb.close()

def test_bayesdb_instantiation():
    # Use bayesdb_open -- don't instantiate directly.
    with pytest.raises(TypeError):
        bayeslite.BayesDB()  # pylint: disable=no-value-for-parameter
    with pytest.raises(ValueError):
        bayeslite.BayesDB(':memory:', 0xdeadbeef)

def test_prng_determinism():
    bdb = bayeslite.bayesdb_open(builtin_metamodels=False)
    assert bdb._prng.weakrandom_uniform(1000) == 303
    assert bdb.py_prng.uniform(0, 1) == 0.6156331606142532
    assert bdb.np_prng.uniform(0, 1) == 0.28348770982811367

def test_openclose():
    with bayesdb():
        pass

def test_bad_db_application_id():
    with tempfile.NamedTemporaryFile(prefix='bayeslite') as f:
        with sqlite3_connection(f.name) as db:
            db.cursor().execute('PRAGMA application_id = 42')
            db.cursor().execute('PRAGMA user_version = 3')
        with pytest.raises(IOError):
            with bayesdb(pathname=f.name):
                pass

def test_bad_db_user_version():
    # XXX Would be nice to avoid a named temporary file here.  Pass
    # the sqlite3 database connection in?
    with tempfile.NamedTemporaryFile(prefix='bayeslite') as f:
        with sqlite3_connection(f.name) as db:
            db.cursor().execute('PRAGMA application_id = 1113146434')
            db.cursor().execute('PRAGMA user_version = 42')
        with pytest.raises(IOError):
            with bayesdb(pathname=f.name):
                pass

class DotdogMetamodel(metamodel.IBayesDBMetamodel):
    def name(self):
        return 'dotdog'
    def register(self, bdb):
        sql = '''
            INSERT OR IGNORE INTO bayesdb_metamodel
                VALUES ('dotdog', 42)
        '''
        bdb.sql_execute(sql)
    def create_generator(self, bdb, generator_id, schema_tokens, **kwargs):
        pass

def test_hackmetamodel():
    bdb = bayeslite.bayesdb_open(builtin_metamodels=False)
    bdb.sql_execute('CREATE TABLE t(a INTEGER, b TEXT)')
    bdb.sql_execute("INSERT INTO t (a, b) VALUES (42, 'fnord')")
    bdb.sql_execute('CREATE TABLE u AS SELECT * FROM t')
    bdb.execute('CREATE POPULATION p FOR t(b IGNORE; a NUMERICAL)')
    with pytest.raises(bayeslite.BQLError):
        bdb.execute('CREATE GENERATOR p_cc FOR p USING crosscat(a NUMERICAL)')
    with pytest.raises(bayeslite.BQLError):
        bdb.execute('CREATE GENERATOR p_dd FOR p USING dotdog(a NUMERICAL)')
    dotdog_metamodel = DotdogMetamodel()
    bayeslite.bayesdb_register_metamodel(bdb, dotdog_metamodel)
    bayeslite.bayesdb_deregister_metamodel(bdb, dotdog_metamodel)
    bayeslite.bayesdb_register_metamodel(bdb, dotdog_metamodel)
    with pytest.raises(bayeslite.BQLError):
        bdb.execute('CREATE GENERATOR p_cc FOR p USING crosscat(a NUMERICAL)')
    bdb.execute('CREATE GENERATOR p_dd FOR p USING dotdog(a NUMERICAL)')
    with pytest.raises(bayeslite.BQLError):
        bdb.execute('CREATE GENERATOR p_dd FOR p USING dotdog(a NUMERICAL)')
    with pytest.raises(bayeslite.BQLError):
        bdb.execute('CREATE GENERATOR p_cc FOR p USING crosscat(a NUMERICAL)')
    with pytest.raises(bayeslite.BQLError):
        bdb.execute('CREATE GENERATOR p_dd FOR p USING dotdog(a NUMERICAL)')
    # XXX Rest of test originally exercised default metamodel, but
    # syntax doesn't support that now.  Not clear that's wrong either.
    bdb.execute('CREATE GENERATOR q_dd FOR p USING dotdog(a NUMERICAL)')
    with pytest.raises(bayeslite.BQLError):
        bdb.execute('CREATE GENERATOR q_dd FOR p USING dotdog(a NUMERICAL)')

@contextlib.contextmanager
def bayesdb_population(mkbdb, tab, pop, gen, table_schema, data, columns,
        metamodel_name='crosscat'):
    with mkbdb as bdb:
        table_schema(bdb)
        data(bdb)
        qt = bql_quote_name(tab)
        qp = bql_quote_name(pop)
        qg = bql_quote_name(gen)
        qmm = bql_quote_name(metamodel_name)
        bdb.execute('CREATE POPULATION %s FOR %s(%s)' %
            (qp, qt, ';'.join(columns)))
        bdb.execute('CREATE GENERATOR %s FOR %s USING %s(%s)' %
            (qg, qp, qmm, ','.join(columns)))
        population_id = core.bayesdb_get_population(bdb, pop)
        generator_id = core.bayesdb_get_generator(bdb, population_id, gen)
        yield bdb, population_id, generator_id

@contextlib.contextmanager
def analyzed_bayesdb_population(mkbdb, nmodels, nsteps, max_seconds=None):
    with mkbdb as (bdb, population_id, generator_id):
        generator = core.bayesdb_generator_name(bdb, generator_id)
        qg = bql_quote_name(generator)
        bql = 'INITIALIZE %d MODELS FOR %s' % (nmodels, qg)
        bdb.execute(bql)
        # XXX Syntax currently doesn't support both nsteps and
        # max_seconds.
        duration = None
        if max_seconds:
            duration = '%d SECONDS' % (max_seconds,)
        else:
            duration = '%d ITERATIONS' % (nsteps,)
        bql = 'ANALYZE %s FOR %s WAIT' % (qg, duration)
        bdb.execute(bql)
        yield bdb, population_id, generator_id

def bayesdb_maxrowid(bdb, population_id):
    table_name = core.bayesdb_population_table(bdb, population_id)
    qt = bql_quote_name(table_name)
    sql = 'SELECT MAX(_rowid_) FROM %s' % (qt,)
    return cursor_value(bdb.sql_execute(sql))

def test_casefold_colname():
    def t(tname, pname, gname, sql, *args, **kwargs):
        def schema(bdb):
            bdb.sql_execute(sql)
        def data(_bdb):
            pass
        return bayesdb_population(bayesdb(), tname, pname, gname, schema, data,
            *args, **kwargs)
    with pytest.raises(apsw.SQLError):
        with t('t', 'p', 'p_cc', 'create table t(x, X)', []):
            pass
    with pytest.raises(bayeslite.BQLError):
        columns = ['x CATEGORICAL', 'x CATEGORICAL']
        with t('t', 'p', 'p_cc', 'create table t(x, y)', columns):
            pass
    with pytest.raises(bayeslite.BQLError):
        columns = ['x CATEGORICAL', 'X CATEGORICAL']
        with t('t', 'p', 'p_cc', 'create table t(x, y)', columns):
            pass
    with pytest.raises(bayeslite.BQLError):
        columns = ['x CATEGORICAL', 'X NUMERICAL']
        with t('t', 'p', 'p_cc', 'create table t(x, y)', columns):
            pass
    columns = ['x CATEGORICAL', 'y CATEGORICAL']
    with t('t', 'p', 'p_cc', 'create table t(x, y)', columns):
        pass
    columns = ['X CATEGORICAL', 'y CATEGORICAL']
    with t('t', 'p', 'p_cc', 'create table t(x, y)', columns):
        pass
    columns = ['x CATEGORICAL', 'Y NUMERICAL']
    with t('t', 'p', 'p_cc', 'CREATE TABLE T(X, Y)', columns):
        pass

def t0_schema(bdb):
    bdb.sql_execute('create table t0 (id integer primary key, n integer)')
def t0_data(bdb):
    for row in [(0, 0), (1, 1), (2, 42), (3, 87)]:
        bdb.sql_execute('insert into t0 (id, n) values (?, ?)', row)

def t0():
    return bayesdb_population(
        bayesdb(), 't0', 'p0', 'p0_cc', t0_schema, t0_data,
        columns=['id IGNORE', 'n NUMERICAL'])

def test_t0_badname():
    with pytest.raises(bayeslite.BQLError):
        with bayesdb_population(
                bayesdb(), 't0', 'p0', 't0_cc', t0_schema, t0_data,
                columns=['n CATEGORICAL', 'm CATEGORICAL']):
            pass

def t1_schema(bdb):
    bdb.sql_execute('''
        create table t1 (
            id integer primary key,
            label text,
            age double,
            weight double
        )
    ''')
def t1_data(bdb):
    for row in t1_rows:
        bdb.sql_execute('insert into t1 (label,age,weight) values (?,?,?)',
            row)

t1_rows = [
    ('foo', 12, 24),
    ('bar', 14, 28),
    (None, 10, 20),
    ('baz', None, 32),
    ('quux', 4, None),
    ('zot', 8, 16),
    ('mumble', 8, 16),
    ('frotz', 8, 16),
    ('gargle', 8, 16),
    ('mumph', 8, 16),
    ('hunf', 11, 22),
    ('blort', 16, 32),
    (None, 16, 32),
    (None, 17, 34),
    (None, 18, 36),
    (None, 19, 38),
    (None, 20, 40),
    (None, 21, 42),
    (None, 22, 44),
    (None, 23, 46),
    (None, 24, 48),
    (None, 25, 50),
    (None, 26, 52),
    (None, 27, 54),
    (None, 28, 56),
    (None, 29, 58),
    (None, 30, 60),
    (None, 31, 62),
]

def t1(*args, **kwargs):
    return bayesdb_population(
        bayesdb(*args, **kwargs), 't1', 'p1', 'p1_cc',
        t1_schema, t1_data,
        columns=['id IGNORE', 'label CATEGORICAL',
            'age NUMERICAL', 'weight NUMERICAL',])

def t1_sub():
    return bayesdb_population(bayesdb(), 't1', 'p1', 'p1_sub_cc',
        t1_schema, t1_data,
        columns=['id IGNORE', 'age IGNORE', 'label CATEGORICAL',
            'weight NUMERICAL',])

def t1_subcat():
    return bayesdb_population(bayesdb(), 't1', 'p1', 'p1_subcat_cc',
        t1_schema, t1_data,
        columns=['id IGNORE', 'age IGNORE','label CATEGORICAL',
            'weight CATEGORICAL'])

def t1_mp():
    crosscat = multiprocessing_crosscat()
    metamodel = CrosscatMetamodel(crosscat)
    return bayesdb_population(bayesdb(metamodel=metamodel),
        't1', 'p1', 'p1_cc', t1_schema, t1_data,
         columns=['id IGNORE','label CATEGORICAL', 'age NUMERICAL',
            'weight NUMERICAL'])

def t2_schema(bdb):
    bdb.sql_execute('''create table t2 (id, label, age, weight)''')

def t2_data(bdb):
    bdb.sql_execute('''
        insert into t2 (label,age,weight) values
            ('1', '2', ?),
            ('2', '3', '1.2'),
            ('3', '48', '3e10'),
            ('4', '3', ?)
    ''', (2/3., -0.,))

def t2():
    return bayesdb_population(
        bayesdb(), 't2', 'p2', 'p2_cc', t2_schema, t2_data,
        columns=['id IGNORE','label CATEGORICAL', 'age CATEGORICAL',
            'weight CATEGORICAL'])

def test_t1_nokey():
    with bayesdb_population(
            bayesdb(), 't1', 'p1', 'p1_cc', t1_schema, t1_data,
            columns=['id IGNORE','label IGNORE', 'age NUMERICAL',
                'weight NUMERICAL']):
        pass

def test_t1_nocase():
    with bayesdb_population(bayesdb(), 't1', 'p1', 'p1_cc', t1_schema, t1_data,
            columns=[
                'id IGNORE',
                'label CATEGORICAL',
                'age NUMERICAL',
                'weight NUMERICAL',
            ]) \
            as (bdb, population_id, generator_id):
        bdb.execute('select id from t1').fetchall()
        bdb.execute('select ID from T1').fetchall()
        bdb.execute('select iD from T1').fetchall()
        bdb.execute('select Id from T1').fetchall()
        # bdb.execute('select id from p1_cc').fetchall()
        # bdb.execute('select ID from P1_cC').fetchall()
        # bdb.execute('select iD from P1_Cc').fetchall()
        # bdb.execute('select Id from P1_CC').fetchall()

examples = {
    't0': t0,
    't1': t1,
#    't1_mp': t1_mp,
    't1_sub': t1_sub,
    't1_subcat': t1_subcat,
    't2': t2,
}

@pytest.mark.parametrize('exname', examples.keys())
def test_example(exname):
    with examples[exname]():
        pass

@pytest.mark.parametrize('exname', examples.keys())
def test_example_analysis0(exname):
    with analyzed_bayesdb_population(examples[exname](), 1, 0):
        pass

@pytest.mark.parametrize('exname', examples.keys())
def test_example_analysis1(exname):
    with analyzed_bayesdb_population(examples[exname](), 1, 1):
        pass

# The multiprocessing engine has a large overhead, too much to try
# every normal test with it, so we'll just run this one test to make
# sure it doesn't crash and burn with ten models.
def test_t1_mp_analysis():
    with analyzed_bayesdb_population(t1_mp(), 10, 2):
        pass

def test_t1_mp_analysis_time_deadline():
    with analyzed_bayesdb_population(t1_mp(), 10, None, max_seconds=1):
        pass

def test_t1_mp_analysis_iter_deadline__ci_slow():
    with analyzed_bayesdb_population(t1_mp(), 10, 1, max_seconds=10):
        pass

def test_t1_analysis_time_deadline():
    with analyzed_bayesdb_population(t1(), 10, None, max_seconds=1):
        pass

def test_t1_analysis_iter_deadline__ci_slow():
    with analyzed_bayesdb_population(t1(), 10, 1, max_seconds=10):
        pass

@pytest.mark.parametrize('rowid,colno,confidence',
    [(i+1, j, conf)
        for i in range(min(5, len(t1_rows)))
        for j in range(1,3)
        for conf in [0.01, 0.5, 0.99]])
def test_t1_predict(rowid, colno, confidence):
    with analyzed_bayesdb_population(t1(), 1, 1) as (bdb, pop_id, gen_id):
        if rowid == 0: rowid = bayesdb_maxrowid(bdb, pop_id)
        bqlfn.bql_predict(bdb, pop_id, None, rowid, colno, confidence, None)

@pytest.mark.parametrize('colnos,constraints,numpredictions',
    [(colnos, constraints, numpred)
        for colnos in powerset(range(1,3))
        for constraints in [None] + list(powerset(range(1,3)))
        for numpred in range(3)])
def test_t1_simulate(colnos, constraints, numpredictions):
    if len(colnos) == 0:
        # No need to try this or confirm it fails gracefully --
        # nothing should be trying it anyway, and bayeslite_simulate
        # is not exposed to users of the bayeslite API.
        return
    with analyzed_bayesdb_population(t1(), 1, 1) \
            as (bdb, population_id, generator_id):
        if constraints is not None:
            rowid = 1           # XXX Avoid hard-coding this.
            # Can't use t1_rows[0][i] because not all t1-based tables
            # use the same column indexing -- some use a subset of the
            # columns.
            #
            # XXX Automatically test the correct exception.
            constraints = [
                (i, core.bayesdb_population_cell_value(
                    bdb, population_id, rowid, i))
                for i in constraints
            ]
        bqlfn.bayesdb_simulate(bdb, population_id, constraints, colnos,
            numpredictions=numpredictions)

@pytest.mark.parametrize('exname,colno',
    [(exname, colno)
        for exname in examples.keys()
        for colno in range(1,3)])
def test_onecolumn(exname, colno):
    if exname == 't0' and colno > 0:
        pytest.skip('Not enough columns in %s.' % (exname,))
    if exname.startswith('t1_sub') and colno > 1:
        pytest.skip('Not enough columns in %s.' % (exname,))
    with analyzed_bayesdb_population(examples[exname](), 1, 1) \
            as (bdb, population_id, generator_id):
        bqlfn.bql_column_value_probability(bdb, population_id, None, colno, 4)
        bdb.sql_execute('select bql_column_value_probability(?, NULL, ?, 4)',
            (population_id, colno)).fetchall()

@pytest.mark.parametrize('exname,colno0,colno1',
    [(exname, colno0, colno1)
        for exname in examples.keys()
        for colno0 in range(1,3)
        for colno1 in range(1,3)])
def test_twocolumn(exname, colno0, colno1):
    if exname == 't0':
        pytest.skip('Not enough columns in t0.')
    if exname.startswith('t1_sub') and (colno0 > 1 or colno1 > 1):
        pytest.skip('Not enough columns in %s.' % (exname,))
    with analyzed_bayesdb_population(examples[exname](), 1, 1) \
            as (bdb, population_id, generator_id):
        bqlfn.bql_column_correlation(bdb, population_id, None, colno0, colno1)
        bdb.sql_execute('select bql_column_correlation(?, NULL, ?, ?)',
            (population_id, colno0, colno1)).fetchall()
        bqlfn.bql_column_dependence_probability(bdb, population_id, None,
            colno0, colno1)
        bdb.sql_execute('select'
            ' bql_column_dependence_probability(?, NULL, ?, ?)',
            (population_id, colno0, colno1)).fetchall()
        colno0_json = json.dumps([colno0])
        colno1_json = json.dumps([colno1])
        bqlfn.bql_column_mutual_information(
            bdb, population_id, None, colno0_json, colno1_json, None)
        bqlfn.bql_column_mutual_information(
            bdb, population_id, None, colno0_json, colno1_json, None)
        bqlfn.bql_column_mutual_information(
            bdb, population_id, None, colno0_json, colno1_json, 1)
        bdb.sql_execute('select'
            ' bql_column_mutual_information(?, NULL, ?, ?, NULL)',
            (population_id, colno0_json, colno1_json)).fetchall()
        bdb.sql_execute('select'
            ' bql_column_mutual_information(?, NULL, ?, ?, 1)',
            (population_id, colno0_json, colno1_json)).fetchall()
        bdb.sql_execute('select'
            ' bql_column_mutual_information(?, NULL, ?, ?, 100)',
            (population_id, colno0_json, colno1_json)).fetchall()

@pytest.mark.parametrize('colno,rowid',
    [(colno, rowid)
        for colno in range(1,4)
        for rowid in range(6)])
def test_t1_column_value_probability(colno, rowid):
    with analyzed_bayesdb_population(t1(), 1, 1) \
            as (bdb, population_id, generator_id):
        if rowid == 0:
            rowid = bayesdb_maxrowid(bdb, population_id)
        value = core.bayesdb_population_cell_value(
            bdb, population_id, rowid, colno)
        bqlfn.bql_column_value_probability(bdb, population_id, None, colno,
            value)
        table_name = core.bayesdb_population_table(bdb, population_id)
        var = core.bayesdb_variable_name(bdb, population_id, colno)
        qt = bql_quote_name(table_name)
        qv = bql_quote_name(var)
        sql = '''
            select bql_column_value_probability(?, NULL, ?,
                (select %s from %s where rowid = ?))
        ''' % (qv, qt)
        bdb.sql_execute(sql, (population_id, colno, rowid)).fetchall()

@pytest.mark.parametrize('exname,source,target,colnos',
    [(exname, source, target, list(colnos))
        for exname in examples.keys()
        for source in range(1,3)
        for target in range(2,4)
        for colnos in powerset(range(1,3))])
def test_row_similarity(exname, source, target, colnos):
    if exname == 't0' and any(colno > 0 for colno in colnos):
        pytest.skip('Not enough columns in t0.')
    if exname.startswith('t1_sub') and any(colno > 1 for colno in colnos):
        pytest.skip('Not enough columns in %s.' % (exname,))
    with analyzed_bayesdb_population(examples[exname](), 1, 1) \
            as (bdb, population_id, generator_id):
        popcols = core.bayesdb_variable_numbers(
            bdb, population_id, generator_id)
        # Forbid multiple columns specified in WITH RESPECT TO.
        def test_row_similarity_one(f):
            try:
                f()
                if len(colnos) != 1 and len(popcols) != 1:
                    pytest.fail('No exception on similarity with respect to.')
            except bayeslite.BQLError:
                if len(colnos) == 1:
                    pytest.fail('Bad exception on similarity with respect to.')
        def f_api():
            bqlfn.bql_row_similarity(
                bdb, population_id, None, source, target, *colnos)
        def f_sql():
            sql = 'select bql_row_similarity(?, NULL, ?, ?%s%s)' % \
                ('' if 0 == len(colnos) else ', ', ', '.join(map(str, colnos)))
            bdb.sql_execute(sql, (population_id, source, target)).fetchall()
        test_row_similarity_one(f_sql)
        test_row_similarity_one(f_api)

@pytest.mark.parametrize('exname,rowid,colno',
    [(exname, rowid, colno)
        for exname in examples.keys()
        for rowid in range(4)
        for colno in range(1,3)])
def test_row_column_predictive_probability(exname, rowid, colno):
    if exname == 't0' and colno > 1:
        pytest.skip('Not enough columns in t0.')
    if exname.startswith('t1_sub') and colno > 1:
        pytest.skip('Not enough columns in %s.' % (exname,))
    with analyzed_bayesdb_population(examples[exname](), 1, 1) \
            as (bdb, population_id, generator_id):
        if rowid == 0: rowid = bayesdb_maxrowid(bdb, population_id)
        bqlfn.bql_row_column_predictive_probability(bdb, population_id, None,
            rowid, colno)
        sql = 'select bql_row_column_predictive_probability(?, NULL, ?, ?)'
        bdb.sql_execute(sql, (population_id, rowid, colno)).fetchall()

def test_crosscat_constraints():
    class FakeEngine(crosscat.LocalEngine.LocalEngine):
        def predictive_probability_multistate(self, M_c, X_L_list,
                X_D_list, Y, Q):
            self._last_Y = Y
            sup = super(FakeEngine, self)
            return sup.simple_predictive_probability_multistate(M_c=M_c,
                X_L_list=X_L_list, X_D_list=X_D_list, Y=Y, Q=Q)
        def simple_predictive_sample(self, seed, M_c, X_L, X_D, Y, Q, n):
            self._last_Y = Y
            return super(FakeEngine, self).simple_predictive_sample(seed=seed,
                M_c=M_c, X_L=X_L, X_D=X_D, Y=Y, Q=Q, n=n)
        def impute_and_confidence(self, seed, M_c, X_L, X_D, Y, Q, n):
            self._last_Y = Y
            return super(FakeEngine, self).impute_and_confidence(seed=seed,
                M_c=M_c, X_L=X_L, X_D=X_D, Y=Y, Q=Q, n=n)
    engine = FakeEngine(seed=0)
    mm = CrosscatMetamodel(engine)
    with bayesdb(metamodel=mm) as bdb:
        t1_schema(bdb)
        t1_data(bdb)
        bdb.execute('''
            CREATE POPULATION p1 FOR t1 (
                id IGNORE;
                label CATEGORICAL;
                age NUMERICAL;
                weight NUMERICAL
            )
        ''')
        bdb.execute('''
            CREATE GENERATOR p1_cc FOR p1 USING crosscat(
                label CATEGORICAL,
                age NUMERICAL,
                weight NUMERICAL
            )
        ''')
        pid = core.bayesdb_get_population(bdb, 'p1')
        assert core.bayesdb_variable_number(bdb, pid, None, 'label') == 1
        assert core.bayesdb_variable_number(bdb, pid, None, 'age') == 2
        assert core.bayesdb_variable_number(bdb, pid, None, 'weight') == 3
        gid = core.bayesdb_get_generator(bdb, pid, 'p1_cc')
        from bayeslite.metamodels.crosscat import crosscat_cc_colno
        assert crosscat_cc_colno(bdb, gid, 1) == 0
        assert crosscat_cc_colno(bdb, gid, 2) == 1
        assert crosscat_cc_colno(bdb, gid, 3) == 2
        bdb.execute('INITIALIZE 1 MODEL FOR p1_cc')
        bdb.execute('ANALYZE p1_cc FOR 1 ITERATION WAIT')
        bdb.execute('ESTIMATE PROBABILITY OF age = 8 GIVEN (weight = 16)'
            ' BY p1').next()
        assert engine._last_Y == [(28, 2, 16)]
        bdb.execute("SELECT age FROM t1 WHERE label = 'baz'").next()
        bdb.execute("INFER age FROM p1 WHERE label = 'baz'").next()
        assert engine._last_Y == [(3, 0, 1), (3, 2, 32)]
        bdb.execute('SIMULATE weight FROM p1 GIVEN age = 8 LIMIT 1').next()
        assert engine._last_Y == [(28, 1, 8)]
        # Simulate with an unknown nominal value should throw an error.
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('SIMULATE weight FROM p1 GIVEN label = \'q\' LIMIT 1;')

def test_bayesdb_population_fresh_row_id():
    with bayesdb_population(
            bayesdb(), 't1', 'p1', 'p1_cc', t1_schema, lambda x: 0,\
            columns=['id IGNORE','label CATEGORICAL', 'age NUMERICAL',
                'weight NUMERICAL'])\
            as (bdb, population_id, generator_id):
        assert core.bayesdb_population_fresh_row_id(bdb, population_id) == 1
        t1_data(bdb)
        assert core.bayesdb_population_fresh_row_id(bdb, population_id) == \
            len(t1_rows) + 1

def test_bayesdb_population_add_variable():
    with bayesdb() as bdb:
        bdb.sql_execute('create table t (a real, b ignore, c real)')
        bdb.execute('''
            create population p for t with schema(
                model a, c as numerical;
                b ignore;
            );
        ''')
        population_id = core.bayesdb_get_population(bdb, 'p')
        # Checks column a.
        assert core.bayesdb_has_variable(bdb, population_id, None, 'a')
        assert core.bayesdb_table_column_number(bdb, 't', 'a') == 0
        assert core.bayesdb_variable_number(bdb, population_id, None, 'a') == 0
        # Checks column b, which is not in the population yet.
        assert not core.bayesdb_has_variable(bdb, population_id, None, 'b')
        assert core.bayesdb_table_column_number(bdb, 't', 'b') == 1
        # Checks column c.
        assert core.bayesdb_has_variable(bdb, population_id, None, 'c')
        assert core.bayesdb_table_column_number(bdb, 't', 'c') == 2
        assert core.bayesdb_variable_number(bdb, population_id, None, 'c') == 2
        # Cannot add variable 'c', already exists.
        with pytest.raises(apsw.ConstraintError):
            core.bayesdb_add_variable(bdb, population_id, 'c', 'nominal')
        # Cannot add variable 'b' with a bad stattype.
        with pytest.raises(apsw.ConstraintError):
            core.bayesdb_add_variable(bdb, population_id, 'b', 'quzz')
        # Now add column b to the population.
        core.bayesdb_add_variable(bdb, population_id, 'b', 'nominal')
        assert core.bayesdb_variable_number(bdb, population_id, None, 'b') == 1
        # Add a new column q to table t, then add it to population p.
        bdb.sql_execute('alter table t add column q real;')
        assert core.bayesdb_table_column_number(bdb, 't', 'q') == 3
        assert not core.bayesdb_has_variable(bdb, population_id, None, 'q')
        core.bayesdb_add_variable(bdb, population_id, 'q', 'numerical')
        assert core.bayesdb_has_variable(bdb, population_id, None, 'q')
        assert core.bayesdb_variable_number(bdb, population_id, None, 'q') == 3
