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
import pytest
import tempfile

import crosscat.LocalEngine
import crosscat.MultiprocessingEngine

import bayeslite
import bayeslite.bqlfn as bqlfn
import bayeslite.core as core
from bayeslite.metamodels.crosscat import CrosscatMetamodel
import bayeslite.guess as guess
import bayeslite.metamodel as metamodel

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
    def create_generator(self, bdb, table, schema, instantiate):
        instantiate(schema)

def test_hackmetamodel():
    bdb = bayeslite.bayesdb_open(builtin_metamodels=False)
    bdb.sql_execute('CREATE TABLE t(a INTEGER, b TEXT)')
    bdb.sql_execute("INSERT INTO t (a, b) VALUES (42, 'fnord')")
    bdb.sql_execute('CREATE TABLE u AS SELECT * FROM t')
    with pytest.raises(bayeslite.BQLError):
        bdb.execute('CREATE GENERATOR t_cc FOR t USING crosscat(a NUMERICAL)')
    with pytest.raises(bayeslite.BQLError):
        bdb.execute('CREATE GENERATOR t_dd FOR t USING dotdog(a NUMERICAL)')
    dotdog_metamodel = DotdogMetamodel()
    bayeslite.bayesdb_register_metamodel(bdb, dotdog_metamodel)
    bayeslite.bayesdb_deregister_metamodel(bdb, dotdog_metamodel)
    bayeslite.bayesdb_register_metamodel(bdb, dotdog_metamodel)
    with pytest.raises(bayeslite.BQLError):
        bdb.execute('CREATE GENERATOR t_cc FOR t USING crosscat(a NUMERICAL)')
    bdb.execute('CREATE GENERATOR t_dd FOR t USING dotdog(a NUMERICAL)')
    with pytest.raises(bayeslite.BQLError):
        bdb.execute('CREATE GENERATOR t_dd FOR t USING dotdog(a NUMERICAL)')
    with pytest.raises(bayeslite.BQLError):
        bdb.execute('CREATE GENERATOR t_cc FOR t USING crosscat(a NUMERICAL)')
    with pytest.raises(bayeslite.BQLError):
        bdb.execute('CREATE GENERATOR t_dd FOR t USING dotdog(a NUMERICAL)')
    # XXX Rest of test originally exercised default metamodel, but
    # syntax doesn't support that now.  Not clear that's wrong either.
    bdb.execute('CREATE GENERATOR u_dd FOR u USING dotdog(a NUMERICAL)')
    with pytest.raises(bayeslite.BQLError):
        bdb.execute('CREATE GENERATOR u_dd FOR u USING dotdog(a NUMERICAL)')

@contextlib.contextmanager
def bayesdb_generator(mkbdb, tab, gen, table_schema, data, columns,
        metamodel_name='crosscat'):
    with mkbdb as bdb:
        table_schema(bdb)
        data(bdb)
        qt = bql_quote_name(tab)
        qg = bql_quote_name(gen)
        qmm = bql_quote_name(metamodel_name)
        bdb.execute('CREATE GENERATOR %s FOR %s USING %s(%s)' %
            (qg, qt, qmm, ','.join(columns)))
        yield bdb, core.bayesdb_get_generator(bdb, gen)

@contextlib.contextmanager
def analyzed_bayesdb_generator(mkbdb, nmodels, nsteps, max_seconds=None):
    with mkbdb as (bdb, generator_id):
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
        yield bdb, generator_id

def bayesdb_maxrowid(bdb, generator_id):
    table_name = core.bayesdb_generator_table(bdb, generator_id)
    qt = bql_quote_name(table_name)
    sql = 'SELECT MAX(_rowid_) FROM %s' % (qt,)
    return cursor_value(bdb.sql_execute(sql))

def test_casefold_colname():
    def t(tname, gname, sql, *args, **kwargs):
        def schema(bdb):
            bdb.sql_execute(sql)
        def data(_bdb):
            pass
        return bayesdb_generator(bayesdb(), tname, gname, schema, data, *args,
            **kwargs)
    with pytest.raises(apsw.SQLError):
        with t('t', 't_cc', 'create table t(x, X)', []):
            pass
    with pytest.raises(bayeslite.BQLError):
        columns = ['x CATEGORICAL', 'x CATEGORICAL']
        with t('t', 't_cc', 'create table t(x, y)', columns):
            pass
    with pytest.raises(bayeslite.BQLError):
        columns = ['x CATEGORICAL', 'X CATEGORICAL']
        with t('t', 't_cc', 'create table t(x, y)', columns):
            pass
    with pytest.raises(bayeslite.BQLError):
        columns = ['x CATEGORICAL', 'X NUMERICAL']
        with t('t', 't_cc', 'create table t(x, y)', columns):
            pass
    columns = ['x CATEGORICAL', 'y CATEGORICAL']
    with t('t', 't_cc', 'create table t(x, y)', columns):
        pass
    columns = ['X CATEGORICAL', 'y CATEGORICAL']
    with t('t', 't_cc', 'create table t(x, y)', columns):
        pass
    columns = ['x CATEGORICAL', 'Y NUMERICAL']
    with t('t', 't_cc', 'CREATE TABLE T(X, Y)', columns):
        pass

def t0_schema(bdb):
    bdb.sql_execute('create table t0 (id integer primary key, n integer)')
def t0_data(bdb):
    for row in [(0, 0), (1, 1), (2, 42), (3, 87)]:
        bdb.sql_execute('insert into t0 (id, n) values (?, ?)', row)

def t0():
    return bayesdb_generator(bayesdb(), 't0', 't0_cc', t0_schema, t0_data,
        columns=['n NUMERICAL'])

def test_t0_badname():
    with pytest.raises(bayeslite.BQLError):
        with bayesdb_generator(bayesdb(), 't0', 't0_cc', t0_schema, t0_data,
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
    return bayesdb_generator(bayesdb(*args, **kwargs), 't1', 't1_cc',
        t1_schema, t1_data,
        columns=['label CATEGORICAL', 'age NUMERICAL', 'weight NUMERICAL'])

def t1_sub():
    return bayesdb_generator(bayesdb(), 't1', 't1_sub_cc',
        t1_schema, t1_data,
        columns=['label CATEGORICAL', 'weight NUMERICAL'])

def t1_subcat():
    return bayesdb_generator(bayesdb(), 't1', 't1_subcat_cc',
        t1_schema, t1_data,
        columns=['label CATEGORICAL', 'weight CATEGORICAL'])

def t1_mp():
    crosscat = multiprocessing_crosscat()
    metamodel = CrosscatMetamodel(crosscat)
    return bayesdb_generator(bayesdb(metamodel=metamodel),
        't1', 't1_cc', t1_schema, t1_data,
         columns=['label CATEGORICAL', 'age NUMERICAL', 'weight NUMERICAL'])

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
    return bayesdb_generator(bayesdb(), 't2', 't2_cc', t2_schema, t2_data,
        columns=['label CATEGORICAL', 'age CATEGORICAL', 'weight CATEGORICAL'])

def test_t1_nokey():
    with bayesdb_generator(bayesdb(), 't1', 't1_cc', t1_schema, t1_data,
            columns=['age NUMERICAL', 'weight NUMERICAL']):
        pass

def test_t1_nocase():
    with bayesdb_generator(bayesdb(), 't1', 't1_cc', t1_schema, t1_data,
            columns=[
                'label CATEGORICAL',
                'age NUMERICAL',
                'weight NUMERICAL',
            ]) \
            as (bdb, generator_id):
        bdb.execute('select id from t1').fetchall()
        bdb.execute('select ID from T1').fetchall()
        bdb.execute('select iD from T1').fetchall()
        bdb.execute('select Id from T1').fetchall()
        # bdb.execute('select id from t1_cc').fetchall()
        # bdb.execute('select ID from T1_cC').fetchall()
        # bdb.execute('select iD from T1_Cc').fetchall()
        # bdb.execute('select Id from T1_CC').fetchall()

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
    with analyzed_bayesdb_generator(examples[exname](), 1, 0):
        pass

@pytest.mark.parametrize('exname', examples.keys())
def test_example_analysis1(exname):
    with analyzed_bayesdb_generator(examples[exname](), 1, 1):
        pass

# The multiprocessing engine has a large overhead, too much to try
# every normal test with it, so we'll just run this one test to make
# sure it doesn't crash and burn with ten models.
def test_t1_mp_analysis():
    with analyzed_bayesdb_generator(t1_mp(), 10, 2):
        pass

def test_t1_mp_analysis_time_deadline():
    with analyzed_bayesdb_generator(t1_mp(), 10, None, max_seconds=1):
        pass

def test_t1_mp_analysis_iter_deadline__ci_slow():
    with analyzed_bayesdb_generator(t1_mp(), 10, 1, max_seconds=10):
        pass

def test_t1_analysis_time_deadline():
    with analyzed_bayesdb_generator(t1(), 10, None, max_seconds=1):
        pass

def test_t1_analysis_iter_deadline__ci_slow():
    with analyzed_bayesdb_generator(t1(), 10, 1, max_seconds=10):
        pass

@pytest.mark.parametrize('rowid,colno,confidence',
    [(i+1, j, conf)
        for i in range(min(5, len(t1_rows)))
        for j in range(1,3)
        for conf in [0.01, 0.5, 0.99]])
def test_t1_predict(rowid, colno, confidence):
    with analyzed_bayesdb_generator(t1(), 1, 1) as (bdb, generator_id):
        if rowid == 0: rowid = bayesdb_maxrowid(bdb, generator_id)
        bqlfn.bql_predict(bdb, generator_id, None, colno, rowid, confidence)

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
    with analyzed_bayesdb_generator(t1(), 1, 1) as (bdb, generator_id):
        if constraints is not None:
            rowid = 1           # XXX Avoid hard-coding this.
            # Can't use t1_rows[0][i] because not all t1-based tables
            # use the same column indexing -- some use a subset of the
            # columns.
            #
            # XXX Automatically test the correct exception.
            constraints = \
                [(i, bayesdb_generator_cell_value(bdb, generator_id, i, rowid))
                    for i in constraints]
        bqlfn.bayesdb_simulate(bdb, generator_id, constraints, colnos,
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
    with analyzed_bayesdb_generator(examples[exname](), 1, 1) \
            as (bdb, generator_id):
        bqlfn.bql_column_value_probability(bdb, generator_id, None, colno, 4)
        bdb.sql_execute('select bql_column_value_probability(?, NULL, ?, 4)',
            (generator_id, colno)).fetchall()

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
    with analyzed_bayesdb_generator(examples[exname](), 1, 1) \
            as (bdb, generator_id):
        bqlfn.bql_column_correlation(bdb, generator_id, colno0, colno1)
        bdb.sql_execute('select bql_column_correlation(?, ?, ?)',
            (generator_id, colno0, colno1)).fetchall()
        bqlfn.bql_column_dependence_probability(bdb, generator_id, None,
            colno0, colno1)
        bdb.sql_execute('select'
            ' bql_column_dependence_probability(?, NULL, ?, ?)',
            (generator_id, colno0, colno1)).fetchall()
        bqlfn.bql_column_mutual_information(bdb, generator_id, None, colno0,
            colno1)
        bqlfn.bql_column_mutual_information(bdb, generator_id, None, colno0,
            colno1, numsamples=None)
        bqlfn.bql_column_mutual_information(bdb, generator_id, None, colno0,
            colno1, numsamples=1)
        bdb.sql_execute('select'
            ' bql_column_mutual_information(?, NULL, ?, ?, NULL)',
            (generator_id, colno0, colno1)).fetchall()
        bdb.sql_execute('select'
            ' bql_column_mutual_information(?, NULL, ?, ?, 1)',
            (generator_id, colno0, colno1)).fetchall()
        bdb.sql_execute('select'
            ' bql_column_mutual_information(?, NULL, ?, ?, 100)',
            (generator_id, colno0, colno1)).fetchall()

@pytest.mark.parametrize('colno,rowid',
    [(colno, rowid)
        for colno in range(1,4)
        for rowid in range(6)])
def test_t1_column_value_probability(colno, rowid):
    with analyzed_bayesdb_generator(t1(), 1, 1) as (bdb, generator_id):
        if rowid == 0: rowid = bayesdb_maxrowid(bdb, generator_id)
        value = bayesdb_generator_cell_value(bdb, generator_id, colno, rowid)
        bqlfn.bql_column_value_probability(bdb, generator_id, None, colno,
            value)
        table_name = core.bayesdb_generator_table(bdb, generator_id)
        colname = core.bayesdb_generator_column_name(bdb, generator_id, colno)
        qt = bql_quote_name(table_name)
        qc = bql_quote_name(colname)
        sql = '''
            select bql_column_value_probability(?, NULL, ?,
                (select %s from %s where rowid = ?))
        ''' % (qc, qt)
        bdb.sql_execute(sql, (generator_id, colno, rowid)).fetchall()

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
    with analyzed_bayesdb_generator(examples[exname](), 1, 1) \
            as (bdb, generator_id):
        bqlfn.bql_row_similarity(bdb, generator_id, None, source, target,
            *colnos)
        sql = 'select bql_row_similarity(?, NULL, ?, ?%s%s)' % \
            ('' if 0 == len(colnos) else ', ', ', '.join(map(str, colnos)))
        bdb.sql_execute(sql, (generator_id, source, target)).fetchall()

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
    with analyzed_bayesdb_generator(examples[exname](), 1, 1) \
            as (bdb, generator_id):
        if rowid == 0: rowid = bayesdb_maxrowid(bdb, generator_id)
        bqlfn.bql_row_column_predictive_probability(bdb, generator_id, None,
            rowid, colno)
        sql = 'select bql_row_column_predictive_probability(?, NULL, ?, ?)'
        bdb.sql_execute(sql, (generator_id, rowid, colno)).fetchall()

def test_insert():
    with test_csv.bayesdb_csv_stream(test_csv.csv_data) as (bdb, f):
        bayeslite.bayesdb_read_csv(bdb, 't', f, header=True, create=True)
        guess.bayesdb_guess_generator(bdb, 't_cc', 't', 'crosscat')
        bdb.execute('initialize 2 models for t_cc')
        bdb.execute('analyze t_cc for 1 iteration wait')
        generator_id = core.bayesdb_get_generator(bdb, 't_cc')
        row = (41, 'F', 96000, 73, 'data science', 2)
        bqlfn.bayesdb_insert(bdb, generator_id, row)

def bayesdb_generator_cell_value(bdb, generator_id, colno, rowid):
    table_name = core.bayesdb_generator_table(bdb, generator_id)
    qt = bql_quote_name(table_name)
    colname = core.bayesdb_generator_column_name(bdb, generator_id, colno)
    qcn = bql_quote_name(colname)
    sql = 'SELECT %s FROM %s WHERE _rowid_ = ?' % (qcn, qt)
    cursor = bdb.sql_execute(sql, (rowid,))
    try:
        row = cursor.next()
    except StopIteration:
        assert False, 'Missing row at %d!' % (rowid,)
    else:
        return row[0]

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
            CREATE GENERATOR t1_cc FOR t1 USING crosscat(
                label CATEGORICAL,
                age NUMERICAL,
                weight NUMERICAL
            )
        ''')
        gid = core.bayesdb_get_generator(bdb, 't1_cc')
        assert core.bayesdb_generator_column_number(bdb, gid, 'label') == 1
        assert core.bayesdb_generator_column_number(bdb, gid, 'age') == 2
        assert core.bayesdb_generator_column_number(bdb, gid, 'weight') == 3
        from bayeslite.metamodels.crosscat import crosscat_cc_colno
        assert crosscat_cc_colno(bdb, gid, 1) == 0
        assert crosscat_cc_colno(bdb, gid, 2) == 1
        assert crosscat_cc_colno(bdb, gid, 3) == 2
        bdb.execute('INITIALIZE 1 MODEL FOR t1_cc')
        bdb.execute('ANALYZE t1_cc FOR 1 ITERATION WAIT')
        bdb.execute('ESTIMATE PROBABILITY OF age = 8 GIVEN (weight = 16)'
            ' BY t1_cc').next()
        assert engine._last_Y == [(28, 2, 16)]
        bdb.execute("SELECT age FROM t1 WHERE label = 'baz'").next()
        bdb.execute("INFER age FROM t1_cc WHERE label = 'baz'").next()
        assert engine._last_Y == [(3, 0, 1), (3, 2, 32)]
        bdb.execute('SIMULATE weight FROM t1_cc GIVEN age = 8 LIMIT 1').next()
        assert engine._last_Y == [(28, 1, 8)]

def test_bayesdb_generator_fresh_row_id():
    with bayesdb_generator(bayesdb(), 't1', 't1_cc', t1_schema, lambda x: 0,\
            columns=['label CATEGORICAL', 'age NUMERICAL', 'weight NUMERICAL'])\
            as (bdb, generator_id):
        assert core.bayesdb_generator_fresh_row_id(bdb, generator_id) == 1
        t1_data(bdb)
        assert core.bayesdb_generator_fresh_row_id(bdb, generator_id) == \
            len(t1_rows) + 1
