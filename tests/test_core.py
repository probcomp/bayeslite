# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2014, MIT Probabilistic Computing Project
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
import sqlite3
import tempfile

import crosscat.LocalEngine
import crosscat.MultiprocessingEngine

import bayeslite
import bayeslite.bqlfn as bqlfn
import bayeslite.core as core
import bayeslite.crosscat
import bayeslite.guess as guess
import bayeslite.metamodel as metamodel

from bayeslite.sqlite3_util import sqlite3_exec_1
from bayeslite.sqlite3_util import sqlite3_quote_name

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
        metamodel = bayeslite.crosscat.CrosscatMetamodel(crosscat)
    bdb = bayeslite.BayesDB(**kwargs)
    bayeslite.bayesdb_register_metamodel(bdb, metamodel)
    bayeslite.bayesdb_set_default_metamodel(bdb, metamodel)
    try:
        yield bdb
    finally:
        bdb.close()

def test_openclose():
    with bayesdb():
        pass

def test_bad_db_application_id():
    with tempfile.NamedTemporaryFile(prefix='bayeslite') as f:
        with sqlite3.connect(f.name, isolation_level=None) as db:
            db.execute('PRAGMA application_id = 42')
            db.execute('PRAGMA user_version = 3')
        with pytest.raises(IOError):
            with bayesdb(pathname=f.name):
                pass

def test_bad_db_user_version():
    # XXX Would be nice to avoid a named temporary file here.  Pass
    # the sqlite3 database connection in?
    with tempfile.NamedTemporaryFile(prefix='bayeslite') as f:
        with sqlite3.connect(f.name, isolation_level=None) as db:
            db.execute('PRAGMA application_id = 1113146434')
            db.execute('PRAGMA user_version = 42')
        with pytest.raises(IOError):
            with bayesdb(pathname=f.name):
                pass

class DotdogMetamodel(metamodel.IMetamodel):
    def name(self):
        return 'dotdog'
    def register(self, bdb):
        sql = '''
            INSERT OR IGNORE INTO bayesdb_metamodel
                VALUES ('dotdog', 42)
        '''
        bdb.sql_execute(sql)
    def create_generator(self, bdb, schema, instantiate):
        instantiate(schema)

def test_hackmetamodel():
    bdb = bayeslite.BayesDB()
    bdb.sql_execute('CREATE TABLE t(a INTEGER, b TEXT)')
    bdb.sql_execute("INSERT INTO t (a, b) VALUES (42, 'fnord')")
    bdb.sql_execute('CREATE TABLE u AS SELECT * FROM t')
    with pytest.raises(ValueError):
        bdb.execute('CREATE GENERATOR t_cc FOR t USING crosscat(a NUMERICAL)')
    with pytest.raises(ValueError):
        bdb.execute('CREATE GENERATOR t_dd FOR t USING dotdog(a NUMERICAL)')
    crosscat = local_crosscat()
    crosscat_metamodel = bayeslite.crosscat.CrosscatMetamodel(crosscat)
    dotdog_metamodel = DotdogMetamodel()
    bayeslite.bayesdb_register_metamodel(bdb, dotdog_metamodel)
    bayeslite.bayesdb_deregister_metamodel(bdb, dotdog_metamodel)
    bayeslite.bayesdb_register_metamodel(bdb, dotdog_metamodel)
    with pytest.raises(ValueError):
        bdb.execute('CREATE GENERATOR t_cc FOR t USING crosscat(a NUMERICAL)')
    bdb.execute('CREATE GENERATOR t_dd FOR t USING dotdog(a NUMERICAL)')
    with pytest.raises(sqlite3.IntegrityError):
        bdb.execute('CREATE GENERATOR t_dd FOR t USING dotdog(a NUMERICAL)')
    bayeslite.bayesdb_set_default_metamodel(bdb, dotdog_metamodel)
    with pytest.raises(AssertionError):
        bayeslite.bayesdb_deregister_metamodel(bdb, dotdog_metamodel)
    with pytest.raises(ValueError):
        bdb.execute('CREATE GENERATOR t_cc FOR t USING crosscat(a NUMERICAL)')
    bayeslite.bayesdb_set_default_metamodel(bdb, None)
    with pytest.raises(sqlite3.IntegrityError):
        bdb.execute('CREATE GENERATOR t_dd FOR t USING dotdog(a NUMERICAL)')
    # XXX Rest of test originally exercised default metamodel, but
    # syntax doesn't support that now.  Not clear that's wrong either.
    bayeslite.bayesdb_set_default_metamodel(bdb, dotdog_metamodel)
    bdb.execute('CREATE GENERATOR u_dd FOR u USING dotdog(a NUMERICAL)')
    with pytest.raises(sqlite3.IntegrityError):
        bdb.execute('CREATE GENERATOR u_dd FOR u USING dotdog(a NUMERICAL)')

@contextlib.contextmanager
def bayesdb_generator(mkbdb, tab, gen, table_schema, data, columns,
        metamodel_name='crosscat'):
    with mkbdb as bdb:
        table_schema(bdb)
        data(bdb)
        qt = sqlite3_quote_name(tab)
        qg = sqlite3_quote_name(gen)
        qmm = sqlite3_quote_name(metamodel_name)
        bdb.execute('CREATE GENERATOR %s FOR %s USING %s(%s)' %
            (qg, qt, qmm, ','.join(columns)))
        sql = 'SELECT id FROM bayesdb_generator WHERE name = ?'
        cursor = bdb.sql_execute(sql, (gen,))
        try:
            row = cursor.next()
        except StopIteration:
            assert False, 'Generator didn\'t make it!'
        else:
            assert len(row) == 1
            assert isinstance(row[0], int)
            generator_id = row[0]
            yield bdb, generator_id

@contextlib.contextmanager
def analyzed_bayesdb_generator(mkbdb, nmodels, nsteps, max_seconds=None):
    with mkbdb as (bdb, generator_id):
        generator = core.bayesdb_generator_name(bdb, generator_id)
        qg = sqlite3_quote_name(generator)
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
    qt = sqlite3_quote_name(table_name)
    sql = 'SELECT MAX(_rowid_) FROM %s' % (qt,)
    return bdb.sql_execute(sql).next()[0]

def test_casefold_colname():
    def t(tname, gname, sql, *args, **kwargs):
        def schema(bdb):
            bdb.sql_execute(sql)
        def data(_bdb):
            pass
        return bayesdb_generator(bayesdb(), tname, gname, schema, data, *args,
            **kwargs)
    with pytest.raises(sqlite3.OperationalError):
        with t('t', 't_cc', 'create table t(x, X)', []):
            pass
    with pytest.raises(ValueError):
        columns = ['x CATEGORICAL', 'x CATEGORICAL']
        with t('t', 't_cc', 'create table t(x, y)', columns):
            pass
    with pytest.raises(ValueError):
        columns = ['x CATEGORICAL', 'X CATEGORICAL']
        with t('t', 't_cc', 'create table t(x, y)', columns):
            pass
    with pytest.raises(ValueError):
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
    for row in [(0, 0), (1, 1), (42, 42)]:
        bdb.sql_execute('insert into t0 (id, n) values (?, ?)', row)

def t0():
    return bayesdb_generator(bayesdb(), 't0', 't0_cc', t0_schema, t0_data,
        columns=['n NUMERICAL'])

def test_t0_badname():
    with pytest.raises(ValueError):
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

def t1():
    return bayesdb_generator(bayesdb(), 't1', 't1_cc', t1_schema, t1_data,
        columns=['label CATEGORICAL', 'age NUMERICAL', 'weight NUMERICAL'])

def t1_sub():
    return bayesdb_generator(bayesdb(), 't1', 't1_sub_cc',
        t1_schema, t1_data,
        columns=['label CATEGORICAL', 'weight NUMERICAL'])

def t1_subcat():
    return bayesdb_generator(bayesdb(), 't1', 't1_subcat_cc',
        t1_schema, t1_data,
        columns=['label CATEGORICAL', 'weight CATEGORICAL'])

# def t1_mp():
#     crosscat = multiprocessing_crosscat()
#     metamodel = bayeslite.crosscat.CrosscatMetamodel(crosscat)
#     return bayesdb_generator(bayesdb(metamodel=metamodel),
#         't1', 't1_cc', t1_schema, t1_data,
#         columns=['label CATEGORICAL', 'age NUMERICAL', 'weight NUMERICAL'])

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
        bdb.execute('select id from t1')
        bdb.execute('select ID from T1')
        bdb.execute('select iD from T1')
        bdb.execute('select Id from T1')
        # bdb.execute('select id from t1_cc')
        # bdb.execute('select ID from T1_cC')
        # bdb.execute('select iD from T1_Cc')
        # bdb.execute('select Id from T1_CC')

examples = {
    't0': t0,
    't1': t1,
    't1_sub': t1_sub,
    't1_subcat': t1_subcat,
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
    if exname == 't0':
        pytest.xfail("Crosscat can't handle a table with only one column.")
    with analyzed_bayesdb_generator(examples[exname](), 1, 1):
        pass

# The multiprocessing engine has a large overhead, too much to try
# every normal test with it, so we'll just run this one test to make
# sure it doesn't crash and burn with ten models.
# def test_t1_mp_analysis():
#     with analyzed_bayesdb_generator(t1_mp(), 10, 2):
#         pass

# def test_t1_mp_analysis_time_deadline():
#     with analyzed_bayesdb_generator(t1_mp(), 10, None, max_seconds=1):
#         pass

# def test_t1_mp_analysis_iter_deadline():
#     with analyzed_bayesdb_generator(t1_mp(), 10, 1, max_seconds=10):
#         pass

def test_t1_analysis_time_deadline():
    with analyzed_bayesdb_generator(t1(), 10, None, max_seconds=1):
        pass

def test_t1_analysis_iter_deadline():
    pytest.skip('XXX TAKES TOO LONG FOR NOW')
    with analyzed_bayesdb_generator(t1(), 10, 1, max_seconds=10):
        pass

# def test_t1_mp_analysis_iter_deadline():
#     with analyzed_bayesdb_generator(t1_mp(), 10, 1, max_seconds=10):
#         pass

@pytest.mark.parametrize('rowid,colno,confidence',
    [(i+1, j, conf)
        for i in range(min(5, len(t1_rows)))
        for j in range(1,3)
        for conf in [0.01, 0.5, 0.99]])
def test_t1_infer(rowid, colno, confidence):
    with analyzed_bayesdb_generator(t1(), 1, 1) as (bdb, generator_id):
        if rowid == 0: rowid = bayesdb_maxrowid(bdb, generator_id)
        bqlfn.bql_infer(bdb, generator_id, colno, rowid, confidence)

@pytest.mark.parametrize('colnos,constraints,numpredictions',
    [(colnos, constraints, numpred)
        for colnos in powerset(range(1,3))
        for constraints in [None] + list(powerset(range(1,3)))
        for numpred in range(3)])
def test_t1_simulate(colnos, constraints, numpredictions):
    if len(colnos) == 0:
        pytest.xfail("Crosscat can't simulate zero columns.")
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
        bayeslite.bayesdb_simulate(bdb, generator_id, constraints, colnos,
            numpredictions=numpredictions)

@pytest.mark.parametrize('exname,colno',
    [(exname, colno)
        for exname in examples.keys()
        for colno in range(1,3)])
def test_onecolumn(exname, colno):
    if exname == 't0':
        # XXX Also too few columns for this test.
        pytest.xfail("Crosscat can't handle a table with only one column.")
    if exname.startswith('t1_sub') and colno > 1:
        pytest.skip('Not enough columns in %s.' % (exname,))
    with analyzed_bayesdb_generator(examples[exname](), 1, 1) \
            as (bdb, generator_id):
        bqlfn.bql_column_typicality(bdb, generator_id, colno)
        bdb.sql_execute('select bql_column_typicality(?, ?)',
            (generator_id, colno))

@pytest.mark.parametrize('exname,colno0,colno1',
    [(exname, colno0, colno1)
        for exname in examples.keys()
        for colno0 in range(1,3)
        for colno1 in range(1,3)])
def test_twocolumn(exname, colno0, colno1):
    if exname == 't0':
        pytest.xfail("Crosscat can't handle a table with only one column.")
    if exname == 't0':
        pytest.skip('Not enough columns in t0.')
    if exname.startswith('t1_sub') and (colno0 > 1 or colno1 > 1):
        pytest.skip('Not enough columns in %s.' % (exname,))
    with analyzed_bayesdb_generator(examples[exname](), 1, 1) \
            as (bdb, generator_id):
        bqlfn.bql_column_correlation(bdb, generator_id, colno0, colno1)
        bdb.sql_execute('select bql_column_correlation(?, ?, ?)',
            (generator_id, colno0, colno1))
        bqlfn.bql_column_dependence_probability(bdb, generator_id, colno0,
            colno1)
        bdb.sql_execute('select bql_column_dependence_probability(?, ?, ?)',
            (generator_id, colno0, colno1))
        bqlfn.bql_column_mutual_information(bdb, generator_id, colno0, colno1)
        bqlfn.bql_column_mutual_information(bdb, generator_id, colno0, colno1,
            numsamples=None)
        bqlfn.bql_column_mutual_information(bdb, generator_id, colno0, colno1,
            numsamples=1)
        bdb.sql_execute('select bql_column_mutual_information(?, ?, ?, NULL)',
            (generator_id, colno0, colno1))
        bdb.sql_execute('select bql_column_mutual_information(?, ?, ?, 1)',
            (generator_id, colno0, colno1))
        bdb.sql_execute('select bql_column_mutual_information(?, ?, ?, 100)',
            (generator_id, colno0, colno1))

@pytest.mark.parametrize('colno,rowid',
    [(colno, rowid)
        for colno in range(1,4)
        for rowid in range(6)])
def test_t1_column_value_probability(colno, rowid):
    with analyzed_bayesdb_generator(t1(), 1, 1) as (bdb, generator_id):
        if rowid == 0: rowid = bayesdb_maxrowid(bdb, generator_id)
        value = bayesdb_generator_cell_value(bdb, generator_id, colno, rowid)
        bqlfn.bql_column_value_probability(bdb, generator_id, colno, value)
        table_name = core.bayesdb_generator_table(bdb, generator_id)
        colname = core.bayesdb_generator_column_name(bdb, generator_id, colno)
        qt = sqlite3_quote_name(table_name)
        qc = sqlite3_quote_name(colname)
        sql = '''
            select bql_column_value_probability(?, ?,
                (select %s from %s where rowid = ?))
        ''' % (qc, qt)
        bdb.sql_execute(sql, (generator_id, colno, rowid))

@pytest.mark.parametrize('exname,source,target,colnos',
    [(exname, source, target, list(colnos))
        for exname in examples.keys()
        for source in range(1,3)
        for target in range(2,4)
        for colnos in powerset(range(1,3))])
def test_row_similarity(exname, source, target, colnos):
    if exname == 't0':
        pytest.xfail("Crosscat can't handle a table with only one column.")
    if exname == 't0' and colnos != [] and colnos != [0]:
        pytest.skip('Not enough columns in t0.')
    if exname.startswith('t1_sub') and any(colno > 1 for colno in colnos):
        pytest.skip('Not enough columns in %s.' % (exname,))
    with analyzed_bayesdb_generator(examples[exname](), 1, 1) \
            as (bdb, generator_id):
        bqlfn.bql_row_similarity(bdb, generator_id, source, target, *colnos)
        sql = 'select bql_row_similarity(?, ?, ?%s%s)' % \
            ('' if 0 == len(colnos) else ', ', ', '.join(map(str, colnos)))
        bdb.sql_execute(sql, (generator_id, source, target))

@pytest.mark.parametrize('exname,rowid',
    [(exname, rowid)
        for exname in examples.keys()
        for rowid in range(4)])
def test_row_typicality(exname, rowid):
    if exname == 't0':
        pytest.xfail("Crosscat can't handle a table with only one column.")
    if exname == 't0' and colnos != [] and colnos != [0]:
        pytest.skip('Not enough columns in t0.')
    with analyzed_bayesdb_generator(examples[exname](), 1, 1) \
            as (bdb, generator_id):
        if rowid == 0: rowid = bayesdb_maxrowid(bdb, generator_id)
        bqlfn.bql_row_typicality(bdb, generator_id, rowid)
        bdb.sql_execute('select bql_row_typicality(?, ?)',
            (generator_id, rowid))

@pytest.mark.parametrize('exname,rowid,colno',
    [(exname, rowid, colno)
        for exname in examples.keys()
        for rowid in range(4)
        for colno in range(1,3)])
def test_row_column_predictive_probability(exname, rowid, colno):
    if exname == 't0':
        pytest.xfail("Crosscat can't handle a table with only one column.")
    if exname == 't0' and colnos != [] and colnos != [0]:
        pytest.skip('Not enough columns in t0.')
    if exname.startswith('t1_sub') and any(colno > 1 for colno in colnos):
        pytest.skip('Not enough columns in %s.' % (exname,))
    with analyzed_bayesdb_generator(examples[exname](), 1, 1) \
            as (bdb, generator_id):
        if rowid == 0: rowid = bayesdb_maxrowid(bdb, generator_id)
        bqlfn.bql_row_column_predictive_probability(bdb, generator_id, rowid,
            colno)
        sql = 'select bql_row_column_predictive_probability(?, ?, ?)'
        bdb.sql_execute(sql, (generator_id, rowid, colno))

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
    qt = sqlite3_quote_name(table_name)
    colname = core.bayesdb_generator_column_name(bdb, generator_id, colno)
    qcn = sqlite3_quote_name(colname)
    sql = 'SELECT %s FROM %s WHERE _rowid_ = ?' % (qcn, qt)
    cursor = bdb.sql_execute(sql, (rowid,))
    try:
        row = cursor.next()
    except StopIteration:
        assert False, 'Missing row at %d!' % (rowid,)
    else:
        return row[0]
