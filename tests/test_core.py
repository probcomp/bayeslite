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
def bayesdb(metamodel=None, engine=None, **kwargs):
    if metamodel is None:
        metamodel = 'crosscat'
    if engine is None:
        engine = local_crosscat()
    bdb = bayeslite.BayesDB(**kwargs)
    bayeslite.bayesdb_register_metamodel(bdb, metamodel, engine)
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

def test_hackmetamodel():
    bdb = bayeslite.BayesDB()
    bdb.sql_execute('CREATE TABLE t(a INTEGER, b TEXT)')
    bdb.sql_execute("INSERT INTO t (a, b) VALUES (42, 'fnord')")
    bdb.sql_execute('CREATE TABLE u AS SELECT * FROM t')
    with pytest.raises(ValueError):
        bayeslite.bayesdb_import_sqlite_table(bdb, 't')
    with pytest.raises(AssertionError):
        bayeslite.bayesdb_import_sqlite_table(bdb, 't', metamodel='dotdog')
    bayeslite.bayesdb_register_metamodel(bdb, 'dotdog', local_crosscat())
    bayeslite.bayesdb_deregister_metamodel(bdb, 'dotdog')
    bayeslite.bayesdb_register_metamodel(bdb, 'dotdog', local_crosscat())
    with pytest.raises(ValueError):
        bayeslite.bayesdb_import_sqlite_table(bdb, 't')
    with pytest.raises(AssertionError):
        bayeslite.bayesdb_import_sqlite_table(bdb, 't', metamodel='crosscat')
    bayeslite.bayesdb_import_sqlite_table(bdb, 't', metamodel='dotdog')
    with pytest.raises(sqlite3.IntegrityError):
        bayeslite.bayesdb_import_sqlite_table(bdb, 't', metamodel='dotdog')
    bayeslite.bayesdb_set_default_metamodel(bdb, 'dotdog')
    with pytest.raises(AssertionError):
        bayeslite.bayesdb_deregister_metamodel(bdb, 'dotdog')
    with pytest.raises(AssertionError):
        bayeslite.bayesdb_import_sqlite_table(bdb, 'u', metamodel='crosscat')
    bayeslite.bayesdb_set_default_metamodel(bdb, None)
    with pytest.raises(sqlite3.IntegrityError):
        bayeslite.bayesdb_import_sqlite_table(bdb, 't', metamodel='dotdog')
    bayeslite.bayesdb_set_default_metamodel(bdb, 'dotdog')
    bayeslite.bayesdb_import_sqlite_table(bdb, 'u')
    with pytest.raises(sqlite3.IntegrityError):
        bayeslite.bayesdb_import_sqlite_table(bdb, 'u')
    with pytest.raises(sqlite3.IntegrityError):
        bayeslite.bayesdb_import_sqlite_table(bdb, 'u', metamodel='dotdog')

@contextlib.contextmanager
def sqlite_bayesdb_table(mkbdb, name, schema, data, **kwargs):
    with mkbdb as bdb:
        schema(bdb)
        data(bdb)
        bayeslite.bayesdb_import_sqlite_table(bdb, name, **kwargs)
        sql = 'select id from bayesdb_table where name = ?'
        table_id = core.bayesdb_sql_execute1(bdb, sql, (name,))
        assert table_id == 1
        yield bdb, table_id

@contextlib.contextmanager
def analyzed_bayesdb_table(mkbdb, nmodels, nsteps, max_seconds=None):
    with mkbdb as (bdb, table_id):
        bqlfn.bayesdb_models_initialize(bdb, table_id, range(nmodels))
        bqlfn.bayesdb_models_analyze(bdb, table_id, iterations=nsteps,
            max_seconds=max_seconds)
        yield bdb, table_id

def bayesdb_maxrowid(bdb, table_id):
    table_name = bayeslite.bayesdb_table_name(bdb, table_id)
    qt = sqlite3_quote_name(table_name)
    sql = 'select max(rowid) from %s' % (qt,)
    return core.bayesdb_sql_execute1(bdb, sql)

def test_casefold_colname():
    def t(name, sql, *args, **kwargs):
        def schema(bdb):
            bdb.sql_execute(sql)
        def data(_bdb):
            pass
        return sqlite_bayesdb_table(bayesdb(), name, schema, data, *args,
            **kwargs)
    with pytest.raises(sqlite3.OperationalError):
        with t('t', 'create table t(x, X)'):
            pass
    with pytest.raises(ValueError):
        with t('t', 'create table t(x, y)', column_names=['x', 'x']):
            pass
    with pytest.raises(ValueError):
        with t('t', 'create table t(x, y)', column_names=['x', 'X']):
            pass
    with pytest.raises(ValueError):
        with t('t', 'create table t(x, y)',
                column_types={'x': 'categorical', 'X': 'numerical'}):
            pass
    with pytest.raises(ValueError):
        with t('t', 'create table t(x, y)',
                column_names=['x', 'X'],
                column_types={'x': 'categorical', 'X': 'numerical'}):
            pass
    with t('t', 'create table t(x, y)', column_names=['x', 'Y']):
        pass
    with t('t', 'create table t(x, y)', column_names=['x', 'Y'],
            column_types={'X': 'categorical', 'y': 'numerical'}):
        pass
    with t('t', 'CREATE TABLE T(X, Y)', column_names=['x', 'Y'],
            column_types={'X': 'categorical', 'y': 'numerical'}):
        pass

def t0_schema(bdb):
    bdb.sql_execute('create table t0 (id integer primary key, n integer)')
def t0_data(bdb):
    for row in [(0, 0), (1, 1), (42, 42)]:
        bdb.sql_execute('insert into t0 (id, n) values (?, ?)', row)

def t0():
    return sqlite_bayesdb_table(bayesdb(), 't0', t0_schema, t0_data)

def test_t0_badname():
    with pytest.raises(ValueError):
        with sqlite_bayesdb_table(bayesdb(), 't0', t0_schema, t0_data,
                column_names=['n', 'm']):
            pass

def test_t0_badtype():
    with pytest.raises(ValueError):
        with sqlite_bayesdb_table(bayesdb(), 't0', t0_schema, t0_data,
                column_types={'m': 'categorical'}):
            pass

def test_t0_missingtype():
    with pytest.raises(ValueError):
        with sqlite_bayesdb_table(bayesdb(), 't0', t0_schema, t0_data,
                column_types={}):
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
    return sqlite_bayesdb_table(bayesdb(), 't1', t1_schema, t1_data)

def t1_sub():
    return sqlite_bayesdb_table(bayesdb(), 't1', t1_schema, t1_data,
        column_names=['id', 'label', 'age'])

def t1_subcat():
    return sqlite_bayesdb_table(bayesdb(), 't1', t1_schema, t1_data,
        column_names=['id', 'label', 'age'],
        column_types={
            'id': 'categorical',
            'label': 'key',
            'age': 'categorical',
        })

def t1_mp():
    return sqlite_bayesdb_table(bayesdb(engine=multiprocessing_crosscat()),
        't1', t1_schema, t1_data)

def test_t1_missingtype():
    with pytest.raises(ValueError):
        with sqlite_bayesdb_table(bayesdb(), 't1', t1_schema, t1_data,
                column_names=['id', 'label', 'age'],
                column_types={'id': 'key', 'label': 'key'}):
            pass

def test_t1_multikey():
    with pytest.raises(ValueError):
        with sqlite_bayesdb_table(bayesdb(), 't1', t1_schema, t1_data,
                column_names=['id', 'label', 'age'],
                column_types={
                    'id': 'key',
                    'label': 'key',
                    'age': 'categorical',
                }):
            pass

def test_t1_nokey():
    with sqlite_bayesdb_table(bayesdb(), 't1', t1_schema, t1_data,
            column_names=['age', 'weight']):
        pass

def test_t1_nocase():
    with sqlite_bayesdb_table(bayesdb(), 't1', t1_schema, t1_data) \
            as (bdb, table_id):
        bdb.execute('select id from t1')
        bdb.execute('select ID from T1')
        bdb.execute('select iD from T1')
        bdb.execute('select Id from T1')

btable_generators = {
    't0': t0,
    't1': t1,
    't1_sub': t1_sub,
    't1_subcat': t1_subcat,
}

@pytest.mark.parametrize('btable_name', btable_generators.keys())
def test_btable(btable_name):
    with btable_generators[btable_name]():
        pass

@pytest.mark.parametrize('btable_name', btable_generators.keys())
def test_btable_analysis0(btable_name):
    with analyzed_bayesdb_table(btable_generators[btable_name](), 1, 0):
        pass

@pytest.mark.parametrize('btable_name', btable_generators.keys())
def test_btable_analysis1(btable_name):
    if btable_name == 't0':
        pytest.xfail("Crosscat can't handle a table with only one column.")
    with analyzed_bayesdb_table(btable_generators[btable_name](), 1, 1):
        pass

# The multiprocessing engine has a large overhead, too much to try
# every normal test with it, so we'll just run this one test to make
# sure it doesn't crash and burn with ten models.
def test_t1_mp_analysis():
    with analyzed_bayesdb_table(t1_mp(), 10, 2):
        pass

def test_t1_mp_analysis_time_deadline():
    with analyzed_bayesdb_table(t1_mp(), 10, None, max_seconds=1):
        pass

def test_t1_mp_analysis_iter_deadline():
    with analyzed_bayesdb_table(t1_mp(), 10, 1, max_seconds=10):
        pass

def test_t1_analysis_time_deadline():
    with analyzed_bayesdb_table(t1(), 10, None, max_seconds=1):
        pass

def test_t1_analysis_iter_deadline():
    with analyzed_bayesdb_table(t1(), 10, 1, max_seconds=10):
        pass

def test_btable_mp_analysis_iter_deadline():
    with analyzed_bayesdb_table(t1_mp(), 10, 1, max_seconds=10):
        pass

@pytest.mark.parametrize('rowid,colno,confidence',
    [(i+1, j, conf)
        for i in range(min(5, len(t1_rows)))
        for j in range(2)
        for conf in [0.01, 0.5, 0.99]])
def test_t1_infer(rowid, colno, confidence):
    with analyzed_bayesdb_table(t1(), 1, 1) as (bdb, table_id):
        if rowid == 0: rowid = bayesdb_maxrowid(bdb, table_id)
        bqlfn.bql_infer(bdb, table_id, colno, rowid, None, confidence,
            numsamples=1)

@pytest.mark.parametrize('colnos,constraints,numpredictions',
    [(colnos, constraints, numpred)
        for colnos in powerset(range(2))
        for constraints in [None] + list(powerset(range(2)))
        for numpred in range(3)])
def test_t1_simulate(colnos, constraints, numpredictions):
    if len(colnos) == 0:
        pytest.xfail("Crosscat can't simulate zero columns.")
    with analyzed_bayesdb_table(t1(), 1, 1) as (bdb, table_id):
        if constraints is not None:
            rowid = 1           # XXX Avoid hard-coding this.
            # Can't use t1_rows[0][i] because not all t1-based tables
            # use the same column indexing -- some use a subset of the
            # columns.
            #
            # XXX Automatically test the correct exception.
            constraints = \
                [(i, core.bayesdb_cell_value(bdb, table_id, rowid, i))
                    for i in constraints]
        bayeslite.bayesdb_simulate(bdb, table_id, constraints, colnos,
            numpredictions=numpredictions)

@pytest.mark.parametrize('btable_name,colno',
    [(btable_name, colno)
        for btable_name in btable_generators.keys()
        for colno in range(2)])
def test_onecolumn(btable_name, colno):
    if btable_name == 't0':
        # XXX Also too few columns for this test.
        pytest.xfail("Crosscat can't handle a table with only one column.")
    with analyzed_bayesdb_table(btable_generators[btable_name](), 1, 1) \
            as (bdb, table_id):
        bqlfn.bql_column_typicality(bdb, table_id, colno)
        bdb.sql_execute('select bql_column_typicality(?, ?)',
            (table_id, colno))

@pytest.mark.parametrize('btable_name,colno0,colno1',
    [(btable_name, colno0, colno1)
        for btable_name in btable_generators.keys()
        for colno0 in range(2)
        for colno1 in range(2)])
def test_twocolumn(btable_name, colno0, colno1):
    if btable_name == 't0':
        pytest.xfail("Crosscat can't handle a table with only one column.")
    if btable_name == 't0':
        pytest.skip('Not enough columns in t0.')
    with analyzed_bayesdb_table(btable_generators[btable_name](), 1, 1) \
            as (bdb, table_id):
        bqlfn.bql_column_correlation(bdb, table_id, colno0, colno1)
        bdb.sql_execute('select bql_column_correlation(?, ?, ?)',
            (table_id, colno0, colno1))
        bqlfn.bql_column_dependence_probability(bdb, table_id, colno0, colno1)
        bdb.sql_execute('select bql_column_dependence_probability(?, ?, ?)',
            (table_id, colno0, colno1))
        bqlfn.bql_column_mutual_information(bdb, table_id, colno0, colno1)
        bqlfn.bql_column_mutual_information(bdb, table_id, colno0, colno1, None)
        bqlfn.bql_column_mutual_information(bdb, table_id, colno0, colno1, 1)
        bdb.sql_execute('select bql_column_mutual_information(?, ?, ?, NULL)',
            (table_id, colno0, colno1))
        bdb.sql_execute('select bql_column_mutual_information(?, ?, ?, 1)',
            (table_id, colno0, colno1))
        bdb.sql_execute('select bql_column_mutual_information(?, ?, ?, 100)',
            (table_id, colno0, colno1))

@pytest.mark.parametrize('colno,rowid',
    [(colno, rowid)
        for colno in range(2)
        for rowid in range(6)])
def test_t1_column_value_probability(colno, rowid):
    with analyzed_bayesdb_table(t1(), 1, 1) as (bdb, table_id):
        if rowid == 0: rowid = bayesdb_maxrowid(bdb, table_id)
        value = core.bayesdb_cell_value(bdb, table_id, rowid, colno)
        bqlfn.bql_column_value_probability(bdb, table_id, colno, value)
        tn = bayeslite.bayesdb_table_name(bdb, table_id)
        cn = bayeslite.bayesdb_column_name(bdb, table_id, colno)
        qt = sqlite3_quote_name(tn)
        qc = sqlite3_quote_name(cn)
        sql = '''
            select bql_column_value_probability(?, ?,
                (select %s from %s where rowid = ?))
        ''' % (qc, qt)
        bdb.sql_execute(sql, (table_id, colno, rowid))

@pytest.mark.parametrize('btable_name,source,target,colnos',
    [(btable_name, source, target, list(colnos))
        for btable_name in btable_generators.keys()
        for source in range(1,3)
        for target in range(2,4)
        for colnos in powerset(range(2))])
def test_row_similarity(btable_name, source, target, colnos):
    if btable_name == 't0':
        pytest.xfail("Crosscat can't handle a table with only one column.")
    if btable_name == 't0' and colnos != [] and colnos != [0]:
        pytest.skip('Not enough columns in t0.')
    with analyzed_bayesdb_table(btable_generators[btable_name](), 1, 1) \
            as (bdb, table_id):
        bqlfn.bql_row_similarity(bdb, table_id, source, target, *colnos)
        sql = 'select bql_row_similarity(?, ?, ?%s%s)' % \
            ('' if 0 == len(colnos) else ', ', ', '.join(map(str, colnos)))
        bdb.sql_execute(sql, (table_id, source, target))

@pytest.mark.parametrize('btable_name,rowid',
    [(btable_name, rowid)
        for btable_name in btable_generators.keys()
        for rowid in range(4)])
def test_row_typicality(btable_name, rowid):
    if btable_name == 't0':
        pytest.xfail("Crosscat can't handle a table with only one column.")
    if btable_name == 't0' and colnos != [] and colnos != [0]:
        pytest.skip('Not enough columns in t0.')
    with analyzed_bayesdb_table(btable_generators[btable_name](), 1, 1) \
            as (bdb, table_id):
        if rowid == 0: rowid = bayesdb_maxrowid(bdb, table_id)
        bqlfn.bql_row_typicality(bdb, table_id, rowid)
        bdb.sql_execute('select bql_row_typicality(?, ?)', (table_id, rowid))

@pytest.mark.parametrize('btable_name,rowid,colno',
    [(btable_name, rowid, colno)
        for btable_name in btable_generators.keys()
        for rowid in range(4)
        for colno in range(2)])
def test_row_column_predictive_probability(btable_name, rowid, colno):
    if btable_name == 't0':
        pytest.xfail("Crosscat can't handle a table with only one column.")
    if btable_name == 't0' and colnos != [] and colnos != [0]:
        pytest.skip('Not enough columns in t0.')
    with analyzed_bayesdb_table(btable_generators[btable_name](), 1, 1) \
            as (bdb, table_id):
        if rowid == 0: rowid = bayesdb_maxrowid(bdb, table_id)
        bqlfn.bql_row_column_predictive_probability(bdb, table_id, rowid, colno)
        sql = 'select bql_row_column_predictive_probability(?, ?, ?)'
        bdb.sql_execute(sql, (table_id, rowid, colno))

def test_insert():
    with test_csv.bayesdb_csv_stream(test_csv.csv_data) as (bdb, f):
        bayeslite.bayesdb_import_csv(bdb, 't', f)
        bdb.execute('initialize 2 models for t')
        bdb.execute('analyze t for 1 iteration wait')
        table_id = core.bayesdb_table_id(bdb, 't')
        row = (41, 'F', 96000, 73, 'data science', 2)
        bqlfn.bayesdb_insert(bdb, table_id, row)
