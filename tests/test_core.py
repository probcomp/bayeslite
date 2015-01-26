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

import crosscat.CrossCatClient

import bayeslite

def powerset(s):
    s = list(s)
    combinations = (itertools.combinations(s, r) for r in range(len(s) + 1))
    return itertools.chain.from_iterable(combinations)

def local_crosscat():
    return crosscat.CrossCatClient.get_CrossCatClient('local', seed=0)

@contextlib.contextmanager
def bayesdb(engine=None, **kwargs):
    if engine is None:
        engine = local_crosscat()
    bdb = bayeslite.BayesDB(engine, **kwargs)
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
            db.execute('PRAGMA user_version = 1')
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

@contextlib.contextmanager
def sqlite_bayesdb_table(mkbdb, name, schema, data, **kwargs):
    with mkbdb as bdb:
        schema(bdb)
        data(bdb)
        bayeslite.bayesdb_import_sqlite_table(bdb, name, **kwargs)
        sql = 'select id from bayesdb_table where name = ?'
        table_id = bayeslite.sqlite3_exec_1(bdb.sqlite, sql, (name,))
        assert table_id == 1
        yield bdb, table_id

@contextlib.contextmanager
def analyzed_bayesdb_table(mkbdb, nmodels, nsteps):
    with mkbdb as (bdb, table_id):
        bayeslite.bayesdb_models_initialize(bdb, table_id, nmodels)
        for modelno in range(nmodels):
            bayeslite.bayesdb_models_analyze1(bdb, table_id, modelno, nsteps)
        yield bdb, table_id

def bayesdb_maxrowid(bdb, table_id):
    table_name = bayeslite.bayesdb_table_name(bdb, table_id)
    qt = bayeslite.sqlite3_quote_name(table_name)
    sql = 'select max(rowid) from %s' % (qt,)
    return bayeslite.sqlite3_exec_1(bdb.sqlite, sql)

def t0_schema(bdb):
    bdb.sqlite.execute('create table t0 (id integer primary key)')
def t0_data(bdb):
    bdb.sqlite.executemany('insert into t0 (id) values (?)',
        [(0,), (1,), (42,)])

def t0():
    return sqlite_bayesdb_table(bayesdb(), 't0', t0_schema, t0_data)

def test_t0_badname():
    with pytest.raises(ValueError):
        with sqlite_bayesdb_table(bayesdb(), 't0', t0_schema, t0_data,
                column_names=['id', 'nid']):
            pass

def test_t0_badtype():
    with pytest.raises(ValueError):
        with sqlite_bayesdb_table(bayesdb(), 't0', t0_schema, t0_data,
                column_types={'nid': 'categorical'}):
            pass

def test_t0_missingtype():
    with pytest.raises(ValueError):
        with sqlite_bayesdb_table(bayesdb(), 't0', t0_schema, t0_data,
                column_types={}):
            pass

def t1_schema(bdb):
    bdb.sqlite.execute('''
        create table t1 (
            id integer primary key,
            label text,
            age double,
            weight double
        )
    ''')
def t1_data(bdb):
    bdb.sqlite.executemany('insert into t1 (label,age,weight) values (?,?,?)',
        t1_rows)

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

@pytest.mark.parametrize('rowid,colno,confidence',
    [(i+1, j, conf)
        for i in range(min(5, len(t1_rows)))
        for j in range(3)
        for conf in [0.01, 0.5, 0.99]])
def test_t1_infer(rowid, colno, confidence):
    with analyzed_bayesdb_table(t1(), 1, 1) as (bdb, table_id):
        if rowid == 0: rowid = bayesdb_maxrowid(bdb, table_id)
        bayeslite.bql_infer(bdb, table_id, colno, rowid, None, confidence,
            numsamples=1)

@pytest.mark.parametrize('colnos,constraints,numpredictions',
    [(colnos, constraints, numpred)
        for colnos in powerset(range(3))
        for constraints in [None] + list(powerset(range(3)))
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
                [(i, bayeslite.bayesdb_cell_value(bdb, table_id, rowid, i))
                    for i in constraints]
        bayeslite.bayesdb_simulate(bdb, table_id, constraints, colnos,
            numpredictions=numpredictions)

@pytest.mark.parametrize('btable_name,colno',
    [(btable_name, colno)
        for btable_name in btable_generators.keys()
        for colno in range(3)])
def test_onecolumn(btable_name, colno):
    if btable_name == 't0':
        # XXX Also too few columns for this test.
        pytest.xfail("Crosscat can't handle a table with only one column.")
    with analyzed_bayesdb_table(btable_generators[btable_name](), 1, 1) \
            as (bdb, table_id):
        bayeslite.bql_column_typicality(bdb, table_id, colno)
        bdb.sqlite.execute('select bql_column_typicality(?, ?)',
            (table_id, colno))

@pytest.mark.parametrize('btable_name,colno0,colno1',
    [(btable_name, colno0, colno1)
        for btable_name in btable_generators.keys()
        for colno0 in range(3)
        for colno1 in range(3)])
def test_twocolumn(btable_name, colno0, colno1):
    if btable_name == 't0':
        pytest.xfail("Crosscat can't handle a table with only one column.")
    if btable_name == 't0':
        pytest.skip('Not enough columns in t0.')
    with analyzed_bayesdb_table(btable_generators[btable_name](), 1, 1) \
            as (bdb, table_id):
        bayeslite.bql_column_correlation(bdb, table_id, colno0, colno1)
        bayeslite.sqlite3_exec_1(bdb.sqlite,
            'select bql_column_correlation(?, ?, ?)',
            (table_id, colno0, colno1))
        bayeslite.bql_column_dependence_probability(bdb, table_id, colno0,
            colno1)
        bayeslite.sqlite3_exec_1(bdb.sqlite,
            'select bql_column_dependence_probability(?, ?, ?)',
            (table_id, colno0, colno1))
        bayeslite.bql_column_mutual_information(bdb, table_id, colno0,
            colno1)
        bayeslite.sqlite3_exec_1(bdb.sqlite,
            'select bql_column_mutual_information(?, ?, ?)',
            (table_id, colno0, colno1))

@pytest.mark.parametrize('colno,rowid',
    [(colno, rowid)
        for colno in range(3)
        for rowid in range(6)])
def test_t1_column_value_probability(colno, rowid):
    with analyzed_bayesdb_table(t1(), 1, 1) as (bdb, table_id):
        if rowid == 0: rowid = bayesdb_maxrowid(bdb, table_id)
        value = bayeslite.bayesdb_cell_value(bdb, table_id, rowid, colno)
        bayeslite.bql_column_value_probability(bdb, table_id, colno, value)
        tn = bayeslite.bayesdb_table_name(bdb, table_id)
        cn = bayeslite.bayesdb_column_name(bdb, table_id, colno)
        qt = bayeslite.sqlite3_quote_name(tn)
        qc = bayeslite.sqlite3_quote_name(cn)
        sql = '''
            select bql_column_value_probability(?, ?,
                (select %s from %s where rowid = ?))
        ''' % (qc, qt)
        bayeslite.sqlite3_exec_1(bdb.sqlite, sql, (table_id, colno, rowid))

@pytest.mark.parametrize('btable_name,source,target,colnos',
    [(btable_name, source, target, list(colnos))
        for btable_name in btable_generators.keys()
        for source in range(1,3)
        for target in range(2,4)
        for colnos in powerset(range(3))])
def test_row_similarity(btable_name, source, target, colnos):
    if btable_name == 't0':
        pytest.xfail("Crosscat can't handle a table with only one column.")
    if btable_name == 't0' and colnos != [] and colnos != [0]:
        pytest.skip('Not enough columns in t0.')
    with analyzed_bayesdb_table(btable_generators[btable_name](), 1, 1) \
            as (bdb, table_id):
        bayeslite.bql_row_similarity(bdb, table_id, source, target, *colnos)
        sql = 'select bql_row_similarity(?, ?, ?%s%s)' % \
            ('' if 0 == len(colnos) else ', ', ', '.join(map(str, colnos)))
        bayeslite.sqlite3_exec_1(bdb.sqlite, sql, (table_id, source, target))

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
        bayeslite.bql_row_typicality(bdb, table_id, rowid)
        bayeslite.sqlite3_exec_1(bdb.sqlite, 'select bql_row_typicality(?, ?)',
            (table_id, rowid))

@pytest.mark.parametrize('btable_name,rowid,colno',
    [(btable_name, rowid, colno)
        for btable_name in btable_generators.keys()
        for rowid in range(4)
        for colno in range(3)])
def test_row_column_predictive_probability(btable_name, rowid, colno):
    if btable_name == 't0':
        pytest.xfail("Crosscat can't handle a table with only one column.")
    if btable_name == 't0' and colnos != [] and colnos != [0]:
        pytest.skip('Not enough columns in t0.')
    with analyzed_bayesdb_table(btable_generators[btable_name](), 1, 1) \
            as (bdb, table_id):
        if rowid == 0: rowid = bayesdb_maxrowid(bdb, table_id)
        bayeslite.bql_row_column_predictive_probability(bdb, table_id,
            rowid, colno)
        bayeslite.sqlite3_exec_1(bdb.sqlite,
            'select bql_row_column_predictive_probability(?, ?, ?)',
            (table_id, rowid, colno))
