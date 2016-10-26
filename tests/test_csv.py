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

import StringIO
import apsw
import contextlib
import pytest
import tempfile

import bayeslite
import bayeslite.guess

from bayeslite import bql_quote_name

import test_core

@contextlib.contextmanager
def bayesdb_csv_stream(csv):
    with test_core.bayesdb() as bdb:
        yield (bdb, StringIO.StringIO(csv))

@contextlib.contextmanager
def bayesdb_csv_file(csv):
    with test_core.bayesdb() as bdb:
        with tempfile.NamedTemporaryFile(prefix='bayeslite') as f:
            with open(f.name, 'w') as out:
                out.write(csv)
            yield (bdb, f.name)

def test_csv_import_empty():
    with bayesdb_csv_stream('') as (bdb, f):
        with pytest.raises(IOError):
            bayeslite.bayesdb_read_csv(bdb, 'empty', f, header=True,
                create=True)

def test_csv_import_nocols():
    with bayesdb_csv_stream('\n') as (bdb, f):
        # CSV import rejects no columns.
        with pytest.raises(IOError):
            bayeslite.bayesdb_read_csv(bdb, 'nocols', f, header=True,
                create=True)

def test_csv_import_onecol_key():
    with bayesdb_csv_stream('foo\n0\none\n2\n') as (bdb, f):
        # foo will be a key column, hence no columns to model.
        bayeslite.bayesdb_read_csv(bdb, 'onecol_key', f, header=True,
            create=True)
        with pytest.raises(ValueError):
            bayeslite.guess.bayesdb_guess_population(bdb, 'p_onecol_key',
                'onecol_key')

def test_csv_import_onecol():
    with bayesdb_csv_stream('foo\n0\none\n2\n0\n') as (bdb, f):
        bayeslite.bayesdb_read_csv(bdb, 'onecol', f, header=True, create=True)

def test_csv_import_toofewcols():
    with bayesdb_csv_stream('foo,bar\n0,1\n0\n') as (bdb, f):
        with pytest.raises(IOError):
            bayeslite.bayesdb_read_csv(bdb, 'bad', f, header=True, create=True)

def test_csv_import_toomanycols():
    with bayesdb_csv_stream('foo,bar\n0,1\n0,1,2\n') as (bdb, f):
        with pytest.raises(IOError):
            bayeslite.bayesdb_read_csv(bdb, 'bad', f, header=True, create=True)

def test_csv_import_dupcols():
    with bayesdb_csv_stream('foo,foo\n0,1\n') as (bdb, f):
        with pytest.raises(IOError):
            bayeslite.bayesdb_read_csv(bdb, 'bad', f, header=True, create=True)
    with bayesdb_csv_stream('foo,FOO\n0,1\n') as (bdb, f):
        with pytest.raises(IOError):
            bayeslite.bayesdb_read_csv(bdb, 'bad', f, header=True, create=True)

# Where did I get these data?  The gender balance needs work, as does
# representation of nonbinaries.
csv_data = '''age, gender, salary, height, division, rank
34, M, 74000, 65, sales, 3
41, M, 65600, 72, marketing, 4
25, M, 52000, 69, accounting, 5
23, F, 81000, 67, data science, 3
36, F, 96000, 70, management, 2
30, M, 70000, 73, sales, 4
30, F, 81000, 73, engineering, 3
'''

def test_csv_import():
    with bayesdb_csv_stream(csv_data) as (bdb, f):
        bayeslite.bayesdb_read_csv(bdb, 'employees', f, header=True,
            create=True)

def test_csv_import_schema():
    with bayesdb_csv_stream(csv_data) as (bdb, f):
        bdb.sql_execute('''
            CREATE TABLE employees(
                age INTEGER,
                gender TEXT,
                salary REAL,
                height INTEGER,
                division TEXT,
                rank INTEGER
            )
        ''')
        bayeslite.bayesdb_read_csv(bdb, 'employees', f, header=True,
            create=False)
        bdb.execute('select height from employees').fetchall()
        # XXX Currently this test fails because we compile the query
        # into `SELECT "idontexist" FROM "employees"', and for
        # compatibility with MySQL idiocy or something, SQLite treats
        # double-quotes as single-quotes if the alternative would be
        # an error.
        with pytest.raises(apsw.SQLError):
            bdb.execute('select idontexist from employees')
            raise apsw.SQLError('BQL compiler is broken;'
                ' a.k.a. sqlite3 is stupid.')
        bdb.execute('''
            CREATE POPULATION p_employees FOR employees (
                height IGNORE;
                age NUMERICAL;
                gender CATEGORICAL;
                salary CYCLIC;
                division CATEGORICAL;
                rank CATEGORICAL
            )
        ''')
        bdb.execute('''
            CREATE GENERATOR p_employees_cc for p_employees USING crosscat ()
        ''')
        bdb.execute('estimate height from p_employees').fetchall()
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('estimate predict height with confidence 0.9'
                ' from p_employees')

def test_csv_import_schema_case():
    with bayesdb_csv_stream(csv_data) as (bdb, f):
        bdb.sql_execute('''
            CREATE TABLE emPloyEES(
                AGE INTeger,
                geNder Text,
                saLAry REal,
                heighT inteGER,
                DIVision TEXt,
                rank INTEGER
            )
        ''')
        bayeslite.bayesdb_read_csv(bdb, 'employees', f, header=True,
            create=False)
        bayeslite.guess.bayesdb_guess_population(bdb, 'p_employees',
            'employees')

def test_csv_import_badschema0():
    with bayesdb_csv_stream(csv_data) as (bdb, f):
        bdb.sql_execute('''
            CREATE TABLE emPloyEES(
                AGE INTeger,
                geNder Text,
                -- saLAry REal,
                heighT inteGER,
                DIVision TEXt,
                rank INTEGER
            )
        ''')
        with pytest.raises(IOError):
            bayeslite.bayesdb_read_csv(bdb, 'employees', f, header=True,
                create=False)

def test_csv_import_badschema1():
    with bayesdb_csv_stream(csv_data) as (bdb, f):
        bdb.sql_execute('''
            CREATE TABLE employees(
                age INTEGER,
                zorblaxianism TEXT,
                salary INTEGER,
                height INTEGER NOT NULL PRIMARY KEY,
                division TEXT,
                rank CATEGORICAL
            )
        ''')
        with pytest.raises(IOError):
            bayeslite.bayesdb_read_csv(bdb, 'employees', f, header=True,
                create=False)

csv_data_missing = '''a,b,c
1,2,3
10,,30
100,200,nan
4,5,6
'''

def test_csv_missing():
    with bayesdb_csv_stream(csv_data_missing) as (bdb, f):
        # XXX Test the automatic column type guessing too.
        bdb.sql_execute('CREATE TABLE t(a REAL, b REAL, c REAL)')
        bayeslite.bayesdb_read_csv(bdb, 't', f, header=True, create=False)
        def clean(column_name):
            qcn = bql_quote_name(column_name)
            sql = "UPDATE t SET %s = NULL WHERE %s = '' OR %s LIKE 'NaN'" % \
                (qcn, qcn, qcn)
            bdb.sql_execute(sql)
        clean('a')
        clean('b')
        clean('c')
        assert bdb.execute('select * from t').fetchall() == [
            (1.0, 2.0, 3.0),
            (10.0, None, 30.0),
            (100.0, 200.0, None),
            (4.0, 5.0, 6.0),
        ]
