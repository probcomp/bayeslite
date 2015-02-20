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

import StringIO
import contextlib
import pytest
import tempfile

import bayeslite

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
            bayeslite.bayesdb_import_csv(bdb, 'empty', f)

def test_csv_import_nocols():
    with bayesdb_csv_stream('\n') as (bdb, f):
        # CSV import rejects no columns.
        with pytest.raises(IOError):
            bayeslite.bayesdb_import_csv(bdb, 'nocols', f)

def test_csv_import_onecol_key():
    with bayesdb_csv_stream('foo\n0\none\n2\n') as (bdb, f):
        # foo will be a key column, hence no columns to model.
        with pytest.raises(ValueError):
            bayeslite.bayesdb_import_csv(bdb, 'onecol_key', f)

def test_csv_import_onecol():
    with bayesdb_csv_stream('foo\n0\none\n2\n0\n') as (bdb, f):
        bayeslite.bayesdb_import_csv(bdb, 'onecol', f)

def test_csv_import_toofewcols():
    with bayesdb_csv_stream('foo,bar\n0,1\n0\n') as (bdb, f):
        with pytest.raises(IOError):
            bayeslite.bayesdb_import_csv(bdb, 'bad', f)

def test_csv_import_toomanycols():
    with bayesdb_csv_stream('foo,bar\n0,1\n0,1,2\n') as (bdb, f):
        with pytest.raises(IOError):
            bayeslite.bayesdb_import_csv(bdb, 'bad', f)

def test_csv_import_dupcols():
    with bayesdb_csv_stream('foo,foo\n0,1\n') as (bdb, f):
        with pytest.raises(IOError):
            bayeslite.bayesdb_import_csv(bdb, 'bad', f)
    with bayesdb_csv_stream('foo,FOO\n0,1\n') as (bdb, f):
        with pytest.raises(IOError):
            bayeslite.bayesdb_import_csv(bdb, 'bad', f)

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
        bayeslite.bayesdb_import_csv(bdb, 'employees', f)

def test_csv_import_file():
    with bayesdb_csv_file(csv_data) as (bdb, fname):
        bayeslite.bayesdb_import_csv_file(bdb, 'employees', fname)

def test_csv_import_schema():
    with bayesdb_csv_stream(csv_data) as (bdb, f):
        bayeslite.bayesdb_import_csv(bdb, 'employees', f,
            column_types={
                'age': 'numerical',
                'gender': 'categorical',
                'salary': 'cyclic',
                'height': 'ignore',
                'division': 'categorical',
                'rank': 'categorical',
            })

def test_csv_import_schema_case():
    with bayesdb_csv_stream(csv_data) as (bdb, f):
        bayeslite.bayesdb_import_csv(bdb, 'employees', f,
            column_types={
                'age': 'numerical',
                'GENDER': 'categorical',
                'salary': 'cyclic',
                'HEIght': 'ignore',
                'division': 'categorical',
                'RANK': 'categorical',
            })

def test_csv_import_badschema0():
    with bayesdb_csv_stream(csv_data) as (bdb, f):
        with pytest.raises(IOError):
            bayeslite.bayesdb_import_csv(bdb, 'employees', f,
                column_types={
                    'age': 'numerical',
                    'division': 'categorical',
                    'rank': 'categorical',
                })

def test_csv_import_badschema1():
    with bayesdb_csv_stream(csv_data) as (bdb, f):
        with pytest.raises(IOError):
            bayeslite.bayesdb_import_csv(bdb, 'employees', f,
                column_types={
                    'age': 'numerical',
                    'zorblaxianism': 'categorical',
                    'salary': 'cyclic',
                    'height': 'key',
                    'division': 'categorical',
                    'rank': 'categorical',
                })

csv_data_missing = '''a,b,c
1,2,3
10,,30
100,200,nan
4,5,6
'''

def test_csv_missing():
    with bayesdb_csv_stream(csv_data_missing) as (bdb, f):
        # XXX Test the automatic column type guessing too.
        bayeslite.bayesdb_import_csv(bdb, 't', f,
            column_types={
                'a': 'numerical',
                'b': 'numerical',
                'c': 'numerical',
            })
        # XXX These should be proper NaNs, not None.
        assert list(bdb.execute('select * from t')) == [
            (1.0, 2.0, 3.0),
            (10.0, None, 30.0),
            (100.0, 200.0, None),
            (4.0, 5.0, 6.0),
        ]
