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
import pytest
import tempfile

import bayeslite

import test_core

@contextlib.contextmanager
def bayesdb_csv(csv):
    with test_core.bayesdb() as bdb:
        with tempfile.NamedTemporaryFile(prefix='bayeslite') as f:
            with open(f.name, 'w') as out:
                out.write(csv)
            yield (bdb, f.name)

def test_csv_import_empty():
    with bayesdb_csv('') as (bdb, fname):
        with pytest.raises(IOError):
            bayeslite.bayesdb_import_csv_file(bdb, 'empty', fname)

def test_csv_import_nocols():
    with bayesdb_csv('\n') as (bdb, fname):
        with pytest.raises(IOError):
            bayeslite.bayesdb_import_csv_file(bdb, 'nocols', fname)

def test_csv_import_onecol():
    with bayesdb_csv('foo\n0\none\n2\n') as (bdb, fname):
        bayeslite.bayesdb_import_csv_file(bdb, 'onecol', fname)

def test_csv_import_toofewcols():
    with bayesdb_csv('foo,bar\n0,1\n0\n') as (bdb, fname):
        with pytest.raises(IOError):
            bayeslite.bayesdb_import_csv_file(bdb, 'bad', fname)

def test_csv_import_toomanycols():
    with bayesdb_csv('foo,bar\n0,1\n0,1,2\n') as (bdb, fname):
        with pytest.raises(IOError):
            bayeslite.bayesdb_import_csv_file(bdb, 'bad', fname)

def test_csv_import_dupcols():
    with bayesdb_csv('foo,foo\n0,1\n') as (bdb, fname):
        with pytest.raises(IOError):
            bayeslite.bayesdb_import_csv_file(bdb, 'bad', fname)
    with bayesdb_csv('foo,FOO\n0,1\n') as (bdb, fname):
        with pytest.raises(IOError):
            bayeslite.bayesdb_import_csv_file(bdb, 'bad', fname)

# Where did I get these data?  The gender balance needs work, as does
# representation of nonbinaries.
csv_data = '''age, gender, salary, height, division, rank
34, M, 74000, 65, sales, 3
41, M, 65600, 72, marketing, 4
25, M, 52000, 69, accounting, 5
23, F, 81000, 67, data science, 3
36, F, 96000, 70, management, 2
30, M, 70000, 73, sales, 4
'''

def test_csv_import():
    with bayesdb_csv(csv_data) as (bdb, fname):
        bayeslite.bayesdb_import_csv_file(bdb, 'employees', fname)

def test_csv_import_schema():
    with bayesdb_csv(csv_data) as (bdb, fname):
        bayeslite.bayesdb_import_csv_file(bdb, 'employees', fname,
            column_types={
                'age': 'numerical',
                'gender': 'categorical',
                'salary': 'cyclic',
                'height': 'key',
                'division': 'categorical',
                'rank': 'categorical',
            })

def test_csv_import_badschema0():
    with bayesdb_csv(csv_data) as (bdb, fname):
        with pytest.raises(IOError):
            bayeslite.bayesdb_import_csv_file(bdb, 'employees', fname,
                column_types={
                    'age': 'numerical',
                    'division': 'categorical',
                    'rank': 'categorical',
                })

def test_csv_import_badschema1():
    with bayesdb_csv(csv_data) as (bdb, fname):
        with pytest.raises(IOError):
            bayeslite.bayesdb_import_csv_file(bdb, 'employees', fname,
                column_types={
                    'age': 'numerical',
                    'zorblaxianism': 'categorical',
                    'salary': 'cyclic',
                    'height': 'key',
                    'division': 'categorical',
                    'rank': 'categorical',
                })
