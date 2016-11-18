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

import os

import pytest

import crosscat.LocalEngine

import bayeslite
import bayeslite.read_csv as read_csv

from bayeslite.guess import bayesdb_guess_population
from bayeslite.metamodels.crosscat import CrosscatMetamodel


data_multivariate = [
    ('foo', 6, 7, None),
    ('bar', 1, 1, 2),
    ('baz', 100, 100, 200),
    ('quux', 1000, 2000, 3000),
    ('zot', 0, 2, 2),
    ('mumble', 20, 10, 30),
    ('frotz', 4, 13, 17),
    ('gargle', 34, 2, 36),
    ('mumph', 78, 4, 82),
    ('hunf', 90, 1, 91),
    ('blort', 80, 80, 160)
]


# Test that GIVEN statement can accept a multivariate constraint clause in
# which one of the constraints is on _rowid_.
bdb = bayeslite.bayesdb_open(':memory:')
bdb.sql_execute(
    'CREATE TABLE t(x TEXT, y NUMERIC, z NUMERIC, w NUMERIC)')
for row in data_multivariate[:4]:
    bdb.sql_execute(
        'INSERT INTO t (x, y, z, w) VALUES (?, ?, ?, ?)', row)
bdb.execute('''
    CREATE POPULATION p FOR t WITH SCHEMA (
        MODEL y, z, w AS NUMERICAL;
        IGNORE x
    )
''')

bdb.execute('CREATE METAMODEL m0 FOR p;')
bdb.execute('INITIALIZE 2 MODELS FOR m0')
bdb.execute('ANALYZE m0 FOR 20 ITERATION WAIT ( OPTIMIZED )')

bdb.execute('CREATE METAMODEL m1 FOR p;')
bdb.execute('INITIALIZE 2 MODELS FOR m1')
bdb.execute('ANALYZE m1 FOR 20 ITERATION WAIT ( OPTIMIZED )')

bdb.execute('ALTER POPULATION p RESAMPLE(100);')