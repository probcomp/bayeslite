# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2015, MIT Probabilistic Computing Project
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

import pytest
import tempfile

from bayeslite import bayesdb_open
from bayeslite import bayesdb_upgrade_schema
from bayeslite.exception import BayesDBException
from bayeslite.schema import bayesdb_schema_required

import test_core

def test_schema_upgrade():
    with bayesdb_open(version=6) as bdb:
        bayesdb_schema_required(bdb, 6, 'test pre-upgrade 6')
        with pytest.raises(BayesDBException):
            bayesdb_schema_required(bdb, 7, 'test pre-upgrade 7')
        test_core.t1_schema(bdb)
        test_core.t1_data(bdb)
        bayesdb_upgrade_schema(bdb)
        bayesdb_schema_required(bdb, 6, 'test post-upgrade 6')
        bayesdb_schema_required(bdb, 7, 'test post-upgrade 7')
        with pytest.raises(BayesDBException):
            # Nobody'll ever bump the schema version this many times,
            # right?
            bayesdb_schema_required(bdb, 1000000, 'test a gazillion')

def test_schema_incompatible():
    with tempfile.NamedTemporaryFile(prefix='bayeslite') as f:
        with bayesdb_open(pathname=f.name, version=6) as bdb:
            bayesdb_schema_required(bdb, 6, 'test incompatible 0/6')
            with pytest.raises(BayesDBException):
                bayesdb_schema_required(bdb, 7, 'test incompatible 0/7')
        with bayesdb_open(pathname=f.name) as bdb:
            bayesdb_schema_required(bdb, 6, 'test incompatible 1/6')
            bayesdb_schema_required(bdb, 7, 'test incompatible 1/7')
            bayesdb_upgrade_schema(bdb)
        with bayesdb_open(pathname=f.name) as bdb:
            bayesdb_schema_required(bdb, 6, 'test incompatible 2/6')
            bayesdb_schema_required(bdb, 7, 'test incompatible 2/7')

def test_schema_compatible():
    with tempfile.NamedTemporaryFile(prefix='bayeslite') as f:
        with bayesdb_open(pathname=f.name, version=6) as bdb:
            bayesdb_schema_required(bdb, 6, 'test compatible 0/6')
            with pytest.raises(BayesDBException):
                bayesdb_schema_required(bdb, 7, 'test compatible 0/7')
        with bayesdb_open(pathname=f.name, compatible=True) as bdb:
            bayesdb_schema_required(bdb, 6, 'test nonautomatic 1/6')
            with pytest.raises(BayesDBException):
                bayesdb_schema_required(bdb, 7, 'test compatible 1/7')
            bayesdb_upgrade_schema(bdb)
        with bayesdb_open(pathname=f.name, compatible=True) as bdb:
            bayesdb_schema_required(bdb, 6, 'test compatible 2/6')
            bayesdb_schema_required(bdb, 7, 'test compatible 2/7')
