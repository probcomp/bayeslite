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

import bayeslite.core as core

def bayesdb_register_metamodel(bdb, name, engine):
    with bdb.savepoint():
        # Name it in the SQL database.
        insert_sql = """
            INSERT OR IGNORE INTO bayesdb_metamodel (name) VALUES (?)
        """
        bdb.sql_execute(insert_sql, (name,))
        # Associate it with the engine by id.
        #
        # XXX Can't use lastrowid here because
        # sqlite3_last_insert_rowid doesn't consider INSERT OR IGNORE
        # to be successful if it has to ignore the insertion, even
        # though the obvious sensible thing to do is to return the
        # existing rowid.
        lookup_sql = "SELECT id FROM bayesdb_metamodel WHERE name = ?"
        metamodel_id = core.bayesdb_sql_execute1(bdb, lookup_sql, (name,))
        assert metamodel_id not in bdb.metamodels_by_id
        engine.register(bdb, name)
        bdb.metamodels_by_id[metamodel_id] = engine

def bayesdb_deregister_metamodel(bdb, name):
    with bdb.savepoint():
        lookup_sql = "SELECT id FROM bayesdb_metamodel WHERE name = ?"
        metamodel_id = core.bayesdb_sql_execute1(bdb, lookup_sql, (name,))
        assert metamodel_id in bdb.metamodels_by_id
        assert bdb.default_metamodel_id != metamodel_id
        del bdb.metamodels_by_id[metamodel_id]

def bayesdb_set_default_metamodel(bdb, name):
    if name is None:
        bdb.default_metamodel_id = None
    else:
        lookup_sql = "SELECT id FROM bayesdb_metamodel WHERE name = ?"
        metamodel_id = core.bayesdb_sql_execute1(bdb, lookup_sql, (name,))
        bdb.default_metamodel_id = metamodel_id

class IMetamodelEngine(object):
    def register(self, bdb, name):
        raise NotImplementedError
    def create_metadata(self, bdb, table, column_names, column_types):
        raise NotImplementedError
    def initialize(self, **kwargs):
        raise NotImplementedError
    def analyze(self, **kwargs):
        raise NotImplementedError
    def mutual_information(self, **kwargs):
        raise NotImplementedError
    def column_structural_typicality(self, **kwargs):
        raise NotImplementedError
    def simple_predictive_probability_multistate(self, **kwargs):
        raise NotImplementedError
    def similarity(self, **kwargs):
        raise NotImplementedError
    def row_structural_typicality(self, **kwargs):
        raise NotImplementedError
    def impute_and_confidence(self, **kwargs):
        raise NotImplementedError
    def simple_predictive_sample(self, **kwargs):
        raise NotImplementedError
    def insert(self, **kwargs):
        raise NotImplementedError
