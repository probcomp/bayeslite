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
import sqlite3

import bayeslite.bql as bql
import bayeslite.bqlfn as bqlfn
import bayeslite.parse as parse
import bayeslite.schema as schema
import bayeslite.txn as txn

class BayesDB(object):
    """Class of Bayesian databases.

    Interface is loosely based on PEP-249 DB-API.
    """

    def __init__(self, pathname=":memory:"):
        # isolation_level=None actually means that the sqlite3 module
        # will not randomly begin and commit transactions where we
        # didn't ask it to.
        self.sqlite3 = sqlite3.connect(pathname, isolation_level=None)
        self.txn_depth = 0
        self.metamodels = {}
        self.default_metamodel = None
        self.tracer = None
        self.sql_tracer = None
        self.cache = None
        schema.bayesdb_install_schema(self.sqlite3)
        bqlfn.bayesdb_install_bql(self.sqlite3, self)

    def close(self):
        """Close the database.  Further use is not allowed."""
        assert self.txn_depth == 0, "pending BayesDB transactions"
        self.sqlite3.close()
        self.sqlite3 = None

    def trace(self, tracer):
        assert self.tracer is None
        self.tracer = tracer

    def untrace(self, tracer):
        assert self.tracer == tracer
        self.tracer = None

    def sql_trace(self, tracer):
        assert self.sql_tracer is None
        self.sql_tracer = tracer

    def sql_untrace(self, tracer):
        assert self.sql_tracer == tracer
        self.sql_tracer = None

    def execute(self, string, bindings=()):
        """Execute a BQL query and return a cursor for its results."""
        if self.tracer:
            self.tracer(string, bindings)
        phrases = parse.parse_bql_string(string)
        phrase = None
        try:
            phrase = phrases.next()
        except StopIteration:
            raise ValueError('no BQL phrase in string')
        more = None
        try:
            phrases.next()
            more = True
        except StopIteration:
            more = False
        if more:
            raise ValueError('>1 phrase in string')
        return bql.execute_phrase(self, phrase, bindings)

    def sql_execute(self, string, bindings=()):
        """Execute a SQL query on the underlying SQLite database."""
        if self.sql_tracer:
            self.sql_tracer(string, bindings)
        return self.sqlite3.execute(string, bindings)

    @contextlib.contextmanager
    def savepoint(self):
        """Savepoint context.  On return, commit; on exception, roll back.

        Savepoints may be nested.  Parsed metadata and models are
        cached in Python during a savepoint.
        """
        with txn.bayesdb_savepoint(self):
            yield

    def set_progress_handler(self, handler, n):
        """Call HANDLER periodically during query execution."""
        self.sqlite3.set_progress_handler(handler, n)
