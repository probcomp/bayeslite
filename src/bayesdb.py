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
import bayeslite.core as core
import bayeslite.parse as parse
import bayeslite.schema as schema

from bayeslite.sqlite3_util import sqlite3_savepoint

class BayesDB(core.IBayesDB):
    """Class of Bayesian databases.

    Interface is loosely based on PEP-249 DB-API.
    """

    def __init__(self, engine, pathname=":memory:"):
        self.engine = engine
        # isolation_level=None actually means that the sqlite3 module
        # will not randomly begin and commit transactions where we
        # didn't ask it to.
        self.sqlite = sqlite3.connect(pathname, isolation_level=None)
        self.txn_depth = 0
        self.metadata_cache = None
        self.models_cache = None
        schema.bayesdb_install_schema(self.sqlite)
        core.bayesdb_install_bql(self.sqlite, self)

    def close(self):
        """Close the database.  Further use is not allowed."""
        assert self.txn_depth == 0, "pending BayesDB transactions"
        self.sqlite.close()
        self.sqlite = None

    def execute(self, string, bindings=()):
        """Execute a BQL query and return a cursor for its results."""
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

    @contextlib.contextmanager
    def savepoint(bdb):
        """Enter a savepoint.  On return, commit; on exception, roll back.

        Savepoints may be nested.  Parsed metadata and models are
        cached in Python during a savepoint.
        """
        # XXX Can't do this simultaneously in multiple threads.  Need
        # lightweight per-thread state.
        if bdb.txn_depth == 0:
            assert bdb.metadata_cache is None
            assert bdb.models_cache is None
            bdb.metadata_cache = {}
            bdb.models_cache = {}
        else:
            assert bdb.metadata_cache is not None
            assert bdb.models_cache is not None
        bdb.txn_depth += 1
        try:
            with sqlite3_savepoint(bdb.sqlite):
                yield
        finally:
            assert 0 < bdb.txn_depth
            bdb.txn_depth -= 1
            if bdb.txn_depth == 0:
                bdb.metadata_cache = None
                bdb.models_cache = None
