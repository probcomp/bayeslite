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

import sqlite3

import bayeslite.core as core

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
        core.bayesdb_install_schema(self.sqlite)
        core.bayesdb_install_bql(self.sqlite, self)

    def close(self):
        """Close the database.  Further use is not allowed."""
        assert self.txn_depth == 0, "pending BayesDB transactions"
        self.sqlite.close()
        self.sqlite = None

    def cursor(self):
        """Return a cursor fit for executing BQL queries."""
        # XXX Make our own cursors that handle BQL.
        return self.sqlite.cursor()

    def execute(self, query, *args):
        """Execute a BQL query and return a cursor for its results."""
        # XXX Parse and compile query first.  Would be nice if we
        # could hook into the sqlite parser, but that's not going to
        # happen.
        return self.sqlite.execute(query, *args)
