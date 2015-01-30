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

import binascii
import contextlib
import os
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
            with sqlite3_savepoint(bdb):
                yield
        finally:
            assert 0 < bdb.txn_depth
            bdb.txn_depth -= 1
            if bdb.txn_depth == 0:
                bdb.metadata_cache = None
                bdb.models_cache = None

@contextlib.contextmanager
def sqlite3_transaction(db):
    """Run a transaction.  On return, commit.  On exception, roll back.

    Transactions may not be nested.  Use savepoints if you want a
    nestable analogue to transactions.
    """
    db.execute("BEGIN")
    ok = False
    try:
        yield
        db.execute("COMMIT")
        ok = True
    finally:
        if not ok:
            db.execute("ROLLBACK")

@contextlib.contextmanager
def sqlite3_savepoint(db):
    """Run a savepoint.  On return, commit; on exception, roll back.

    Savepoints are like transactions, but they may be nested in
    transactions or in other savepoints.
    """
    # This is not symmetric with sqlite3_transaction because ROLLBACK
    # undoes any effects and makes the transaction cease to be,
    # whereas ROLLBACK TO undoes any effects but leaves the savepoint
    # as is.  So for either success or failure we must release the
    # savepoint explicitly.
    savepoint = binascii.b2a_hex(os.urandom(32))
    db.execute("SAVEPOINT x%s" % (savepoint,))
    ok = False
    try:
        yield
        ok = True
    finally:
        if not ok:
            db.execute("ROLLBACK TO x%s" % (savepoint,))
        db.execute("RELEASE x%s" % (savepoint,))
