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

import contextlib

from bayeslite.exception import BayesDBException
from bayeslite.sqlite3_util import sqlite3_savepoint
from bayeslite.sqlite3_util import sqlite3_savepoint_rollback
from bayeslite.sqlite3_util import sqlite3_transaction

# XXX Can't do this simultaneously in multiple threads.  Need
# lightweight per-thread state.

@contextlib.contextmanager
def bayesdb_caching(bdb):
    bayesdb_txn_push(bdb)
    try:
        yield
    finally:
        bayesdb_txn_pop(bdb)

@contextlib.contextmanager
def bayesdb_savepoint(bdb):
    bayesdb_txn_push(bdb)
    try:
        with sqlite3_savepoint(bdb._sqlite3):
            yield
    finally:
        bayesdb_txn_pop(bdb)

@contextlib.contextmanager
def bayesdb_savepoint_rollback(bdb):
    bayesdb_txn_push(bdb)
    try:
        with sqlite3_savepoint_rollback(bdb._sqlite3):
            yield
    finally:
        bayesdb_txn_pop(bdb)

@contextlib.contextmanager
def bayesdb_transaction(bdb):
    if bdb._txn_depth != 0:
        raise BayesDBTxnError(bdb, 'Already in a transaction!')
    bayesdb_txn_init(bdb)
    bdb._txn_depth = 1
    try:
        with sqlite3_transaction(bdb._sqlite3):
            yield
    finally:
        assert bdb._txn_depth == 1
        bdb._txn_depth = 0
        bayesdb_txn_fini(bdb)

def bayesdb_begin_transaction(bdb):
    if bdb._txn_depth != 0:
        raise BayesDBTxnError(bdb, 'Already in a transaction!')
    bayesdb_txn_init(bdb)
    bdb._txn_depth = 1
    bdb.sql_execute("BEGIN")

def bayesdb_rollback_transaction(bdb):
    if bdb._txn_depth == 0:
        raise BayesDBTxnError(bdb, 'Not in a transaction!')
    bdb.sql_execute("ROLLBACK")
    bdb._txn_depth = 0
    bayesdb_txn_fini(bdb)

def bayesdb_commit_transaction(bdb):
    if bdb._txn_depth == 0:
        raise BayesDBTxnError(bdb, 'Not in a transaction!')
    bdb.sql_execute("COMMIT")
    bdb._txn_depth = 0
    bayesdb_txn_fini(bdb)

# XXX Maintaining a stack of savepoints in BQL is a little more
# trouble than it is worth at the moment, since users can rollback to
# or release any savepoint in the stack, not just the most recent one.
# (For the bdb.savepoint() context manager that is not an issue.)
# We'll implement that later.

def bayesdb_txn_push(bdb):
    if bdb._txn_depth == 0:
        bayesdb_txn_init(bdb)
    else:
        assert bdb._cache is not None
    bdb._txn_depth += 1

def bayesdb_txn_pop(bdb):
    bdb._txn_depth -= 1
    if bdb._txn_depth == 0:
        bayesdb_txn_fini(bdb)
    else:
        assert bdb._cache is not None

def bayesdb_txn_init(bdb):
    assert bdb._txn_depth == 0
    assert bdb._cache is None
    bdb._cache = {}

def bayesdb_txn_fini(bdb):
    assert bdb._txn_depth == 0
    assert bdb._cache is not None
    bdb._cache = None

class BayesDBTxnError(BayesDBException):
    """Transaction errors in a BayesDB."""

    pass
