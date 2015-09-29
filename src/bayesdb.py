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
import time
import json
import string

import bayeslite.bql as bql
import bayeslite.bqlfn as bqlfn
import bayeslite.metamodel as metamodel
import bayeslite.parse as parse
import bayeslite.schema as schema
import bayeslite.txn as txn

bayesdb_open_cookie = 0xed63e2c26d621a5b5146a334849d43f0

def bayesdb_open(pathname=None, builtin_metamodels=None, save_sessions=True):
    """Open the BayesDB in the file at `pathname`.

    If there is no file at `pathname`, it is automatically created.
    If `pathname` is unspecified or ``None``, a temporary in-memory
    BayesDB instance is created.
    If save_sessions is true or unspecified, query trace logging
    information is saved in the database.
    """
    if builtin_metamodels is None:
        builtin_metamodels = True
    bdb = BayesDB(bayesdb_open_cookie, pathname=pathname,
                   save_sessions=save_sessions)
    if builtin_metamodels:
        metamodel.bayesdb_register_builtin_metamodels(bdb)
    return bdb

class BayesDB(object):
    """A handle for a Bayesian database in memory or on disk.

    Do not create BayesDB instances directly; use :func:`bayesdb_open` instead.

    An instance of `BayesDB` is a context manager that returns itself
    on entry and closes itself on exit, so you can write::

        with bayesdb_open(pathname='foo.bdb') as bdb:
            ...
    """

    def __init__(self, cookie, pathname=None, save_sessions=True):
        if cookie != bayesdb_open_cookie:
            raise ValueError('Do not construct BayesDB objects directly!')
        if pathname is None:
            pathname = ":memory:"
        # isolation_level=None actually means that the sqlite3 module
        # will not randomly begin and commit transactions where we
        # didn't ask it to.
        self.sqlite3 = sqlite3.connect(pathname, isolation_level=None)
        self.txn_depth = 0
        self.metamodels = {}
        self.tracer = None
        self.sql_tracer = None
        self.save_sessions = save_sessions
        self.cache = None
        self.temptable = 0
        schema.bayesdb_install_schema(self.sqlite3)
        bqlfn.bayesdb_install_bql(self.sqlite3, self)
        # warn about previous uncompleted commands
        if save_sessions:
            self.sqlite3.execute('INSERT INTO bayesdb_session DEFAULT VALUES')
            curs = self.sqlite3.execute('SELECT last_insert_rowid()')
            self.session_id = int([row[0] for row in curs][0])

        # Cache an empty cursor for convenience.
        self.empty_cursor = bql.BayesDBCursor(self, self.sqlite3.execute(''))

    def __enter__(self):
        return self
    def __exit__(self, *_exc_info):
        self.close()

    def close(self):
        """Close the database.  Further use is not allowed."""
        assert self.txn_depth == 0, "pending BayesDB transactions"
        self.sqlite3.close()
        self.sqlite3 = None

    def trace(self, tracer):
        """Call `tracer` for each BQL query executed.

        `tracer` will be called with two arguments: the query to be
        executed, as a string; and the sequence or dictionary of
        bindings.

        Only one tracer can be established at a time.  To remove it,
        use :meth:`~BayesDB.untrace`.
        """
        assert self.tracer is None
        self.tracer = tracer

    def untrace(self, tracer):
        """Stop calling `tracer` for each BQL query executed.

        `tracer` must have been previously established with
        :meth:`~BayesDB.trace`.
        """
        assert self.tracer == tracer
        self.tracer = None

    def sql_trace(self, tracer):
        """Call `tracer` for each SQL query executed.

        `tracer` will be called with two arguments: the query to be
        executed, as a string; and the sequence or dictionary of
        bindings.

        Only one tracer can be established at a time.  To remove it,
        use :meth:`~BayesDB.sql_untrace`.
        """
        assert self.sql_tracer is None
        self.sql_tracer = tracer

    def sql_untrace(self, tracer):
        """Stop calling `tracer` for each SQL query executed.

        `tracer` must have been previously established with
        :meth:`~BayesDB.sql_trace`.
        """
        assert self.sql_tracer == tracer
        self.sql_tracer = None

    def check_uncompleted_session_entries(self):
        '''Check if the previous session ended with a failed command and
        suggest sending the session'''
        cursor = self.sqlite3.execute('''SELECT COUNT(*) FROM
        bayesdb_session_entries WHERE session_id=?''', (self.session_id-1,))
        uncompleted_entries = int([row[0] for row in curs][0])
        if uncompleted_entries > 0:
            print '''Previous session contains uncompleted entries. This may
            be due to a bad termination or crash of the previous session.
            Consider uploading the session.'''

    def create_session_entry(self, type, data, extra=None):
        '''Save a session entry into the database, if we are doing that.'''
        if not self.save_sessions:
            return
        if extra:
            data += json.dumps(extra)
        t = time.time()
        self.sqlite3.execute('''INSERT INTO bayesdb_session_entries
                                (session_id, time, type, data)
                                VALUES (?,?,?,?)
                             ''',
                             (self.session_id, t, type, data))
        # the entry is initially in the not-completed state. return the new
        # entry's id so that it can be set to completed when appropriate
        curs = self.sqlite3.execute('SELECT last_insert_rowid()')
        return int([row[0] for row in curs][0])

    def set_entry_completed(self, entry_id):
        self.sqlite3.execute('''UPDATE bayesdb_session_entries
        SET completed=1 WHERE id=?
        ''', (entry_id,))

    def execute(self, string, bindings=None):
        """Execute a BQL query and return a cursor for its results.

        The argument `string` is a string parsed into a single BQL
        query.  It must contain exactly one BQL phrase, optionally
        terminated by a semicolon.

        The argument `bindings` is a sequence or dictionary of
        bindings for parameters in the query, or ``None`` to supply no
        bindings.
        """
        if bindings is None:
            bindings = ()
        if self.tracer:
            self.tracer(string, bindings)
        entry_id = self.create_session_entry("bql", string, bindings)
        phrases = parse.parse_bql_string(string)
        phrase = None
        try:
            phrase = phrases.next()
        except StopIteration:
            raise ValueError('no BQL phrase in string')
        try:
            phrases.next()
        except StopIteration:
            pass
        else:
            raise ValueError('>1 phrase in string')
        cursor = bql.execute_phrase(self, phrase, bindings)
        self.set_entry_completed(entry_id)
        return self.empty_cursor if cursor is None else cursor

    def sql_execute(self, string, bindings=None):
        """Execute a SQL query on the underlying SQLite database.

        The argument `string` is a string parsed into a single SQL
        query.  It must contain exactly one SQL phrase, optionally
        terminated by a semicolon.

        The argument `bindings` is a sequence or dictionary of
        bindings for parameters in the query, or ``None`` to supply no
        bindings.
        """
        if bindings is None:
            bindings = ()
        if self.sql_tracer:
            self.sql_tracer(string, bindings)
        entry_id = self.create_session_entry("sql", string, bindings)
        cursor = self.sqlite3.execute(string, bindings)
        self.set_entry_completed(entry_id)
        return bql.BayesDBCursor(self, cursor)

    @contextlib.contextmanager
    def savepoint(self):
        """Savepoint context.  On return, commit; on exception, roll back.

        Savepoints may be nested.  Parsed metadata and models are
        cached in Python during a savepoint.
        """
        with txn.bayesdb_savepoint(self):
            yield

    @contextlib.contextmanager
    def savepoint_rollback(self):
        """Auto-rollback savepoint context.  Roll back on return or exception.
        """
        with txn.bayesdb_savepoint_rollback(self):
            yield

    @contextlib.contextmanager
    def transaction(self):
        """Transaction context.  On return, commit; on exception, roll back.

        Transactions may not be nested: use a savepoint if you need
        nesting.  Parsed metadata and models are cached in Python
        during a savepoint.
        """
        with txn.bayesdb_transaction(self):
            yield

    def temp_table_name(self):
        n = self.temptable
        self.temptable += 1
        return 'bayesdb_temp_%u' % (n,)
