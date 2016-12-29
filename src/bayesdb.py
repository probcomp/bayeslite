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

import apsw
import contextlib
import numpy.random
import random
import struct

import bayeslite.bql as bql
import bayeslite.bqlfn as bqlfn
import bayeslite.metamodel as metamodel
import bayeslite.parse as parse
import bayeslite.schema as schema
import bayeslite.txn as txn
import bayeslite.weakprng as weakprng

from bayeslite.util import cursor_value

bayesdb_open_cookie = 0xed63e2c26d621a5b5146a334849d43f0

def bayesdb_open(pathname=None, builtin_metamodels=None, seed=None,
        version=None, compatible=None):
    """Open the BayesDB in the file at `pathname`.

    If there is no file at `pathname`, it is automatically created.
    If `pathname` is unspecified or ``None``, a temporary in-memory
    BayesDB instance is created.

    `seed` is a 32-byte string specifying a pseudorandom number
    generation seed.  If not specified, it defaults to all zeros.

    If `compatible` is `None` or `False` and the database already
    exists, `bayesdb_open` may have the effect of incompatibly
    changing the format of the database so that older versions of
    bayeslite cannot read it.  If `compatible` is `True`,
    `bayesdb_open` will not incompatibly change the format of the
    database (but some newer bayesdb features may not work).
    """
    if builtin_metamodels is None:
        builtin_metamodels = True
    bdb = BayesDB(bayesdb_open_cookie, pathname=pathname, seed=seed,
        version=version, compatible=compatible)
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

    def __init__(self, cookie, pathname=None, seed=None, version=None,
            compatible=None):
        if cookie != bayesdb_open_cookie:
            raise ValueError('Do not construct BayesDB objects directly!')
        if pathname is None:
            pathname = ":memory:"
        self.pathname = pathname
        self._sqlite3 = apsw.Connection(pathname)
        self._txn_depth = 0     # managed in txn.py
        self._cache = None      # managed in txn.py
        self.metamodels = {}
        self.tracer = None
        self.sql_tracer = None
        self.temptable = 0
        self.qid = 0
        if seed is None:
            seed = struct.pack('<QQQQ', 0, 0, 0, 0)
        self._prng = weakprng.weakprng(seed)
        pyrseed = self._prng.weakrandom32()
        self._py_prng = random.Random(pyrseed)
        nprseed = [self._prng.weakrandom32() for _ in range(4)]
        self._np_prng = numpy.random.RandomState(nprseed)
        schema.bayesdb_install_schema(self, version=version,
            compatible=compatible)
        bqlfn.bayesdb_install_bql(self._sqlite3, self)

        # Cache an empty cursor for convenience.
        empty_cursor = self._sqlite3.cursor()
        empty_cursor.execute('')
        self._empty_cursor = bql.BayesDBCursor(self, empty_cursor)

    def __enter__(self):
        return self
    def __exit__(self, *_exc_info):
        self.close()

    def close(self):
        """Close the database.  Further use is not allowed."""
        assert self._txn_depth == 0, "pending BayesDB transactions"
        self._sqlite3.close()
        self._sqlite3 = None

    @property
    def py_prng(self):
        """A :class:`random.Random` object local to this BayesDB instance.

        This pseudorandom number generator is deterministically
        initialized from the seed supplied to :func:`bayesdb_open`.
        Use it to conserve reproducibility of results.
        """
        return self._py_prng

    @property
    def np_prng(self):
        """A Numpy RandomState object local to this BayesDB instance.

        This pseudorandom number generator is deterministically
        initialized from the seed supplied to :func:`bayesdb_open`.
        Use it to conserve reproducibility of results.
        """
        return self._np_prng

    @property
    def cache(self):
        return self._cache

    def trace(self, tracer):
        """Trace execution of BQL queries.

        For simple tracing, pass a function or arbitrary Python
        callable as the `tracer`.  It will be called at the start of
        execution of each BQL query, with two arguments: the query to
        be executed, as a string; and the sequence or dictionary of
        bindings.

        For articulated tracing, pass an instance of
        :class:`~IBayesDBTracer`, whose methods will be called in the
        pattern described in its documentation.

        Only one tracer can be established at a time.  To remove it,
        use :meth:`~BayesDB.untrace`.

        """
        assert self.tracer is None
        self.tracer = tracer

    def untrace(self, tracer):
        """Stop tracing execution of BQL queries.

        `tracer` must have been previously established with
        :meth:`~BayesDB.trace`.

        Any queries currently in progress will continue to be traced
        until completion.

        """
        assert self.tracer == tracer
        self.tracer = None

    def sql_trace(self, tracer):
        """Trace execution of SQL queries.

        For simple tracing, pass a function or arbitrary Python
        callable as the `tracer`.  It will be called at the start of
        execution of each SQL query, with two arguments: the query to
        be executed, as a string; and the sequence or dictionary of
        bindings.

        For articulated tracing, pass an instance of
        :class:`~IBayesDBTracer`, whose methods will be called in the
        pattern described in its documentation.

        Only one tracer can be established at a time.  To remove it,
        use :meth:`~BayesDB.sql_untrace`.

        """
        assert self.sql_tracer is None
        self.sql_tracer = tracer

    def sql_untrace(self, tracer):
        """Stop tracing execution of SQL queries.

        `tracer` must have been previously established with
        :meth:`~BayesDB.sql_trace`.

        Any queries currently in progress will continue to be traced
        until completion.

        """
        assert self.sql_tracer == tracer
        self.sql_tracer = None

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
        return self._maybe_trace(
            self.tracer, self._do_execute, string, bindings)

    def _maybe_trace(self, tracer, meth, string, bindings):
        if tracer and isinstance(tracer, IBayesDBTracer):
            return self._trace_articulately(
                tracer, meth, string, bindings)
        if tracer:
            tracer(string, bindings)
        return meth(string, bindings)

    def _qid(self):
        self.qid += 1
        return self.qid

    def _trace_articulately(self, tracer, meth, string, bindings):
        qid = self._qid()
        tracer.start(qid, string, bindings)
        try:
            cursor = meth(string, bindings)
            tracer.ready(qid, cursor)
            if cursor == self._empty_cursor:
                tracer.finished(qid)
                # Calling abandoned here is a choice.  On the one
                # hand, I know the client can't get anything out of
                # the cursor; on the other hand, returning a
                # TracingCursor would let the tracer detect when the
                # client had released their null result cursor.  Why
                # would the tracer care about that?
                tracer.abandoned(qid)
                return cursor
            else:
                return TracingCursor(tracer, qid, cursor)
        except Exception as e:
            tracer.error(qid, e)
            raise

    def _do_execute(self, string, bindings):
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
        return self._empty_cursor if cursor is None else cursor

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
        return self._maybe_trace(
            self.sql_tracer, self._do_sql_execute, string, bindings)

    def _do_sql_execute(self, string, bindings):
        cursor = self._sqlite3.cursor()
        cursor.execute(string, bindings)
        return bql.BayesDBCursor(self, cursor)

    @contextlib.contextmanager
    def savepoint(self):
        """Savepoint context.  On return, commit; on exception, roll back.

        The effects of a savepoint happen durably all at once if
        committed, or not at all if rolled back.

        Savepoints may be nested.  Parsed metadata and models are
        cached in Python during a savepoint.

        Example::

            with bdb.savepoint():
                bdb.execute('DROP GENERATOR foo')
                try:
                    with bdb.savepoint():
                        bdb.execute('ALTER TABLE foo RENAME TO bar')
                        raise NeverMind
                except NeverMind:
                    # No changes will be recorded.
                    pass
                bdb.execute('CREATE GENERATOR foo ...')
            # foo will have been dropped and re-created.
        """
        with txn.bayesdb_savepoint(self):
            yield

    @contextlib.contextmanager
    def savepoint_rollback(self):
        """Auto-rollback savepoint context.  Roll back on return or exception.

        This may be used to compute hypotheticals -- the bdb is
        guaranteed to remain unmodified afterward.
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

    def last_insert_rowid(self):
        """Return the rowid of the row most recently inserted."""
        return self._sqlite3.last_insert_rowid()

    def reconnect(self):
        """Reconnecting may sometimes be necessary, e.g. before a DROP TABLE"""
        # http://stackoverflow.com/questions/32788271
        if self.pathname == ":memory:":
            raise ValueError("""Cannot meaningfully reconnect to an in-memory
                database. All prior transactions would be lost.""")
        assert self._txn_depth == 0, "pending BayesDB transactions"
        self._sqlite3.close()
        self._sqlite3 = apsw.Connection(self.pathname)

    def changes(self):
        """Return the number of changes of the last INSERT, DELETE, or UPDATE.

        This may return unexpected results after a statement that is not an
        INSERT, DELETE, or UPDATE.
        """
        return self._sqlite3.changes()

class IBayesDBTracer(object):
    """BayesDB articulated tracing interface.

    If you just want to trace start of queries, pass a function to
    :meth:`~BayesDB.trace` or :meth:`~BayesDB.sql_trace`.  If you want
    finer-grained event tracing, pass an instance of this interface.

    A successful execution of a BayesDB query goes through the
    following stages:

    0. Not started
    1. Preparing cursor
    2. Result available
    3. All results consumed

    Preparing the cursor and consuming results may both be fast or
    slow, and may succeed or fail, depending on the query.  Also, the
    client may abandon some queries without consuming all the results.

    Thus, a query may experience the following transitions:

    - start: 0 --> 1
    - ready: 1 --> 2
    - error: 1 --> 0 or 2 --> 0
    - finished: 2 --> 3
    - abandoned: 2 --> 0 or 3 --> 0

    To receive notifications of any of those events for BQL or SQL
    queries, override the corresponding method(s) of this interface,
    and install the tracer object using :meth:`~BayesDB.trace` or
    :meth:`~BayesDB.sql_trace` respectively.

    Note 1: The client may run multiple cursors at the same time, so
    queries in the "Result available" state may overlap.

    Note 2: Abandonment of a query is detected when the cursor object
    is garbage collected, so the timing cannot be relied upon.

    """

    def start(self, qid, query, bindings):
        """Called when a query is started.

        The arguments are a unique query id, the query string, and the
        tuple or dictionary of bindings.

        """
        pass

    def ready(self, qid, cursor):
        """Called when a query is ready for consumption of results.

        The arguments are the query id and the BayesDB cursor.

        Note for garbage collector wonks: the passed `cursor` is the
        one wrapped in the :class:`~TracingCursor`, not the
        TracingCursor instance itself, so a tracer retaining a
        reference to `cursor` will not create a reference cycle or
        prevent the :meth:`abandoned` method from being called.

        """
        pass

    def error(self, qid, e):
        """Called when query preparation or result consumption fails.

        The arguments are the query id and the exception object.

        """
        pass

    def finished(self, qid):
        """Called when all query results are consumed."""
        pass

    def abandoned(self, qid):
        """Called when a query is abandoned.

        This is detected when the cursor object is garbage collected,
        so its timing cannot be relied upon.

        """
        pass

class TracingCursor(object):
    """Cursor wrapper for tracing interaction with an underlying cursor."""
    def __init__(self, tracer, qid, cursor):
        self._tracer = tracer
        self._qid = qid
        self._cursor = cursor

    def __iter__(self):
        return self

    def next(self):
        try:
            return self._cursor.next()
        except StopIteration:
            self._tracer.finished(self._qid)
            raise
        except Exception as e:
            self._tracer.error(self._qid, e)
            raise

    def fetchvalue(self):
        return cursor_value(self)

    def fetchone(self):
        try:
            ans = self._cursor.fetchone()
            if ans is None:
                self._tracer.finished(self._qid)
            return ans
        except Exception as e:
            self._tracer.error(self._qid, e)
            raise

    def fetchmany(self, size=1):
        try:
            ans = self._cursor.fetchmany(size=size)
            if len(ans) < size:
                self._tracer.finished(self._qid)
            return ans
        except Exception as e:
            self._tracer.error(self._qid, e)
            raise

    def fetchall(self):
        try:
            ans = self._cursor.fetchall()
            self._tracer.finished(self._qid)
            return ans
        except Exception as e:
            self._tracer.error(self._qid, e)
            raise

    @property
    def connection(self):
        return self._cursor.connection
    @property
    def lastrowid(self):
        return self._cursor.lastrowid
    @property
    def description(self):
        desc = self._cursor.description
        return [] if desc is None else desc

    def __del__(self):
        self._tracer.abandoned(self._qid)
        del self._tracer
        del self._qid
        del self._cursor
