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

"""SQLite3 utilities."""

import apsw
import binascii
import contextlib
import os

@contextlib.contextmanager
def sqlite3_connection(*args, **kwargs):
    """SQLite3 connection context manager.  On exit, runs close."""
    connection = apsw.Connection(*args, **kwargs)
    try:
        yield connection
    finally:
        connection.close()

@contextlib.contextmanager
def sqlite3_transaction(db):
    """Transaction context manager.  On return, commit; on exception, rollback.

    Transactions may not be nested.  Use savepoints if you want a
    nestable analogue to transactions.
    """
    db.cursor().execute("BEGIN")
    ok = False
    try:
        yield
        db.cursor().execute("COMMIT")
        ok = True
    finally:
        if not ok:
            db.cursor().execute("ROLLBACK")

@contextlib.contextmanager
def sqlite3_savepoint(db):
    """Savepoint context manager.  On return, commit; on exception, rollback.

    Savepoints are like transactions, but they may be nested in
    transactions or in other savepoints.
    """
    # This is not symmetric with sqlite3_transaction because ROLLBACK
    # undoes any effects and makes the transaction cease to be,
    # whereas ROLLBACK TO undoes any effects but leaves the savepoint
    # as is.  So for either success or failure we must release the
    # savepoint explicitly.
    savepoint = binascii.b2a_hex(os.urandom(32))
    db.cursor().execute("SAVEPOINT x%s" % (savepoint,))
    ok = False
    try:
        yield
        ok = True
    finally:
        if not ok:
            db.cursor().execute("ROLLBACK TO x%s" % (savepoint,))
        db.cursor().execute("RELEASE x%s" % (savepoint,))

@contextlib.contextmanager
def sqlite3_savepoint_rollback(db):
    """Savepoint context manager that always rolls back."""
    savepoint = binascii.b2a_hex(os.urandom(32))
    db.cursor().execute("SAVEPOINT x%s" % (savepoint,))
    try:
        yield
    finally:
        db.cursor().execute("ROLLBACK TO x%s" % (savepoint,))
        db.cursor().execute("RELEASE x%s" % (savepoint,))

def sqlite3_exec_1(db, query, *args):
    """Execute a query returning a 1x1 table, and return its one value.

    Do not call this if you cannot guarantee the result is a 1x1
    table.  Beware passing user-controlled input in here.
    """
    cursor = db.cursor().execute(query, *args)
    row = cursor.fetchone()
    assert row
    assert len(row) == 1
    assert cursor.fetchone() == None
    return row[0]

def sqlite3_quote_name(name):
    """Quote `name` as a SQL identifier, e.g. a table or column name.

    Do NOT use this for strings, e.g. inserting data into a table.
    Use query parameters instead.
    """
    # XXX Could omit quotes in some cases, but safer this way.
    return '"' + name.replace('"', '""') + '"'

# From <https://www.sqlite.org/datatype3.html#affname>.  Doesn't seem
# to be a built-in SQLite library routine to compute this.
def sqlite3_column_affinity(column_type):
    """Return the sqlite3 column affinity corresponding to a type string."""
    ct = column_type.lower()
    if "int" in ct:
        return "INTEGER"
    elif "char" in ct or "clob" in ct or "text" in ct:
        return "TEXT"
    elif "blob" in ct or ct == "":
        return "NONE"
    elif "real" in ct or "floa" in ct or "doub" in ct:
        return "REAL"
    else:
        return "NUMERIC"

### Trivial SQLite3 utility tests

# XXX This doesn't really belong here, although it doesn't hurt either.

assert sqlite3_quote_name("foo bar") == '"foo bar"'

assert sqlite3_column_affinity("integer") == "INTEGER"
assert sqlite3_column_affinity("CHARINT") == "INTEGER"
assert sqlite3_column_affinity("INT") == "INTEGER"
assert sqlite3_column_affinity("INTEGER") == "INTEGER"
assert sqlite3_column_affinity("TINYINT") == "INTEGER"
assert sqlite3_column_affinity("SMALLINT") == "INTEGER"
assert sqlite3_column_affinity("MEDIUMINT") == "INTEGER"
assert sqlite3_column_affinity("BIGINT") == "INTEGER"
assert sqlite3_column_affinity("UNSIGNED BIG INT") == "INTEGER"
assert sqlite3_column_affinity("INT2") == "INTEGER"
assert sqlite3_column_affinity("INT8") == "INTEGER"
assert sqlite3_column_affinity("FLOATING POINT") == "INTEGER"

assert sqlite3_column_affinity("text") == "TEXT"
assert sqlite3_column_affinity("TEXT") == "TEXT"
assert sqlite3_column_affinity("CHARACTER(20)") == "TEXT"
assert sqlite3_column_affinity("VARCHAR(255)") == "TEXT"
assert sqlite3_column_affinity("VARYING CHARACTER(255)") == "TEXT"
assert sqlite3_column_affinity("NCHAR(55)") == "TEXT"
assert sqlite3_column_affinity("NATIVE CHARACTER(70)") == "TEXT"
assert sqlite3_column_affinity("NVARCHAR(100)") == "TEXT"
assert sqlite3_column_affinity("TEXT") == "TEXT"
assert sqlite3_column_affinity("CLOB") == "TEXT"
assert sqlite3_column_affinity("CLOBBER") == "TEXT"

assert sqlite3_column_affinity("blob") == "NONE"
assert sqlite3_column_affinity("BLOB") == "NONE"
assert sqlite3_column_affinity("AMBLOBORIC") == "NONE"
assert sqlite3_column_affinity("") == "NONE"

assert sqlite3_column_affinity("real") == "REAL"
assert sqlite3_column_affinity("REAL") == "REAL"
assert sqlite3_column_affinity("DOUBLE") == "REAL"
assert sqlite3_column_affinity("DOUBLE PRECISION") == "REAL"
assert sqlite3_column_affinity("FLOAT") == "REAL"

assert sqlite3_column_affinity("numeric") == "NUMERIC"
assert sqlite3_column_affinity("MAGICAL MYSTERY TYPE") == "NUMERIC"
assert sqlite3_column_affinity("NUMERIC") == "NUMERIC"
assert sqlite3_column_affinity("DECIMAL(10,5)") == "NUMERIC"
assert sqlite3_column_affinity("BOOLEAN") == "NUMERIC"
assert sqlite3_column_affinity("DATE") == "NUMERIC"
assert sqlite3_column_affinity("DATETIME") == "NUMERIC"
assert sqlite3_column_affinity("STRING") == "NUMERIC"
