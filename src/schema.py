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

bayesdb_schema = """
PRAGMA foreign_keys = ON;
PRAGMA application_id = 1113146434; -- #x42594442, `BYDB'
PRAGMA user_version = 1;

BEGIN;
CREATE TABLE bayesdb_engine (
	id		INTEGER NOT NULL UNIQUE PRIMARY KEY CHECK (id >= 0),
	name		TEXT NOT NULL UNIQUE
);

CREATE UNIQUE INDEX bayesdb_engine_by_name ON bayesdb_engine (name);

INSERT INTO bayesdb_engine (id, name) VALUES (0, 'crosscat');

CREATE TABLE bayesdb_table (
	id		INTEGER NOT NULL UNIQUE PRIMARY KEY CHECK (id >= 0),
	name		TEXT NOT NULL UNIQUE, -- REFERENCES sqlite_master(name)
	metadata	BLOB NOT NULL
);

CREATE UNIQUE INDEX bayesdb_table_by_name ON bayesdb_table (name);

CREATE TABLE bayesdb_table_column (
	id		INTEGER NOT NULL PRIMARY KEY CHECK (id >= 0),
	table_id	INTEGER NOT NULL REFERENCES bayesdb_table(id),
	name		TEXT NOT NULL,
	colno		INTEGER NOT NULL
);

CREATE UNIQUE INDEX bayesdb_table_column_by_name ON bayesdb_table_column
	(table_id, name);

CREATE UNIQUE INDEX bayesdb_table_column_by_number ON bayesdb_table_column
	(table_id, colno);

-- XXX Include the engine in the primary key?
CREATE TABLE bayesdb_model (
	table_id	INTEGER NOT NULL REFERENCES bayesdb_table(id),
	modelno		INTEGER NOT NULL CHECK (modelno >= 0),
	engine_id	INTEGER NOT NULL REFERENCES bayesdb_engine(id),
	theta		BLOB NOT NULL,
	PRIMARY KEY (table_id, modelno)
);
COMMIT;
"""

### BayesDB SQLite setup

def bayesdb_install_schema(db):
    # XXX Check the engine too, and/or add support for multiple
    # simultaneous engines.
    application_id = sqlite3_exec_1(db, "PRAGMA application_id")
    user_version = sqlite3_exec_1(db, "PRAGMA user_version")
    if application_id == 0 and user_version == 0:
        # Assume we just created the database.
        #
        # XXX What if we opened some random other sqlite file which
        # did not have an application_id or user_version set?  Hope
        # everyone else sets application_id and user_version too...
        #
        # XXX Idiotic Python sqlite3 module has no way to execute a
        # string with multiple SQL commands that doesn't muck with the
        # application's transactions -- db.executescript("...") will
        # issue a COMMIT first, if there is a transaction pending, so
        # we can't just write
        #
        #   with sqlite3_transaction(db):
        #       db.executescript(bayesdb_schema)
        #
        # Instead, we abuse the use of sqlite database connections as
        # context managers that commit/rollback if there is a
        # transaction active.  Otherwise we make no use of the sqlite3
        # module's automatic transaction handling.
        with db:
            db.executescript(bayesdb_schema)
        assert sqlite3_exec_1(db, "PRAGMA application_id") == 0x42594442
        assert sqlite3_exec_1(db, "PRAGMA user_version") == 1
    elif application_id != 0x42594442:
        raise IOError("Invalid application_id: 0x%08x" % application_id)
    elif user_version != 1:
        raise IOError("Unknown database version: %d" % user_version)

def sqlite3_exec_1(db, query, *args):
    """Execute a query returning a 1x1 table, and return its one value."""
    cursor = db.execute(query, *args)
    row = cursor.fetchone()
    assert row
    assert len(row) == 1
    assert cursor.fetchone() == None
    return row[0]
