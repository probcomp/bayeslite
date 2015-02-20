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

import apsw

from bayeslite.sqlite3_util import sqlite3_exec_1

# Pragmas that must go outside any transaction.
bayesdb_pragmas = """
PRAGMA foreign_keys = ON;
"""

bayesdb_schema_3 = """
PRAGMA application_id = 1113146434; -- #x42594442, `BYDB'
PRAGMA user_version = 3;

CREATE TABLE bayesdb_metamodel (
	id		INTEGER NOT NULL UNIQUE PRIMARY KEY CHECK (id >= 0),
	name		TEXT COLLATE NOCASE NOT NULL UNIQUE
);

CREATE TABLE bayesdb_table (
	id		INTEGER NOT NULL UNIQUE PRIMARY KEY CHECK (id >= 0),
	name		TEXT COLLATE NOCASE NOT NULL UNIQUE,
				-- REFERENCES sqlite_master(name)
	metamodel_id	INTEGER NOT NULL REFERENCES bayesdb_metamodel(id),
	metadata	BLOB NOT NULL
);

CREATE TABLE bayesdb_table_column (
	id		INTEGER NOT NULL PRIMARY KEY CHECK (id >= 0),
	table_id	INTEGER NOT NULL REFERENCES bayesdb_table(id),
	name		TEXT COLLATE NOCASE NOT NULL,
	colno		INTEGER NOT NULL,
	UNIQUE (table_id, name),
	UNIQUE (table_id, colno)
);

CREATE TABLE bayesdb_model (
	table_id	INTEGER NOT NULL REFERENCES bayesdb_table(id),
	modelno		INTEGER NOT NULL CHECK (modelno >= 0),
	theta		BLOB NOT NULL,
	PRIMARY KEY (table_id, modelno)
);
"""

bayesdb_schema_3to4 = """
PRAGMA user_version = 4;

ALTER TABLE bayesdb_table_column ADD COLUMN short_name TEXT;
ALTER TABLE bayesdb_table_column ADD COLUMN description TEXT;

CREATE TABLE bayesdb_value_map (
	table_id	INTEGER NOT NULL REFERENCES bayesdb_table(id),
	colno		INTEGER NOT NULL,
	value		TEXT NOT NULL,
	extended_value	TEXT NOT NULL,
	PRIMARY KEY(table_id, colno, value),
	FOREIGN KEY(table_id, colno)
		REFERENCES bayesdb_table_column(table_id, colno)
);
"""

### BayesDB SQLite setup

def bayesdb_install_schema(db):
    # Find the current application id and user version.
    application_id = 0
    application_id_ok = None
    fixup_application_id = False
    if list(db.cursor().execute("PRAGMA application_id")):
        application_id = sqlite3_exec_1(db, "PRAGMA application_id")
        application_id_ok = True
    else:
        # raise Warning('SQLite is too old!')
        application_id_ok = False
    user_version = sqlite3_exec_1(db, "PRAGMA user_version")

    # Check them.  If zero, install the schema.  If not, maybe fail.
    if application_id == 0 and user_version == 0:
        # Assume we just created the database.
        #
        # XXX What if we opened some random other sqlite file which
        # did not have an application_id or user_version set?  Hope
        # everyone else sets application_id and user_version too...
        #
        # XXX We rely on APSW's support for multiple statements per
        # cursor execution.  If this were the builtin Python sqlite3
        # module, we'd have to use db.executescript (and a kludgey
        # workaround for the transactional semantics).
        db.cursor().execute(bayesdb_pragmas)
        with db:
            db.cursor().execute(bayesdb_schema_3)
            db.cursor().execute(bayesdb_schema_3to4)
        if application_id_ok:
            assert sqlite3_exec_1(db, "PRAGMA application_id") == 0x42594442
        return
    elif application_id == 0 and \
         list(db.cursor().execute("PRAGMA table_info(bayesdb_table)")):
        # Assume we created it on a system with no application_id
        # support, and just fix that up if we have it now.
        if application_id_ok:
            fixup_application_id = True
    else:
        raise IOError("Invalid application_id: 0x%08x" % (application_id,))
    # Check the schema version and apply upgrades if necessary.
    if user_version == 3:
        with db:
            db.cursor().execute(bayesdb_schema_3to4)
    elif user_version == 4:
        pass
    else:
        raise IOError("Unknown database version: %d" % (user_version,))
        pass
    if fixup_application_id:
        db.cursor().execute("PRAGMA application_id = 1113146434")
    if application_id_ok:
        assert sqlite3_exec_1(db, "PRAGMA application_id") == 0x42594442
    assert sqlite3_exec_1(db, "PRAGMA user_version") == 4
