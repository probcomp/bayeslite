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

from bayeslite.sqlite3_util import sqlite3_exec_1

bayesdb_schema_5 = '''
PRAGMA user_version = 5;

CREATE TABLE bayesdb_metamodel (
	name		TEXT COLLATE NOCASE NOT NULL PRIMARY KEY,
	version		INTEGER NOT NULL
);

-- Statistics type.
CREATE TABLE bayesdb_stattype (
	name		TEXT COLLATE NOCASE NOT NULL PRIMARY KEY
);

INSERT INTO bayesdb_stattype VALUES ('categorical');
INSERT INTO bayesdb_stattype VALUES ('cyclic');
INSERT INTO bayesdb_stattype VALUES ('numerical');

CREATE TABLE bayesdb_column (
	tabname		TEXT COLLATE NOCASE NOT NULL,
	colno		INTEGER NOT NULL CHECK (0 <= colno),
	name		TEXT COLLATE NOCASE NOT NULL,
	shortname	TEXT,
	description	TEXT,
	PRIMARY KEY(tabname, colno),
	UNIQUE(tabname, name)
);

CREATE TABLE bayesdb_column_map (
	tabname		TEXT COLLATE NOCASE NOT NULL,
	colno		INTEGER NOT NULL,
	key		TEXT NOT NULL,
	value		TEXT NOT NULL,
	PRIMARY KEY(tabname, colno, key),
	FOREIGN KEY(tabname, colno) REFERENCES bayesdb_column(tabname, colno)
);

CREATE TABLE bayesdb_generator (
	-- We use AUTOINCREMENT so that generator id numbers don't get
	-- reused and are safe to hang onto outside a transaction.
	id		INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
				CHECK (0 < id),
	name		TEXT COLLATE NOCASE NOT NULL UNIQUE,
	tabname		TEXT COLLATE NOCASE NOT NULL,
				-- REFERENCES sqlite_master(name)
	metamodel	INTEGER NOT NULL REFERENCES bayesdb_metamodel(name)
);

CREATE TABLE bayesdb_generator_column (
	generator_id	INTEGER NOT NULL REFERENCES bayesdb_generator(id),
	colno		INTEGER NOT NULL,
	stattype	TEXT NOT NULL REFERENCES bayesdb_stattype(name),
	PRIMARY KEY(generator_id, colno)
);

CREATE TABLE bayesdb_generator_model (
	generator_id	INTEGER NOT NULL REFERENCES bayesdb_generator(id),
	modelno		INTEGER NOT NULL,
	iterations	INTEGER NOT NULL CHECK (0 <= iterations),
	PRIMARY KEY(generator_id, modelno)
);
'''

bayesdb_schema_5to6 = '''
PRAGMA user_version = 6;

ALTER TABLE bayesdb_generator
    ADD COLUMN defaultp BOOLEAN DEFAULT 0;

CREATE UNIQUE INDEX bayesdb_generator_i_default ON bayesdb_generator (tabname)
    WHERE defaultp;
'''

### BayesDB SQLite setup

def bayesdb_install_schema(db):
    # Get the application id.
    cursor = db.execute('PRAGMA application_id')
    application_id = 0
    try:
        row = cursor.next()
    except StopIteration:
        raise EnvironmentError('Missing application_id in sqlite3.')
    else:
        application_id = row[0]
        assert isinstance(application_id, int)

    # Get the user version.
    cursor = db.execute('PRAGMA user_version')
    user_version = 0
    try:
        row = cursor.next()
    except StopIteration:
        raise EnvironmentError('Missing user_version in sqlite3.')
    else:
        user_version = row[0]
        assert isinstance(user_version, int)

    # Check them.  If zero, install schema.  If mismatch, fail.
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
            db.executescript('''
                BEGIN;
                PRAGMA application_id = %d;
                %s;
                COMMIT;
            ''' % (0x42594442, bayesdb_schema_5))
            user_version = 5
    elif application_id != 0x42594442:
        raise IOError('Wrong application id: 0x%08x' % (application_id,))
    if user_version == 5:
        # XXX One of these days, we'll have to consider making the
        # upgrade something to be explicitly requested by the user
        # when old versions persist.
        db.executescript('BEGIN; %s; COMMIT;' % (bayesdb_schema_5to6,))
        user_version = 6
    if user_version != 6:
        raise IOError('Unknown bayeslite format version: %d' % (user_version,))
    db.execute('PRAGMA foreign_keys = ON')
    db.execute('PRAGMA integrity_check')
    db.execute('PRAGMA foreign_key_check')
