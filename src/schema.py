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

from bayeslite.exception import BayesDBException
from bayeslite.util import cursor_value

APPLICATION_ID = 0x42594442
STALE_VERSIONS = (1,)
USABLE_VERSIONS = (5, 6, 7, 8, 9, 10,)

LATEST_VERSION = USABLE_VERSIONS[-1]

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

bayesdb_schema_6to7 = '''
PRAGMA user_version = 7;

CREATE TABLE bayesdb_session (
	id		INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
				CHECK (0 < id),
	sent	BOOLEAN DEFAULT 0,
	version	TEXT NOT NULL
);

CREATE TABLE bayesdb_session_entries (
	id		INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
				CHECK (0 < id),
	session_id	INTEGER NOT NULL REFERENCES bayesdb_session(id),
	type		TEXT CHECK (type IN ('bql','sql')) NOT NULL,
	data		TEXT NOT NULL,
	-- Timing is by the local POSIX clock.
	start_time	INTEGER NOT NULL,
	end_time	INTEGER,
	error		TEXT
);
'''

bayesdb_schema_7to8 = '''
PRAGMA user_version = 8;

CREATE TABLE bayesdb_population (
	id		INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
				CHECK (0 < id),
	name		TEXT COLLATE NOCASE NOT NULL UNIQUE,
	tabname		TEXT COLLATE NOCASE NOT NULL
				-- REFERENCES sqlite_master(name)
);

CREATE TABLE bayesdb_variable (
	population_id	INTEGER NOT NULL REFERENCES bayesdb_population(id),
	generator_id	INTEGER REFERENCES bayesdb_generator(id)
				ON DELETE CASCADE,
	colno		INTEGER NOT NULL,
	name		TEXT COLLATE NOCASE NOT NULL,
	stattype	TEXT COLLATE NOCASE NOT NULL
				REFERENCES bayesdb_stattype(name),
	PRIMARY KEY(population_id, colno),
	UNIQUE(population_id, name),
	UNIQUE(generator_id, colno),
	UNIQUE(generator_id, name)
);

INSERT INTO bayesdb_variable (population_id, name, colno, stattype)
    SELECT p.id, c.name, gc.colno, gc.stattype
        FROM bayesdb_population AS p,
            bayesdb_generator AS g,
            bayesdb_generator_column AS gc,
            bayesdb_column AS c
        WHERE p.name = g.name
            AND g.id = gc.generator_id
            AND p.tabname = c.tabname
            AND gc.colno = c.colno;

-- Adapt each existing generator to a single population.
INSERT INTO bayesdb_population (name, tabname)
    SELECT name, tabname FROM bayesdb_generator;
ALTER TABLE bayesdb_generator
    ADD COLUMN population_id INTEGER REFERENCES bayesdb_population(id);
UPDATE bayesdb_generator
    SET population_id =
        (SELECT p.id FROM bayesdb_population AS p WHERE p.tabname = tabname);
'''

bayesdb_schema_8to9 = '''
PRAGMA user_version = 9;

INSERT INTO bayesdb_stattype VALUES ('counts');
INSERT INTO bayesdb_stattype VALUES ('magnitude');
INSERT INTO bayesdb_stattype VALUES ('nominal');
INSERT INTO bayesdb_stattype VALUES ('numericalranged');
'''

bayesdb_schema_9to10 = '''
PRAGMA user_version = 10;

CREATE TABLE bayesdb_rowid_tokens(
    token        TEXT COLLATE NOCASE NOT NULL PRIMARY KEY
);

INSERT INTO bayesdb_rowid_tokens VALUES ('_rowid_');
INSERT INTO bayesdb_rowid_tokens VALUES ('rowid');
INSERT INTO bayesdb_rowid_tokens VALUES ('oid');
'''


### BayesDB SQLite setup

def bayesdb_install_schema(bdb, version=None, compatible=None):
    # Get the application id.
    cursor = bdb.sql_execute('PRAGMA application_id')
    application_id = 0
    try:
        row = cursor.next()
    except StopIteration:
        raise EnvironmentError('Missing application_id in sqlite3.')
    else:
        application_id = row[0]
        assert isinstance(application_id, int)

    # Get the user version.
    cursor = bdb.sql_execute('PRAGMA user_version')
    user_version = 0
    try:
        row = cursor.next()
    except StopIteration:
        raise EnvironmentError('Missing user_version in sqlite3.')
    else:
        user_version = row[0]
        assert isinstance(user_version, int)

    # Check them.  If zero, install schema.  If mismatch, fail.
    install = False
    if application_id == 0 and user_version == 0:
        # Assume we just created the database.
        #
        # XXX What if we opened some random other sqlite file which
        # did not have an application_id or user_version set?  Hope
        # everyone else sets application_id and user_version too...
        with bdb.transaction():
            bdb.sql_execute('PRAGMA application_id = %d' % (APPLICATION_ID,))
            bdb.sql_execute(bayesdb_schema_5)
        user_version = 5
        install = True
    elif application_id != APPLICATION_ID:
        raise IOError('Wrong application id: 0x%08x' % (application_id,))
    if user_version > LATEST_VERSION:
        raise IOError('Unknown bayeslite db version: %d' % (user_version,))
    if user_version not in USABLE_VERSIONS:
        raise IOError('Unsupported bayeslite db version: %d' % (user_version,))
    if install or not compatible:
        _upgrade_schema(bdb, user_version, desired_version=version)
    bdb.sql_execute('PRAGMA foreign_keys = ON')
    bdb.sql_execute('PRAGMA integrity_check')
    bdb.sql_execute('PRAGMA foreign_key_check')

def _upgrade_schema(bdb, current_version=None, desired_version=None):
    if current_version is None:
        with bdb.transaction():
            current_version = _schema_version(bdb)
    if desired_version is None:
        desired_version = LATEST_VERSION

    if desired_version > LATEST_VERSION:
        raise IOError('Unknown bayeslite desired version: %d' % (
            desired_version,))
    if desired_version not in USABLE_VERSIONS:
        raise IOError('Unsupported bayeslite desired version: %d' % (
            desired_version,))

    # 5 was the last prerelease version (and where we start replay for new bdbs)
    if current_version == 5 and current_version < desired_version:
        with bdb.transaction():
            bdb.sql_execute(bayesdb_schema_5to6)
        current_version = 6
    if current_version == 6 and current_version < desired_version:
        with bdb.transaction():
            bdb.sql_execute(bayesdb_schema_6to7)
        current_version = 7
    if current_version == 7 and current_version < desired_version:
        with bdb.transaction():
            bdb.sql_execute(bayesdb_schema_7to8)
        current_version = 8
    if current_version == 8 and current_version < desired_version:
        with bdb.transaction():
            bdb.sql_execute(bayesdb_schema_8to9)
        current_version = 9
    if current_version == 9 and current_version < desired_version:
        with bdb.transaction():
            bdb.sql_execute(bayesdb_schema_9to10)
        current_version = 10
    bdb.sql_execute('PRAGMA integrity_check')
    bdb.sql_execute('PRAGMA foreign_key_check')

def _schema_version(bdb):
    return cursor_value(bdb.sql_execute('PRAGMA user_version'))

def bayesdb_upgrade_schema(bdb, version=None):
    """Upgrade the BayesDB internal database schema.

    If `version` is `None`, upgrade to the latest database format
    version supported by bayeslite.  Otherwise, it may be a schema
    version number.
    """
    _upgrade_schema(bdb, current_version=None, desired_version=version)

def bayesdb_schema_version(bdb):
    """Return the version number for the BayesDB internal database schema."""
    return _schema_version(bdb)

def bayesdb_schema_required(bdb, version, why):
    """Fail if `bdb`'s internal database schema version is not new enough.

    The string `why` is included in the text of the error message,
    along with advice to use `bayesdb_upgrade_schema` to upgrade it.
    """
    current_version = bayesdb_schema_version(bdb)
    if current_version < version:
        raise BayesDBException(bdb, 'BayesDB schema version too old'
            ' to support %s: current version %r, at least %r required;'
            ' use bayesdb_upgrade_schema to upgrade.'
            % (why, current_version, version))
