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

# pylint: disable=star-args

# Implementation notes:
#
# - Use SQL parameters to pass strings and other values into SQL.
#   Don't use %.
#
#      DO:      db.execute("UPDATE foo SET x = ? WHERE id = ?", (x, id))
#      DON'T:   db.execute("UPDATE foo SET x = '%s' WHERE id = %d" % (x, id))
#
# - Use sqlite3_quote_name and % to make SQL queries that refer to
#   tables, columns, &c.
#
#      DO:      qt = sqlite3_quote_name(table)
#               qc = sqlite3_quote_name(column)
#               db.execute("SELECT %s FROM %s WHERE x = ?" % (qt, qc), (x,))
#      DON'T:   db.execute("SELECT %s FROM %s WHERE x = ?" % (column, table),
#                   (x,))

import contextlib
import csv
import json
import math
import scipy.stats              # pearsonr, chi2_contingency, f_oneway
                                # (For CORRELATION OF <col0> WITH <col1> only.)
import sqlite3

bayesdb_type_table = [
    # column type, numerical?, default sqlite, default model type
    ("categorical",     False,  "text",         "symmetric_dirichlet_discrete"),
    ("cyclic",          True,   "real",         "vonmises"),
    ("key",             False,  "text",         None),
    ("numerical",       True,   "real",         "normal_inverse_gamma"),
]

# XXX What about other model types from the paper?
#
# asymmetric_beta_bernoulli
# pitmanyor_atom
# poisson_gamma
#
# XXX Upgrade column types:
#       continuous -> numerical
#       multinomial -> categorical.

### BayesDB class

class BayesDB(object):
    """Class of Bayesian databases.

    Interface is loosely based on PEP-249 DB-API.
    """

    def __init__(self, engine, pathname=":memory:"):
        self.engine = engine
        # isolation_level=None actually means that the sqlite3 module
        # will not randomly begin and commit transactions where we
        # didn't ask it to.
        self.sqlite = sqlite3.connect(pathname, isolation_level=None)
        bayesdb_install_schema(self.sqlite)
        bayesdb_install_bql(self.sqlite, self)

    def close(self):
        """Close the database.  Further use is not allowed."""
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

### BayesDB SQLite setup

# XXX Temporary location for the schema.  Move this somewhere else!
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

def bayesdb_install_bql(db, cookie):
    def function(name, nargs, fn):
        db.create_function(name, nargs,
            lambda *args: bayesdb_bql(fn, cookie, *args))
    function("column_correlation", 3, bayesdb_column_correlation)
    function("column_dependence_probability", 3,
        bayesdb_column_dependence_probability)
    function("column_mutual_information", 3, bayesdb_column_mutual_information)
    function("column_typicality", 2, bayesdb_column_typicality)
    function("column_value_probability", 3, bayesdb_column_value_probability)
    function("row_similarity", 4, bayesdb_row_similarity)
    function("row_typicality", 2, bayesdb_row_typicality)
    function("row_column_predictive_probability", 2,
        bayesdb_row_column_predictive_probability)

# XXX XXX XXX Temporary debugging kludge!
import traceback

def bayesdb_bql(fn, cookie, *args):
    try:
        return fn(cookie, *args)
    except Exception as e:
        print traceback.format_exc()
        raise e

### Importing SQLite tables

# XXX Use URIs, and attach read-only?
def bayesdb_attach_sqlite_file(bdb, name, pathname):
    """Attach the SQLite database file at PATHNAME under the name NAME."""
    # Turns out Python urlparse is broken and can't handle relative
    # pathnames.  Urgh.
    #uri = urlparse.urlunparse("file", "", urllib.quote(pathname), "", "", "")
    bdb.sqlite.execute("ATTACH DATABASE ? AS %s" % (name,), pathname)

# def bayesdb_attach_sqlite_uri(bdb, name, uri):
#     ...

# XXX Accept parameters for guessing column types: count_cutoff &c.
#
# XXX Allow ignored columns?
def bayesdb_import_sqlite_table(bdb, table,
        column_names=None, column_types=None):
    """Import a SQLite table for use in a BayesDB with BQL.

    COLUMN_NAMES is a list specifying the desired order and selection
    of column names for the BQL table.  COLUMN_TYPES is a dict mapping
    the selected column names to their BQL column types.
    """
    column_names, column_types = bayesdb_determine_columns(bdb, table,
        column_names, column_types)
    metadata = bayesdb_create_metadata(bdb, table, column_names, column_types)
    metadata_json = json.dumps(metadata)
    with sqlite3_savepoint(bdb.sqlite):
        table_sql = "INSERT INTO bayesdb_table (name, metadata) VALUES (?, ?)"
        table_cursor = bdb.sqlite.execute(table_sql, (table, metadata_json))
        table_id = table_cursor.lastrowid
        assert table_id is not None
        for colno, name in enumerate(column_names):
            column_sql = """
                INSERT INTO bayesdb_table_column (table_id, name, colno)
                VALUES (?, ?, ?)
            """
            bdb.sqlite.execute(column_sql, (table_id, name, colno))

def bayesdb_determine_columns(bdb, table, column_names, column_types):
    qt = sqlite3_quote_name(table)
    cursor = bdb.sqlite.execute("PRAGMA table_info(%s)" % (qt,))
    column_descs = cursor.fetchall()
    if column_names is None:
        column_names = [name for _i, name, _t, _n, _d, _p in column_descs]
        column_name_set = set(column_names)
    else:
        all_names = set(name for _i, name, _t, _n, _d, _p in column_descs)
        for name in column_names:
            if name not in all_names:
                raise ValueError("Unknown column: %s" % (name,))
    column_name_set = set(column_names)
    if column_types is None:
        column_types = dict(bayesdb_guess_column(bdb, table, desc)
            for desc in column_descs if desc[1] in column_name_set)
    else:
        for name in column_types:
            if name not in column_name_set:
                raise ValueError("Unknown column: %s" % (name,))
        for name in column_name_set:
            if name not in column_types:
                raise ValueError("Unknown column: %s" % (name,))
    key = None
    for name in column_types:
        if column_types[name] == "key":
            if key is None:
                key = name
            else:
                raise ValueError("More than one key: %s, %s" % (key, name))
    if key is None:
        # XXX Find some way to call it `key', instead of `rowid'?  And
        # make it start at 0 instead of 1...?
        #
        # XXX Quote rowid?  What a pain...  (Could use any of rowid,
        # _rowid_, or oid in sqlite, but they can all be shadowed.)
        assert "rowid" not in column_types
        column_names = ["rowid"] + column_names
        column_types = column_types.copy()
        column_types["rowid"] = "key"
    return column_names, column_types

# XXX Pass count_cutoff/ratio_cutoff through from above.
def bayesdb_guess_column(bdb, table, column_desc,
        count_cutoff=20, ratio_cutoff=0.02):
    (_cid, column_name, _sql_type, _nonnull, _default, primary_key) = \
        column_desc
    if primary_key:
        return (column_name, "key")
    # XXX Use sqlite column type as a heuristic?  Won't help for CSV.
    qt = sqlite3_quote_name(table)
    qcn = sqlite3_quote_name(column_name)
    ndistinct_sql = "SELECT COUNT(DISTINCT %s) FROM %s" % (qcn, qt)
    ndistinct = sqlite3_exec_1(bdb.sqlite, ndistinct_sql)
    if ndistinct <= count_cutoff:
        return (column_name, "categorical")
    ndata_sql = "SELECT COUNT(%s) FROM %s" % (qcn, qt)
    ndata = sqlite3_exec_1(bdb.sqlite, ndata_sql)
    if (float(ndistinct) / float(ndata)) <= ratio_cutoff:
        return (column_name, "categorical")
    if not bayesdb_column_floatable_p(bdb, table, column_desc):
        return (column_name, "categorical")
    return (column_name, "numerical")

# XXX This is a kludge!
def bayesdb_column_floatable_p(bdb, table, column_desc):
    (_cid, column_name, _sql_type, _nonnull, _default, _primary_key) = \
        column_desc
    qt = sqlite3_quote_name(table)
    qcn = sqlite3_quote_name(column_name)
    sql = "SELECT %s FROM %s WHERE %s IS NOT NULL" % (qcn, qt, qcn)
    cursor = bdb.sqlite.execute(sql)
    try:
        for row in cursor:
            float(row[0])
    except ValueError:
        return False
    return True

def bayesdb_create_metadata(bdb, table, column_names, column_types):
    ncols = len(column_names)
    assert ncols == len(column_types)
    return {
        "name_to_idx": dict(zip(column_names, range(ncols))),
        "idx_to_name": dict(zip(map(str, range(ncols)), column_names)),
        "column_metadata":
            [metadata_generators[column_types[name]](bdb, table, name)
                for name in column_names],
    }

def bayesdb_metadata_numerical(_bdb, _table, _column_name):
    return {
        "modeltype": "normal_inverse_gamma",
        "value_to_code": {},
        "code_to_value": {},
    }

def bayesdb_metadata_cyclic(_bdb, _table, _column_name):
    return {
        "modeltype": "vonmises",
        "value_to_code": {},
        "code_to_value": {},
    }

def bayesdb_metadata_ignore(bdb, table, column_name):
    metadata = bayesdb_metadata_categorical(bdb, table, column_name)
    metadata["modeltype"] = "ignore"
    return metadata

def bayesdb_metadata_key(bdb, table, column_name):
    metadata = bayesdb_metadata_categorical(bdb, table, column_name)
    metadata["modeltype"] = "key"
    return metadata

def bayesdb_metadata_categorical(bdb, table, column_name):
    qcn = sqlite3_quote_name(column_name)
    qt = sqlite3_quote_name(table)
    sql = """
        SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL ORDER BY %s
    """ % (qcn, qt, qcn, qcn)
    cursor = bdb.sqlite.execute(sql)
    codes = [row[0] for row in cursor]
    ncodes = len(codes)
    return {
        "modeltype": "symmetric_dirichlet_discrete",
        "value_to_code": dict(zip(range(ncodes), codes)),
        "code_to_value": dict(zip(codes, range(ncodes))),
    }

metadata_generators = {
    "numerical": bayesdb_metadata_numerical,
    "cyclic": bayesdb_metadata_cyclic,
    "ignore": bayesdb_metadata_ignore,   # XXX Why any metadata here?
    "key": bayesdb_metadata_categorical, # XXX Why any metadata here?
    "categorical": bayesdb_metadata_categorical,
}

### Importing CSV tables

def bayesdb_read_csv_with_header(pathname):
    with open(pathname, "rU") as f:
        reader = csv.reader(f)
        column_names = reader.next()
        ncols = len(column_names)
        # XXX Can we get the CSV reader to strip for us?
        rows = [[v.strip() for v in row] for row in reader]
        for row in rows:
            if len(row) != ncols:
                raise IOError("Mismatched number of columns")
        return column_names, rows

# XXX Is this the schema language we want?
def bayesdb_import_csv_file(bdb, table, pathname, column_types=None):
    # XXX Strip ignored columns.
    # XXX Limit the number of rows.
    column_names, rows = bayesdb_read_csv_with_header(pathname)
    if column_types is None:
        column_types = bayesdb_csv_guess_column_types(column_names, rows)
    else:
        if len(column_types) != len(column_names):
            raise IOError("CSV file has %d columns, expected %d" %
                (len(column_names), len(column_types)))
        for name in column_names:
            if name not in column_types:
                # XXX Ignore this column?  Treat as numerical?  Infer?
                raise IOError("CSV file has unknown column: %s" % (name,))
        column_name_set = set(column_names)
        for name in column_types:
            if name not in column_name_set:
                raise IOError("Column not in CSV file: %s" % (name,))
    ncols = len(column_names)
    assert ncols == len(column_types)
    qt = sqlite3_quote_name(table)
    table_def = bayesdb_table_definition(table, column_names, column_types)
    with sqlite3_savepoint(bdb.sqlite):
        bdb.sqlite.execute(table_def)
        qcns = ",".join(map(sqlite3_quote_name, column_names))
        qcps = ",".join("?" * ncols)
        insert_sql = "INSERT INTO %s (%s) VALUES (%s)" % (qt, qcns, qcps)
        bdb.sqlite.executemany(insert_sql, rows)
        bayesdb_import_sqlite_table(bdb, table, column_names, column_types)

def bayesdb_table_definition(table, column_names, column_types):
    column_defs = [bayesdb_column_definition(name, column_types[name])
        for name in column_names]
    qt = sqlite3_quote_name(table)
    return ("CREATE TABLE %s (%s)" % (qt, ",".join(column_defs)))

bayesdb_column_type_to_sqlite_type = \
    dict((ct, sql) for ct, _cont_p, sql, _mt in bayesdb_type_table)
def bayesdb_column_definition(column_name, column_type):
    qcn = sqlite3_quote_name(column_name)
    sqlite_type = bayesdb_column_type_to_sqlite_type[column_type]
    qualifiers = []
    if column_type == "key":
        # XXX SQLite3 quirk: PRIMARY KEY does not imply NOT NULL.
        qualifiers.append("NOT NULL PRIMARY KEY")
    separator = " " if qualifiers else ""
    return ("%s %s%s%s" % (qcn, sqlite_type, separator, " ".join(qualifiers)))

#### CSV column heuristicking

# XXX This logic should not be duplicated for importing CSV tables vs
# importing SQL tables.  However, we need to make slightly different
# decisions for the two cases.  Blah blah blah blah blah.

def bayesdb_csv_guess_column_types(column_names, rows):
    column_types = {}
    need_key = True
    for i, name in enumerate(column_names):
        column_types[name] = bayesdb_csv_guess_column_type(rows, i, need_key)
        if column_types[name] == "key":
            need_key = False
    return column_types

# XXX Pass count_cutoff/ratio_cutoff through from above.
def bayesdb_csv_guess_column_type(rows, i, may_be_key,
        count_cutoff=20, ratio_cutoff=0.02):
    if may_be_key and bayesdb_csv_column_keyable_p(rows, i):
        return "key"
    elif bayesdb_csv_column_numerical_p(rows, i, count_cutoff, ratio_cutoff):
        return "numerical"
    else:
        return "categorical"

def bayesdb_csv_column_keyable_p(rows, i):
    if bayesdb_csv_column_integerable_p(rows, i):
        return len(rows) == len(unique([int(row[i]) for row in rows]))
    elif not bayesdb_csv_column_floatable_p(rows, i):
        # XXX Is str(...) necessary?  I think they should all be
        # strings here.  Where can stripping happen?
        return len(rows) == len(unique([str(row[i]).strip() for row in rows]))
    else:
        return False

def bayesdb_csv_column_integerable_p(rows, i):
    try:
        for row in rows:
            if str(row[i]) != str(int(row[i])):
                return False
    except ValueError:
        return False
    return True

def bayesdb_csv_column_floatable_p(rows, i):
    try:
        for row in rows:
            float(row[i])
    except ValueError:
        return False
    return True

def bayesdb_csv_column_numerical_p(rows, i, count_cutoff, ratio_cutoff):
    if not bayesdb_csv_column_floatable_p(rows, i):
        return False
    ndistinct = len(unique([float(row[i]) for row in rows
        if not math.isnan(float(row[i]))]))
    if ndistinct <= count_cutoff:
        return False
    ndata = len(rows)
    if (float(ndistinct) / float(ndata)) <= ratio_cutoff:
        return False
    return True

### BayesDB data/metadata access

def bayesdb_data(bdb, table_id):
    M_c = bayesdb_metadata(bdb, table_id)
    table = bayesdb_table_name(bdb, table_id)
    column_names = list(bayesdb_column_names(bdb, table_id))
    qt = sqlite3_quote_name(table)
    qcns = ",".join(map(sqlite3_quote_name, column_names))
    sql = "SELECT %s FROM %s" % (qcns, qt)
    for row in bdb.sqlite.execute(sql):
        yield tuple(bayesdb_value_to_code(M_c, i, v)
            for i, v in enumerate(row))

# XXX Cache this?
def bayesdb_metadata(bdb, table_id):
    sql = "SELECT metadata FROM bayesdb_table WHERE id = ?"
    data = sqlite3_exec_1(bdb.sqlite, sql, (table_id,))
    return json.loads(data)

def bayesdb_table_name(bdb, table_id):
    sql = "SELECT name FROM bayesdb_table WHERE id = ?"
    return sqlite3_exec_1(bdb.sqlite, sql, (table_id,))

def bayesdb_column_names(bdb, table_id):
    sql = """
        SELECT name FROM bayesdb_table_column WHERE table_id = ? ORDER BY colno
    """
    for row in bdb.sqlite.execute(sql, (table_id,)):
        yield row[0]

def bayesdb_column_name(bdb, table_id, colno):
    sql = """
        SELECT name FROM bayesdb_table_column WHERE table_id = ? AND colno = ?
    """
    return sqlite3_exec_1(bdb.sqlite, sql, (table_id, colno))

def bayesdb_column_values(bdb, table_id, colno):
    qt = sqlite3_quote_name(bayesdb_table_name(bdb, table_id))
    qc = sqlite3_quote_name(bayesdb_column_name(bdb, table_id, colno))
    for row in bdb.sqlite.execute("SELECT %s FROM %s" % (qc, qt)):
        yield row[0]

def bayesdb_cell_value(bdb, table_id, row_id, colno):
    qt = sqlite3_quote_name(bayesdb_table_name(bdb, table_id))
    qc = sqlite3_quote_name(bayesdb_column_name(bdb, table_id, colno))
    sql = "SELECT %s FROM %s WHERE rowid = ?" % (qc, qt)
    return sqlite3_exec_1(bdb.sqlite, sql, (row_id,))

### BayesDB model access

def bayesdb_init_model(bdb, table_id, modelno, engine_id, theta):
    insert_sql = """
        INSERT INTO bayesdb_model (table_id, modelno, engine_id, theta)
        VALUES (?, ?, ?, ?)
    """
    bdb.sqlite.execute(insert_sql,
        (table_id, modelno, engine_id, json.dumps(theta)))

def bayesdb_set_model(bdb, table_id, modelno, theta):
    sql = """
        UPDATE bayesdb_model SET theta = ? WHERE table_id = ? AND modelno = ?
    """
    bdb.sqlite.execute(sql, (json.dumps(theta), table_id, modelno))

def bayesdb_models(bdb, table_id):
    sql = "SELECT theta FROM bayesdb_model WHERE table_id = ? ORDER BY modelno"
    for row in bdb.sqlite.execute(sql, (table_id,)):
        yield json.loads(row[0])

def bayesdb_model(bdb, table_id, modelno):
    sql = """
        SELECT theta FROM bayesdb_model WHERE table_id = ? AND modelno = ?
    """
    return json.loads(sqlite3_exec_1(bdb.sqlite, sql, (table_id, modelno)))

# XXX Silly name.
def bayesdb_latent_stata(bdb, table_id):
    for model in bayesdb_models(bdb, table_id):
        yield (model["X_L"], model["X_D"])

def bayesdb_latent_state(bdb, table_id):
    for model in bayesdb_models(bdb, table_id):
        yield model["X_L"]

def bayesdb_latent_data(bdb, table_id):
    for model in bayesdb_models(bdb, table_id):
        yield model["X_D"]

### BayesDB model commands

def bayesdb_models_initialize(bdb, table_id, nmodels, model_config=None):
    assert model_config is None         # XXX For now.
    assert 0 < nmodels
    engine_sql = "SELECT id FROM bayesdb_engine WHERE name = ?"
    engine_id = sqlite3_exec_1(bdb.sqlite, engine_sql, ("crosscat",)) # XXX
    model_config = {
        "kernel_list": (),
        "initialization": "from_the_prior",
        "row_initialization": "from_the_prior",
    }
    X_L_list, X_D_list = bdb.engine.initialize(
        M_c=bayesdb_metadata(bdb, table_id),
        M_r=None,            # XXX
        T=list(bayesdb_data(bdb, table_id)),
        n_chains=nmodels,
        initialization=model_config["initialization"],
        row_initialization=model_config["row_initialization"]
    )
    if nmodels == 1:            # XXX Ugh.  Fix crosscat so it doesn't do this.
        X_L_list = [X_L_list]
        X_D_list = [X_D_list]
    with sqlite3_savepoint(bdb.sqlite):
        for modelno, (X_L, X_D) in enumerate(zip(X_L_list, X_D_list)):
            theta = {
                "X_L": X_L,
                "X_D": X_D,
                "iterations": 0,
                "column_crp_alpha": [],
                "logscore": [],
                "num_views": [],
                "model_config": model_config,
            }
            bayesdb_init_model(bdb, table_id, modelno, engine_id, theta)

# XXX Background, deadline, &c.
def bayesdb_models_analyze1(bdb, table_id, modelno, iterations=1):
    assert 0 <= iterations
    theta = bayesdb_model(bdb, table_id, modelno)
    if iterations < 1:
        return
    X_L, X_D, diagnostics = bdb.engine.analyze(
        M_c=bayesdb_metadata(bdb, table_id),
        T=list(bayesdb_data(bdb, table_id)),
        do_diagnostics=True,
        kernel_list=(),
        X_L=theta["X_L"],
        X_D=theta["X_D"],
        n_steps=iterations
    )
    theta["X_L"] = X_L
    theta["X_D"] = X_D
    # XXX Cargo-culted from old persistence layer's update_model.
    for diag_key in "column_crp_alpha", "logscore", "num_views":
        diag_list = [l[0] for l in diagnostics[diag_key]]
        if diag_key in theta and type(theta[diag_key]) == list:
            theta[diag_key] += diag_list
        else:
            theta[diag_key] = diag_list
    bayesdb_set_model(bdb, table_id, modelno, theta)

### BayesDB column functions

# Two-column function:  CORRELATION [OF <col0> WITH <col1>]
def bayesdb_column_correlation(bdb, table_id, colno0, colno1):
    M_c = bayesdb_metadata(bdb, table_id)
    qt = sqlite3_quote_name(bayesdb_table_name(bdb, table_id))
    qc0 = sqlite3_quote_name(bayesdb_column_name(bdb, table_id, colno0))
    qc1 = sqlite3_quote_name(bayesdb_column_name(bdb, table_id, colno1))
    data0 = []
    data1 = []
    n = 0
    try:
        data_sql = """
            SELECT %s, %s FROM %s WHERE %s IS NOT NULL AND %s IS NOT NULL
        """ % (qc0, qc1, qt, qc0, qc1)
        for row in bdb.sqlite.execute(data_sql):
            data0.append(bayesdb_value_to_code(M_c, colno0, row[0]))
            data1.append(bayesdb_value_to_code(M_c, colno1, row[1]))
            n += 1
    except KeyError:
        # XXX Ugh!  What to do?  If we allow importing any SQL table
        # after its schema has been specified, we can't add a foreign
        # key constraint to that table's schema (unless we recreate
        # the table, which is no good for remote read-only tables).
        # We could deal exclusively in heuristic imports of CSV tables
        # and not bother to support importing SQL tables.
        return 0
    assert n == len(data0)
    assert n == len(data1)
    # XXX Push this into the engine.
    modeltype0 = M_c["column_metadata"][colno0]["modeltype"]
    modeltype1 = M_c["column_metadata"][colno1]["modeltype"]
    correlation = float("NaN")  # Default result.
    if bayesdb_modeltype_numerical_p(modeltype0) and \
       bayesdb_modeltype_numerical_p(modeltype1):
        # Both numerical: Pearson R^2
        sqrt_correlation, _p_value = scipy.stats.pearsonr(data0, data1)
        correlation = sqrt_correlation ** 2
    elif bayesdb_modeltype_discrete_p(modeltype0) and \
         bayesdb_modeltype_discrete_p(modeltype1):
        # Both categorical: Cramer's phi
        unique0 = unique_indices(data0)
        unique1 = unique_indices(data1)
        min_levels = min(len(unique0), len(unique1))
        if 1 < min_levels:
            ct = [0] * len(unique0)
            for i0, j0 in enumerate(unique0):
                ct[i0] = [0] * len(unique1)
                for i1, j1 in enumerate(unique1):
                    c = 0
                    for i in range(n):
                        if data0[i] == data0[j0] and data1[i] == data1[j1]:
                            c += 1
                    ct[i0][i1] = c
            chisq, _p, _dof, _expected = scipy.stats.chi2_contingency(ct,
                correction=False)
            correlation = math.sqrt(chisq / (n * (min_levels - 1)))
    else:
        # Numerical/categorical: ANOVA R^2
        if bayesdb_modeltype_discrete_p(modeltype0):
            assert bayesdb_modeltype_numerical_p(modeltype1)
            data_group = data0
            data_y = data1
        else:
            assert bayesdb_modeltype_numerical_p(modeltype0)
            assert bayesdb_modeltype_discrete_p(modeltype1)
            data_group = data1
            data_y = data0
        group_values = unique(data_group)
        n_groups = len(group_values)
        if n_groups < n:
            samples = []
            for v in group_values:
                sample = []
                for i in range(n):
                    if data_group[i] == v:
                        sample.append(data_y[i])
                samples.append(sample)
            F, _p = scipy.stats.f_oneway(*samples)
            correlation = 1 - 1/(1 + F*((n_groups - 1) / (n - n_groups)))
    return correlation

# Two-column function:  DEPENDENCE PROBABILITY [OF <col0> WITH <col1>]
def bayesdb_column_dependence_probability(bdb, table_id, colno0, colno1):
    # XXX Push this into the engine.
    if colno0 == colno1:
        return 1
    count = 0
    nmodels = 0
    for X_L, X_D in bayesdb_latent_stata(bdb, table_id):
        nmodels += 1
        assignments = X_L["column_partition"]["assignments"]
        if assignments[colno0] != assignments[colno1]:
            continue
        if len(unique(X_D[assignments[colno0]])) <= 1:
            continue
        count += 1
    return float("NaN") if nmodels == 0 else (float(count) / float(nmodels))

# Two-column function:  MUTUAL INFORMATION [OF <col0> WITH <col1>]
def bayesdb_column_mutual_information(bdb, table_id, colno0, colno1,
        numsamples=100):
    X_L_list = list(bayesdb_latent_state(bdb, table_id))
    X_D_list = list(bayesdb_latent_data(bdb, table_id))
    r = bdb.engine.mutual_information(
        M_c=bayesdb_metadata(bdb, table_id),
        X_L_list=X_L_list,
        X_D_list=X_D_list,
        Q=[(colno0, colno1)],
        n_samples=int(math.ceil(float(numsamples) / len(X_L_list)))
    )
    # r has one answer per element of Q, so take the first one.
    r0 = r[0]
    # r0 is (mi, linfoot), and we want mi.
    mi = r0[0]
    # mi is [result for model 0, result for model 1, ...], and we want
    # the mean.
    return arithmetic_mean(mi)

# One-column function:  TYPICALITY OF <col>
def bayesdb_column_typicality(bdb, table_id, colno):
    return bdb.engine.column_structural_typicality(
        X_L_list=list(bayesdb_latent_state(bdb, table_id)),
        col_id=colno
    )

# One-column function:  PROBABILITY OF <col>=<value>
def bayesdb_column_value_probability(bdb, table_id, colno, value):
    M_c = bayesdb_metadata(bdb, table_id)
    try:
        code = bayesdb_value_to_code(M_c, colno, value)
    except KeyError:
        return 0
    X_L_list = list(bayesdb_latent_state(bdb, table_id))
    X_D_list = list(bayesdb_latent_data(bdb, table_id))
    # Fabricate a nonexistent (`unobserved') row id.
    row_id = len(X_D_list[0][0])
    r = bdb.engine.simple_predictive_probability_multistate(
        M_c=M_c,
        X_L_list=X_L_list,
        X_D_list=X_D_list,
        Y=[],
        Q=[(row_id, colno, code)]
    )
    return math.exp(r)

### BayesDB row functions

# Row function:  SIMILARITY TO <target_row> [WITH RESPECT TO <columns>]
def bayesdb_row_similarity(bdb, table_id, row_id, target_row_id, columns):
    return bdb.engine.similarity(
        M_c=bayesdb_metadata(bdb, table_id),
        X_L_list=list(bayesdb_latent_state(bdb, table_id)),
        X_D_list=list(bayesdb_latent_data(bdb, table_id)),
        given_row_id=row_id,
        target_row_id=target_row_id,
        target_columns=columns
    )

# Row function:  TYPICALITY
def bayesdb_row_typicality(bdb, table_id, row_id):
    return bdb.engine.row_structural_typicality(
        X_L_list=list(bayesdb_latent_state(bdb, table_id)),
        X_D_list=list(bayesdb_latent_data(bdb, table_id)),
        row_id=row_id
    )

# Row function:  PREDICTIVE PROBABILITY OF <column>
def bayesdb_row_column_predictive_probability(bdb, table_id, row_id, colno):
    M_c = bayesdb_metadata(bdb, table_id)
    value = bayesdb_cell_value(bdb, table_id, row_id, colno)
    code = bayesdb_value_to_code(M_c, colno, value)
    r = bdb.engine.simple_predictive_probability_multistate(
        M_c=M_c,
        X_L_list=list(bayesdb_latent_state(bdb, table_id)),
        X_D_list=list(bayesdb_latent_data(bdb, table_id)),
        Y=[],
        Q=[(row_id, colno, code)]
    )
    return math.exp(r)

### Infer and simulate

def bayesdb_infer(bdb, table_id, colno, row_id, value, confidence_threshold,
        numsamples=1):
    if value is not None:
        return value
    M_c = bayesdb_metadata(bdb, table_id)
    column_names = bayesdb_column_names(bdb, table_id)
    qt = sqlite3_quote_name(bayesdb_table_name(bdb, table_id))
    qcns = ",".join(map(sqlite3_quote_name, column_names))
    select_sql = "SELECT %s FROM %s WHERE rowid = ?" % (qcns, qt)
    c = bdb.sqlite.execute(select_sql, (row_id,))
    row = c.fetchone()
    assert row is not None
    assert c.fetchone() is None
    code, confidence = bdb.engine.impute_and_confidence(
        M_c=M_c,
        X_L=list(bayesdb_latent_state(bdb, table_id)),
        X_D=list(bayesdb_latent_data(bdb, table_id)),
        Y=[(row_id, colno_, bayesdb_value_to_code(M_c, colno_, value))
            for colno_, value in enumerate(row) if value is not None],
        Q=[(row_id, colno)],
        n=numsamples
    )
    if confidence >= confidence_threshold:
        return bayesdb_code_to_value(M_c, colno, code)
    else:
        return None

# XXX Create a virtual table that simulates results?
def bayesdb_simulate(bdb, table_id, constraints, colnos, numpredictions=1):
    M_c = bayesdb_metadata(bdb, table_id)
    qt = sqlite3_quote_name(bayesdb_table_name(bdb, table_id))
    maxrowid = sqlite3_exec_1(bdb.sqlite, "SELECT max(rowid) FROM %s" % (qt,))
    fakerowid = maxrowid + 1
    # XXX Why special-case empty constraints?
    Y = None
    if constraints is not None:
        Y = [(fakerowid, colno, bayesdb_value_to_code(M_c, colno, value))
             for colno, value in constraints]
    raw_outputs = bdb.engine.simple_predictive_sample(
        M_c=M_c,
        X_L=list(bayesdb_latent_state(bdb, table_id)),
        X_D=list(bayesdb_latent_data(bdb, table_id)),
        Y=Y,
        Q=[(fakerowid, colno) for colno in colnos],
        n=numpredictions
    )
    return [[bayesdb_code_to_value(M_c, colno, code)
            for (colno, code) in zip(colnos, raw_output)]
        for raw_output in raw_outputs]

### BayesDB utilities

def bayesdb_value_to_code(M_c, colno, value):
    metadata = M_c["column_metadata"][colno]
    modeltype = metadata["modeltype"]
    if bayesdb_modeltype_discrete_p(modeltype):
        # For hysterical raisins, code_to_value and value_to_code are
        # backwards.
        #
        # XXX Fix this.
        if value is None:
            return float("NaN")         # XXX !?!??!
        # XXX Crosscat expects floating-point codes.
        return float(metadata["code_to_value"][str(value)])
    elif bayesdb_modeltype_numerical_p(modeltype):
        if value is None:
            return float("NaN")
        return float(value)
    else:
        raise KeyError

def bayesdb_code_to_value(M_c, colno, code):
    metadata = M_c["column_metadata"][colno]
    modeltype = metadata["modeltype"]
    if bayesdb_modeltype_discrete_p(modeltype):
        if math.isnan(code):
            return None
        # XXX Whattakludge.
        return metadata["value_to_code"][str(int(code))]
    elif bayesdb_modeltype_numerical_p(modeltype):
        if math.isnan(code):
            return None
        return code
    else:
        raise KeyError

bayesdb_modeltypes_discrete = \
    set(mt for _ct, cont_p, _sql, mt in bayesdb_type_table if not cont_p)
def bayesdb_modeltype_discrete_p(modeltype):
    return modeltype in bayesdb_modeltypes_discrete

bayesdb_modeltypes_numerical = \
    set(mt for _ct, cont_p, _sql, mt in bayesdb_type_table if cont_p)
def bayesdb_modeltype_numerical_p(modeltype):
    return modeltype in bayesdb_modeltypes_numerical

### SQLite3 utilities

def sqlite3_exec_1(db, query, *args):
    """Execute a query returning a 1x1 table, and return its one value."""
    cursor = db.execute(query, *args)
    row = cursor.fetchone()
    assert row
    assert len(row) == 1
    assert cursor.fetchone() == None
    return row[0]

def sqlite3_quote_name(name):
    """Quote NAME as a SQL identifier, e.g. a table or column name.

    Do NOT use this for strings, e.g. inserting data into a table.
    Use query parameters instead.
    """
    # XXX Could omit quotes in some cases, but safer this way.
    return '"' + name.replace('"', '""') + '"'

# From <https://www.sqlite.org/datatype3.html#affname>.  Doesn't seem
# to be a built-in SQLite library routine to compute this.
def sqlite3_column_affinity(column_type):
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

import binascii
import os

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

### Miscellaneous utilities

def unique(array):
    """Return a sorted array of the unique elements in ARRAY.

    No element may be a floating-point NaN.  If your data set includes
    NaNs, omit them before passing them here.
    """
    for x in array:
        assert not (type(x) == float and math.isnan(x))
    if len(array) < 2:
        return array
    array_sorted = sorted(array)
    array_unique = [array_sorted[0]]
    for x in array_sorted[1:]:
        assert array_unique[-1] <= x
        if array_unique[-1] != x:
            array_unique.append(x)
    return array_unique

def unique_indices(array):
    """Return an array of the indices of the unique elements in ARRAY.

    No element may be a floating-point NaN.  If your data set includes
    NaNs, omit them before passing them here.
    """
    for x in array:
        assert not (type(x) == float and math.isnan(x))
    if len(array) < 2:
        return array
    array_sorted = sorted((x, i) for i, x in enumerate(array))
    array_unique = [array_sorted[0][1]]
    for x, i in array_sorted[1:]:
        assert array[array_unique[-1]] <= x
        if array[array_unique[-1]] != x:
            array_unique.append(i)
    return sorted(array_unique)

def arithmetic_mean(array):
    """Return the arithmetic mean of elements of ARRAY in floating-point."""
    return float_sum(array) / len(array)

def float_sum(array):
    """Return the sum of elements of ARRAY in floating-point.

    This implementation uses Kahan-Babuška summation.
    """
    s = 0
    c = 0
    for x in array:
        xf = float(x)
        s1 = s + xf
        if abs(x) < abs(s):
            c += ((s - s1) + xf)
        else:
            c += ((xf - s1) + s)
        s = s1
    return (s + c)
