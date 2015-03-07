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
#               db.execute("SELECT %s FROM %s WHERE x = ?" % (qc, qt), (x,))
#      DON'T:   db.execute("SELECT %s FROM %s WHERE x = ?" % (column, table),
#                   (x,))

import contextlib
import json
import math

from bayeslite.sqlite3_util import sqlite3_quote_name

from bayeslite.util import casefold

bayesdb_type_table = [
    # column type, numerical?, default sqlite, default model type
    ("categorical",     False,  "text",         "symmetric_dirichlet_discrete"),
    ("cyclic",          True,   "real",         "vonmises"),
    ("ignore",          False,  "text",         None),
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

### BayesDB class interface

class IBayesDB(object):
    """Interface of Bayesian databases."""

    def __init__(self):
        self.txn_depth = 0
        self.metadata_cache = None
        self.models_cache = None
        self.metamodels_by_id = {}
        self.default_metamodel_id = None
        raise NotImplementedError

    def close(self):
        """Close the database.  Further use is not allowed."""
        raise NotImplementedError

    def cursor(self):
        """Return a cursor fit for executing BQL queries."""
        raise NotImplementedError

    def execute(self, query, *args):
        """Execute a BQL query and return a cursor for its results."""
        raise NotImplementedError

    def sql_execute(self, query, *args):
        """Execute a SQL query on the underlying database."""
        raise NotImplementedError

    @contextlib.contextmanager
    def savepoint(self):
        """Savepoint context.  On return, commit; on exception, roll back.

        Savepoints may be nested.
        """
        raise NotImplementedError

def bayesdb_register_metamodel(bdb, name, engine):
    with bdb.savepoint():
        # Name it in the SQL database.
        insert_sql = """
            INSERT OR IGNORE INTO bayesdb_metamodel (name) VALUES (?)
        """
        bdb.sql_execute(insert_sql, (name,))
        # Associate it with the engine by id.
        #
        # XXX Can't use lastrowid here because
        # sqlite3_last_insert_rowid doesn't consider INSERT OR IGNORE
        # to be successful if it has to ignore the insertion, even
        # though the obvious sensible thing to do is to return the
        # existing rowid.
        lookup_sql = "SELECT id FROM bayesdb_metamodel WHERE name = ?"
        metamodel_id = bayesdb_sql_execute1(bdb, lookup_sql, (name,))
        assert metamodel_id not in bdb.metamodels_by_id
        bdb.metamodels_by_id[metamodel_id] = engine

def bayesdb_deregister_metamodel(bdb, name):
    with bdb.savepoint():
        lookup_sql = "SELECT id FROM bayesdb_metamodel WHERE name = ?"
        metamodel_id = bayesdb_sql_execute1(bdb, lookup_sql, (name,))
        assert metamodel_id in bdb.metamodels_by_id
        assert bdb.default_metamodel_id != metamodel_id
        del bdb.metamodels_by_id[metamodel_id]

def bayesdb_set_default_metamodel(bdb, name):
    if name is None:
        bdb.default_metamodel_id = None
    else:
        lookup_sql = "SELECT id FROM bayesdb_metamodel WHERE name = ?"
        metamodel_id = bayesdb_sql_execute1(bdb, lookup_sql, (name,))
        bdb.default_metamodel_id = metamodel_id

### Importing SQLite tables

# XXX Use URIs, and attach read-only?
def bayesdb_attach_sqlite_file(bdb, name, pathname):
    """Attach the SQLite database file at PATHNAME under the name NAME."""
    # Turns out Python urlparse is broken and can't handle relative
    # pathnames.  Urgh.
    #uri = urlparse.urlunparse("file", "", urllib.quote(pathname), "", "", "")
    bdb.sql_execute("ATTACH DATABASE ? AS %s" % (name,), pathname)

# def bayesdb_attach_sqlite_uri(bdb, name, uri):
#     ...

# XXX Accept parameters for guessing column types: count_cutoff &c.
#
# XXX Allow ignored columns?
def bayesdb_import_sqlite_table(bdb, table,
        column_names=None, column_types=None, metamodel=None):
    """Import a SQLite table for use in a BayesDB with BQL.

    COLUMN_NAMES is a list specifying the desired order and selection
    of column names for the BQL table.  COLUMN_TYPES is a dict mapping
    the selected column names to their BQL column types.
    """
    column_names, column_types = bayesdb_determine_columns(bdb, table,
        column_names, column_types)
    metadata = bayesdb_create_metadata(bdb, table, column_names, column_types)
    metadata_json = json.dumps(metadata)
    # XXX Check that rowids are contiguous.
    with bdb.savepoint():
        metamodel_id = None
        if metamodel is not None:
            metamodel_sql = "SELECT id FROM bayesdb_metamodel WHERE name = ?"
            metamodel_id = bayesdb_sql_execute1(bdb, metamodel_sql,
                (metamodel,))
        elif bdb.default_metamodel_id is not None:
            metamodel_id = bdb.default_metamodel_id
        else:
            raise ValueError("No metamodel given and no default metamodel!")
        table_sql = """
            INSERT INTO bayesdb_table (name, metamodel_id, metadata)
                VALUES (?, ?, ?)
        """
        table_bindings = (table, metamodel_id, metadata_json)
        table_cursor = bdb.sql_execute(table_sql, table_bindings)
        table_id = table_cursor.lastrowid
        assert table_id is not None
        for colno, name in enumerate(column_names):
            column_sql = """
                INSERT INTO bayesdb_table_column (table_id, name, colno)
                VALUES (?, ?, ?)
            """
            bdb.sql_execute(column_sql, (table_id, name, colno))

def bayesdb_determine_columns(bdb, table, column_names, column_types):
    # Find the columns as SQLite knows about them.
    qt = sqlite3_quote_name(table)
    cursor = bdb.sql_execute("PRAGMA table_info(%s)" % (qt,))
    column_descs = cursor.fetchall()

    # Determine the column names, which must all be among the ones
    # SQLite knows about.
    if column_names is None:
        column_names = [name for _i, name, _t, _n, _d, _p in column_descs]
    else:
        column_name_superset = set(casefold(name)
            for _i, name, _t, _n, _d, _p in column_descs)
        assert len(column_name_superset) == len(column_descs)
        for name in column_names:
            if casefold(name) not in column_name_superset:
                raise ValueError("Unknown column named: %s" % (name,))

    # Make sure there are no names that differ only by case.
    column_names_folded = {}
    for name in column_names:
        if casefold(name) in column_names_folded:
            raise ValueError("Column names differ only in case: %s, %s" %
                (column_names_folded[casefold(name)], name))
        column_names_folded[casefold(name)] = name

    # Determine the column types.  If none are given, guess them from
    # the data in the table; otherwise, make sure every name in
    # column_types appears in the list of column names.
    if column_types is None:
        column_types = dict(bayesdb_guess_column(bdb, table, desc)
            for desc in column_descs
            if casefold(desc[1]) in column_names_folded)
    else:
        for name in column_types:
            if casefold(name) not in column_names_folded:
                raise ValueError("Unknown column typed: %s" % (name,))

    # Make sure every column named is given a type.
    column_types_folded = dict((casefold(name), column_types[name])
        for name in column_types)
    for name in column_names_folded:
        if name not in column_types_folded:
            raise ValueError("Named column missing type: %s" % (name,))

    # Make sure there's at most one key column.
    key = None
    for name in column_types:
        if column_types[name] == "key":
            if key is None:
                key = name
            else:
                raise ValueError("More than one key: %s, %s" % (key, name))

    # Rip out the key column and the ignored columns.
    modelled_column_names = [name for name in column_names
        if column_types_folded[casefold(name)] not in ("key", "ignore")]
    modelled_column_types = dict((name, column_types[name])
        for name in column_types
        if column_types[name] not in ("key", "ignore"))

    # Make sure at least one column is modelled.
    if len(modelled_column_names) == 0:
        raise ValueError("No columns to model")

    return modelled_column_names, modelled_column_types

# XXX Pass count_cutoff/ratio_cutoff through from above.
def bayesdb_guess_column(bdb, table, column_desc,
        count_cutoff=20, ratio_cutoff=0.02):
    (_cid, column_name, _sql_type, _nonnull, _default, primary_key) = \
        column_desc
    # XXXX Can we ask about unique constraints too?
    if primary_key:
        return (column_name, "key")
    # XXX Use sqlite column type as a heuristic?  Won't help for CSV.
    qt = sqlite3_quote_name(table)
    qcn = sqlite3_quote_name(column_name)
    ndistinct_sql = "SELECT COUNT(DISTINCT %s) FROM %s" % (qcn, qt)
    ndistinct = bayesdb_sql_execute1(bdb, ndistinct_sql)
    if ndistinct <= count_cutoff:
        return (column_name, "categorical")
    ndata_sql = "SELECT COUNT(%s) FROM %s" % (qcn, qt)
    ndata = bayesdb_sql_execute1(bdb, ndata_sql)
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
    cursor = bdb.sql_execute(sql)
    try:
        for row in cursor:
            float(row[0])
    except ValueError:
        return False
    return True

def bayesdb_create_metadata(bdb, table, column_names, column_types):
    ncols = len(column_names)
    assert ncols == len(column_types)
    # Weird contortions to ignore case distinctions in column_names
    # and the keys of column_types.
    column_positions = dict((casefold(name), i)
        for i, name in enumerate(column_names))
    column_metadata = [None] * ncols
    for name in column_types:
        metadata = metadata_generators[column_types[name]](bdb, table, name)
        column_metadata[column_positions[casefold(name)]] = metadata
    assert all(metadata is not None for metadata in column_metadata)
    return {
        "name_to_idx": dict(zip(map(casefold, column_names), range(ncols))),
        "idx_to_name": dict(zip(map(unicode, range(ncols)), column_names)),
        "column_metadata": column_metadata,
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
    cursor = bdb.sql_execute(sql)
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

### BayesDB data/metadata access

def bayesdb_data(bdb, table_id):
    M_c = bayesdb_metadata(bdb, table_id)
    table = bayesdb_table_name(bdb, table_id)
    column_names = list(bayesdb_column_names(bdb, table_id))
    qt = sqlite3_quote_name(table)
    qcns = ",".join(map(sqlite3_quote_name, column_names))
    sql = "SELECT %s FROM %s" % (qcns, qt)
    for row in bdb.sql_execute(sql):
        yield tuple(bayesdb_value_to_code(M_c, i, v)
            for i, v in enumerate(row))

def bayesdb_metadata(bdb, table_id):
    if bdb.metadata_cache is not None:
        if table_id in bdb.metadata_cache:
            return bdb.metadata_cache[table_id]
    sql = "SELECT metadata FROM bayesdb_table WHERE id = ?"
    metadata_json = bayesdb_sql_execute1(bdb, sql, (table_id,))
    metadata = json.loads(metadata_json)
    if bdb.metadata_cache is not None:
        assert table_id not in bdb.metadata_cache
        bdb.metadata_cache[table_id] = metadata
    return metadata

def bayesdb_table_exists(bdb, table_name):
    sql = "SELECT COUNT(*) FROM bayesdb_table WHERE name = ?"
    return 0 < bayesdb_sql_execute1(bdb, sql, (table_name,))

def bayesdb_table_name(bdb, table_id):
    sql = "SELECT name FROM bayesdb_table WHERE id = ?"
    return bayesdb_sql_execute1(bdb, sql, (table_id,))

def bayesdb_table_id(bdb, table_name):
    sql = "SELECT id FROM bayesdb_table WHERE name = ?"
    return bayesdb_sql_execute1(bdb, sql, (table_name,))

def bayesdb_table_engine(bdb, table_id):
    sql = "SELECT metamodel_id FROM bayesdb_table WHERE id = ?"
    metamodel_id = bayesdb_sql_execute1(bdb, sql, (table_id,))
    if metamodel_id not in bdb.metamodels_by_id:
        sql = "SELECT name FROM bayesdb_metamodel WHERE id = ?"
        metamodel_name = bayesdb_sql_execute1(bdb, sql, (metamodel_id,))
        raise ValueError("No engine for metamodel: %s", (metamodel_name,))
    return bdb.metamodels_by_id[metamodel_id]

def bayesdb_column_names(bdb, table_id):
    sql = """
        SELECT name FROM bayesdb_table_column WHERE table_id = ? ORDER BY colno
    """
    for row in bdb.sql_execute(sql, (table_id,)):
        yield row[0]

def bayesdb_column_exists(bdb, table_id, name):
    sql = """
        SELECT count(*) FROM bayesdb_table_column
            WHERE table_id = ? AND name = ?
    """
    return 0 < bayesdb_sql_execute1(bdb, sql, (table_id, name))

def bayesdb_column_name(bdb, table_id, colno):
    sql = """
        SELECT name FROM bayesdb_table_column WHERE table_id = ? AND colno = ?
    """
    return bayesdb_sql_execute1(bdb, sql, (table_id, colno))

def bayesdb_column_numbers(bdb, table_id):
    sql = """
        SELECT colno FROM bayesdb_table_column WHERE table_id = ?
        ORDER BY colno
    """
    for row in bdb.sql_execute(sql, (table_id,)):
        yield row[0]

def bayesdb_column_number(bdb, table_id, column_name):
    sql = """
        SELECT colno FROM bayesdb_table_column WHERE table_id = ? AND name = ?
    """
    return bayesdb_sql_execute1(bdb, sql, (table_id, column_name))

def bayesdb_column_values(bdb, table_id, colno):
    qt = sqlite3_quote_name(bayesdb_table_name(bdb, table_id))
    qc = sqlite3_quote_name(bayesdb_column_name(bdb, table_id, colno))
    for row in bdb.sql_execute("SELECT %s FROM %s" % (qc, qt)):
        yield row[0]

def bayesdb_cell_value(bdb, table_id, rowid, colno):
    qt = sqlite3_quote_name(bayesdb_table_name(bdb, table_id))
    qc = sqlite3_quote_name(bayesdb_column_name(bdb, table_id, colno))
    sql = "SELECT %s FROM %s WHERE _rowid_ = ?" % (qc, qt)
    return bayesdb_sql_execute1(bdb, sql, (rowid,))

### BayesDB model access

def bayesdb_init_model(bdb, table_id, modelno, theta, ifnotexists=False):
    insert_sql = """
        INSERT %s INTO bayesdb_model (table_id, modelno, theta)
        VALUES (?, ?, ?)
    """ % ("OR IGNORE" if ifnotexists else "")
    theta_json = json.dumps(theta)
    bdb.sql_execute(insert_sql, (table_id, modelno, theta_json))
    if bdb.models_cache is not None:
        key = (table_id, modelno)
        if ifnotexists:
            if key not in bdb.models_cache:
                bdb.models_cache[key] = theta
        else:
            assert key not in bdb.models_cache
            bdb.models_cache[key] = theta

def bayesdb_set_model(bdb, table_id, modelno, theta):
    sql = """
        UPDATE bayesdb_model SET theta = ? WHERE table_id = ? AND modelno = ?
    """
    theta_json = json.dumps(theta)
    bdb.sql_execute(sql, (theta_json, table_id, modelno))
    if bdb.models_cache is not None:
        bdb.models_cache[table_id, modelno] = theta

def bayesdb_has_model(bdb, table_id, modelno):
    if bdb.models_cache is not None:
        if (table_id, modelno) in bdb.models_cache:
            return True
    sql = """
        SELECT count(*) FROM bayesdb_model WHERE table_id = ? AND modelno = ?
    """
    return 0 < bayesdb_sql_execute1(bdb, sql, (table_id, modelno))

def bayesdb_drop_models(bdb, table_id, modelnos=None):
    if modelnos is None:
        bdb.sql_execute('DELETE FROM bayesdb_model WHERE table_id = ?',
            (table_id,))
    else:
        modelnos = sorted(list(modelnos))
        for modelno in modelnos:
            if not bayesdb_has_model(bdb, table_id, modelno):
                raise ValueError("No such model: %d" % (modelno,))
        sql = 'DELETE FROM bayesdb_model WHERE table_id = ? AND modelno = ?'
        for modelno in modelnos:
            bdb.sql_execute(sql, (table_id, modelno))

def bayesdb_modelnos(bdb, table_id):
    sql = "SELECT modelno FROM bayesdb_model WHERE table_id = ?"
    return (row[0] for row in bdb.sql_execute(sql, (table_id,)))

def bayesdb_models(bdb, table_id, modelnos=None):
    if modelnos is None:
        modelnos = bayesdb_modelnos(bdb, table_id)
    return (bayesdb_model(bdb, table_id, modelno) for modelno in modelnos)

def bayesdb_model(bdb, table_id, modelno):
    if bdb.models_cache is not None:
        key = (table_id, modelno)
        if key in bdb.models_cache:
            return bdb.models_cache[key]
    sql = """
        SELECT theta FROM bayesdb_model WHERE table_id = ? AND modelno = ?
    """
    theta_json = bayesdb_sql_execute1(bdb, sql, (table_id, modelno))
    theta = json.loads(theta_json)
    if bdb.models_cache is not None:
        key = (table_id, modelno)
        assert key not in bdb.models_cache
        bdb.models_cache[key] = theta
    return theta

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
        return float(metadata["code_to_value"][unicode(value)])
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
        return metadata["value_to_code"][unicode(int(code))]
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

# By default, SQLite3 automatically numbers rows starting at 1, and
# not necessarily contiguously (although they are noncontiguous only
# if rows are deleted).  Crosscat expects contiguous 0-indexed row
# ids.  For now, we'll judiciously map between them, and maintain the
# convention that `rowid' means a SQLite3 rowid (like the ROWID column
# built-in to tables by default) and `row_id' means a Crosscat row id.
#
# XXX Revisit row numbering between SQLite3 and Crosscat.

def sqlite3_rowid_to_engine_row_id(rowid):
    return rowid - 1

def engine_row_id_to_sqlite3_rowid(row_id):
    return row_id + 1

def bayesdb_sql_execute1(bdb, sql, bindings=()):
    cursor = bdb.sql_execute(sql, bindings)
    row = cursor.fetchone()
    assert row
    assert len(row) == 1
    assert cursor.fetchone() is None
    return row[0]
