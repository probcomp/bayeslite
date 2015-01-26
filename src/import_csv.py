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

import csv
import math

import bayeslite.core as core

# XXX Allow the user to pass in the desired encoding (and CSV dialect,
# &c.).
#
# XXX Support pandas data frames...
def bayesdb_read_csv_with_header(pathname):
    with open(pathname, "rU") as f:
        reader = csv.reader(f)
        try:
            header = reader.next()
        except StopIteration:
            raise IOError("Empty CSV file")
        # XXX Let the user pass in the desired encoding.
        column_names = [unicode(n, "utf8").strip() for n in header]
        ncols = len(column_names)
        if ncols == 0:
            raise IOError("No columns in CSV file!")
        # XXX Can we get the CSV reader to decode and strip for us?
        rows = [[unicode(v, "utf8").strip() for v in row] for row in reader]
        for row in rows:
            if len(row) != ncols:
                raise IOError("Mismatched number of columns")
        return column_names, rows

# XXX Is this the schema language we want?
def bayesdb_import_csv_file(bdb, table, pathname, column_types=None,
        ifnotexists=False):
    if ifnotexists:
        with core.sqlite3_savepoint(bdb.sqlite):
            if not core.bayesdb_table_exists(bdb, table):
                _bayesdb_import_csv_file(bdb, table, pathname, column_types)
    else:
        _bayesdb_import_csv_file(bdb, table, pathname, column_types)

def _bayesdb_import_csv_file(bdb, table, pathname, column_types):
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
    ncols = len(column_names)
    assert ncols == len(column_types)
    qt = core.sqlite3_quote_name(table)
    table_def = bayesdb_table_definition(table, column_names, column_types)
    with core.sqlite3_savepoint(bdb.sqlite):
        bdb.sqlite.execute(table_def)
        qcns = ",".join(map(core.sqlite3_quote_name, column_names))
        qcps = ",".join("?" * ncols)
        insert_sql = "INSERT INTO %s (%s) VALUES (%s)" % (qt, qcns, qcps)
        bdb.sqlite.executemany(insert_sql, rows)
        core.bayesdb_import_sqlite_table(bdb, table, column_names,
            column_types)

def bayesdb_table_definition(table, column_names, column_types):
    column_defs = [bayesdb_column_definition(name, column_types[name])
        for name in column_names]
    qt = core.sqlite3_quote_name(table)
    return ("CREATE TABLE %s (%s)" % (qt, ",".join(column_defs)))

bayesdb_column_type_to_sqlite_type = \
    dict((ct, sql) for ct, _cont_p, sql, _mt in core.bayesdb_type_table)
def bayesdb_column_definition(column_name, column_type):
    qcn = core.sqlite3_quote_name(column_name)
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
        return len(rows) == len(core.unique([int(row[i]) for row in rows]))
    elif not bayesdb_csv_column_floatable_p(rows, i):
        # XXX Is unicode(...) necessary?  I think they should all be
        # strings here.  Where can stripping happen?
        assert all(row[i] == unicode(row[i]).strip() for row in rows)
        return len(rows) == len(core.unique([row[i] for row in rows]))
    else:
        return False

def bayesdb_csv_column_integerable_p(rows, i):
    try:
        for row in rows:
            if unicode(row[i]) != unicode(int(row[i])):
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
    ndistinct = len(core.unique([float(row[i]) for row in rows
        if not math.isnan(float(row[i]))]))
    if ndistinct <= count_cutoff:
        return False
    ndata = len(rows)
    if (float(ndistinct) / float(ndata)) <= ratio_cutoff:
        return False
    return True
