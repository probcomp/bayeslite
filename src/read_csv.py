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

import csv

import bayeslite.core as core

from bayeslite.sqlite3_util import sqlite3_quote_name
from bayeslite.util import casefold

def bayesdb_read_csv_file(bdb, table, pathname, header=False, create=False,
        ifnotexists=False):
    """Read CSV data from a file into a table.

    :param bayeslite.BayesDB bdb: BayesDB instance
    :param str table: name of table
    :param str pathname: pathname of CSV file
    :param bool header: if true, first line specifies column names
    :param bool create: if true and `table` does not exist, create it
    :param bool ifnotexists: if true and `table` exists, do it anyway
    """
    with open(pathname, 'rU') as f:
        bayesdb_read_csv(bdb, table, f, header=header, create=create,
            ifnotexists=ifnotexists)

def bayesdb_read_csv(bdb, table, f, header=False,
        create=False, ifnotexists=False):
    """Read CSV data from a line iterator into a table.

    :param bayeslite.BayesDB bdb: BayesDB instance
    :param str table: name of table
    :param iterable f: iterator returning lines as :class:`str`
    :param bool header: if true, first line specifies column names
    :param bool create: if true and `table` does not exist, create it
    :param bool ifnotexists: if true and `table` exists, do it anyway
    """
    if not header:
        if create:
            raise ValueError('Can\'t create table from headerless CSV!')
    if not create:
        if ifnotexists:
            raise ValueError('Not creating table whether or not exists!')
    with bdb.savepoint():
        if core.bayesdb_has_table(bdb, table):
            if create and not ifnotexists:
                raise ValueError('Table already exists: %s' % (repr(table),))
        elif not create:
            raise ValueError('No such table: %s' % (repr(table),))
        reader = csv.reader(f)
        line = 1
        if header:
            row = None
            try:
                row = reader.next()
            except StopIteration:
                raise IOError('Missing header in CSV file')
            line += 1
            column_names = [unicode(name, 'utf8').strip() for name in row]
            if len(column_names) == 0:
                raise IOError('No columns in CSV file!')
            if any(len(c)==0 for c in column_names):
                raise IOError(
                    'Missing column names in header: %s' %repr(column_names))
            column_name_map = {}
            duplicates = set([])
            for name in column_names:
                name_folded = casefold(name)
                if name_folded in column_name_map:
                    duplicates.add(name_folded)
                else:
                    column_name_map[name_folded] = name
            if 0 < len(duplicates):
                raise IOError('Duplicate columns in CSV: %s' %
                    (repr(list(duplicates)),))
            if create and not core.bayesdb_has_table(bdb, table):
                qt = sqlite3_quote_name(table)
                qcns = map(sqlite3_quote_name, column_names)
                schema = ','.join('%s NUMERIC' % (qcn,) for qcn in qcns)
                bdb.sql_execute('CREATE TABLE %s(%s)' % (qt, schema))
                core.bayesdb_table_guarantee_columns(bdb, table)
            else:
                core.bayesdb_table_guarantee_columns(bdb, table)
                unknown = set(name for name in column_names
                    if not core.bayesdb_table_has_column(bdb, table, name))
                if len(unknown) != 0:
                    raise IOError('Unknown columns: %s' % (list(unknown),))
        else:
            assert not create
            assert not ifnotexists
            column_names = core.bayesdb_table_column_names(bdb, table)
        ncols = len(column_names)
        qt = sqlite3_quote_name(table)
        qcns = map(sqlite3_quote_name, column_names)
        # XXX Would be nice if we could prepare this statement before
        # reading any rows in order to check whether there are missing
        # nonnull columns with no default value.  However, the only
        # way to prepare a statement in the Python wrapper is to
        # execute a cursor, which also binds and steps the statement.
        sql = 'INSERT INTO %s (%s) VALUES (%s)' % \
            (qt, ','.join(qcns), ','.join('?' for _qcn in qcns))
        for row in reader:
            if len(row) < ncols:
                raise IOError('Line %d: Too few columns: %d < %d' %
                    (line, len(row), ncols))
            if len(row) > ncols:
                raise IOError('Line %d: Too many columns: %d > %d' %
                    (line, len(row), ncols))
            bdb.sql_execute(sql, [unicode(v, 'utf8').strip() for v in row])
