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

import bayeslite.core as core

from bayeslite.sqlite3_util import sqlite3_quote_name

def bayesdb_read_csv(bdb, table, f, database=None, header=False,
        create=False, ifnotexists=False):
    if not header:
        if create:
            raise ValueError('Can\'t create table from headerless CSV!')
    if not create:
        if ifnotexists:
            raise ValueError('Not creating table whether or not exists!')
    with bdb.savepoint():
        if create:
            if core.bayesdb_has_table(bdb, table) and not ifnotexists:
                raise ValueError('Table already exists: %s' % (repr(table),))
        else:
            if not core.bayesdb_has_table(bdb, table):
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
            if create and not core.bayesdb_has_table(bdb, table):
                qdb = ''
                if database is not None:
                    qdb += '.'
                    qdb += sqlite3_quote_name(database)
                qt = sqlite3_quote_name(table)
                qcns = ','.join(map(sqlite3_quote_name, column_names))
                bdb.sql_execute('CREATE TABLE %s%s(%s)' % (qdb, qt, qcns))
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
                raise ValueError('Line %d: Too few columns: %d < %d' %
                    (line, len(row), ncols))
            if len(row) > ncols:
                raise ValueError('Line %d: Too many columns: %d > %d' %
                    (line, len(row), ncols))
            bdb.sql_execute(sql, [unicode(v, 'utf8').strip() for v in row])
