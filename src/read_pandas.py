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

"""Reading data from pandas dataframes."""

import bayeslite.core as core

from bayeslite.sqlite3_util import sqlite3_quote_name

def bayesdb_read_pandas_df(bdb, table, df, create=False, ifnotexists=False):
    """Read data from a pandas dataframe into a table.

    :param bayeslite.BayesDB bdb: BayesDB instance
    :param str table: name of table
    :param pandas.DataFrame df: pandas dataframe
    :param bool create: if true and `table` does not exist, create it
    :param bool ifnotexists: if true, and `create` is true` and `table`
        exists, read data into it anyway
    """
    if not create:
        if ifnotexists:
            raise ValueError('Not creating table whether or not exists!')
    # XXX Whattakludge!
    idxcol = '_rowid_'
    if idxcol in df.columns:
        raise ValueError('Column `_rowid_\' is not allowed.')
    with bdb.savepoint():
        if core.bayesdb_has_table(bdb, table):
            if create and not ifnotexists:
                raise ValueError('Table already exists: %s' % (repr(table),))
            core.bayesdb_table_guarantee_columns(bdb, table)
            unknown = set(name for name in df.columns
                if not core.bayesdb_table_has_column(bdb, table, name))
            if len(unknown) != 0:
                raise ValueError('Unknown columns: %s' % (list(unknown),))
            column_names = ['_rowid_'] + df.columns
        elif create:
            column_names = [idxcol] + list(df.columns)
            qcns = map(sqlite3_quote_name, column_names)
            schema = ','.join('%s NUMERIC' % (qcn,) for qcn in qcns)
            qt = sqlite3_quote_name(table)
            bdb.sql_execute('CREATE TABLE %s(%s)' % (qt, schema))
            core.bayesdb_table_guarantee_columns(bdb, table)
        else:
            raise ValueError('No such table: %s' % (repr(table),))
        qt = sqlite3_quote_name(table)
        qcns = map(sqlite3_quote_name, column_names)
        sql = 'INSERT INTO %s (%s) VALUES (%s)' % \
            (qt, ','.join(qcns), ','.join('?' for _qcn in qcns))
        for row in df.to_records():
            bdb.sql_execute(sql, row)
