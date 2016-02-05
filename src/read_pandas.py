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

"""Reading data from pandas dataframes."""

import bayeslite.core as core

from bayeslite.sqlite3_util import sqlite3_quote_name

def bayesdb_read_pandas_df(bdb, table, df, create=False, ifnotexists=False,
        index=None):
    """Read data from a pandas dataframe into a table.

    :param bayeslite.BayesDB bdb: BayesDB instance
    :param str table: name of table
    :param pandas.DataFrame df: pandas dataframe
    :param bool create: if true and `table` does not exist, create it
    :param bool ifnotexists: if true, and `create` is true` and `table`
        exists, read data into it anyway
    :param str index: name of column for index

    If `index` is `None`, then the dataframe's index dtype must be
    convertible to int64, and it is mapped to the table's rowids.  If
    the dataframe's index dtype is not convertible to int64, you must
    specify `index` to give a primary key for the table.
    """
    if not create:
        if ifnotexists:
            raise ValueError('Not creating table whether or not exists!')
    column_names = [str(column) for column in df.columns]
    if index is None:
        create_column_names = column_names
        insert_column_names = ['_rowid_'] + column_names
        try:
            key_index = df.index.astype('int64')
        except ValueError:
            raise ValueError('Must specify index name for non-integral index!')
    else:
        if index in df.columns:
            raise ValueError('Index name collides with column name: %r'
                % (index,))
        create_column_names = [index] + column_names
        insert_column_names = create_column_names
        key_index = df.index
    with bdb.savepoint():
        if core.bayesdb_has_table(bdb, table):
            if create and not ifnotexists:
                raise ValueError('Table already exists: %s' % (repr(table),))
            core.bayesdb_table_guarantee_columns(bdb, table)
            unknown = set(name for name in create_column_names
                if not core.bayesdb_table_has_column(bdb, table, name))
            if len(unknown) != 0:
                raise ValueError('Unknown columns: %s' % (list(unknown),))
        elif create:
            qccns = map(sqlite3_quote_name, create_column_names)
            def column_schema(column_name, qcn):
                if column_name == index:
                    return '%s NUMERIC PRIMARY KEY' % (qcn,)
                else:
                    return '%s NUMERIC' % (qcn,)
            schema = ','.join(column_schema(ccn, qccn)
                for ccn, qccn in zip(create_column_names, qccns))
            qt = sqlite3_quote_name(table)
            bdb.sql_execute('CREATE TABLE %s(%s)' % (qt, schema))
            core.bayesdb_table_guarantee_columns(bdb, table)
        else:
            raise ValueError('No such table: %s' % (repr(table),))
        qt = sqlite3_quote_name(table)
        qicns = map(sqlite3_quote_name, insert_column_names)
        sql = 'INSERT INTO %s (%s) VALUES (%s)' % \
            (qt, ','.join(qicns), ','.join('?' for _qicn in qicns))
        for key, i in zip(key_index, df.index):
            bdb.sql_execute(sql, (key,) + tuple(df.ix[i]))
