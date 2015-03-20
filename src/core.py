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

from bayeslite.sqlite3_util import sqlite3_quote_name

def bayesdb_has_table(bdb, name):
    qt = sqlite3_quote_name(name)
    cursor = bdb.sql_execute('PRAGMA table_info(%s)' % (qt,))
    try:
        cursor.next()
    except StopIteration:
        return False
    else:
        return True

def bayesdb_table_column_names(bdb, table):
    if not bayesdb_has_table(bdb, table):
        raise ValueError('No such table: %s' % (repr(table),))
    sql = '''
        SELECT name FROM bayesdb_column WHERE tabname = ?
            ORDER BY colno ASC
    '''
    return [row[0] for row in bdb.sql_execute(sql, (table,))]

def bayesdb_table_column_name(bdb, table, colno):
    sql = '''
        SELECT name FROM bayesdb_column WHERE tabname = ? AND colno = ?
    '''
    cursor = bdb.sql_execute(sql, (table, colno))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such column number in table %s: %d' %
            (table, colno))
    else:
        return row[0]

def bayesdb_table_guarantee_columns(bdb, table):
    with bdb.savepoint():
        qt = sqlite3_quote_name(table)
        insert_column_sql = '''
            INSERT OR IGNORE INTO bayesdb_column (tabname, colno, name)
                VALUES (?, ?, ?)
        '''
        nrows = 0
        for row in bdb.sql_execute('PRAGMA table_info(%s)' % (qt,)):
            nrows += 1
            colno, name, _sqltype, _notnull, _default, _primary_key = row
            bdb.sql_execute(insert_column_sql, (table, colno, name))
        if nrows == 0:
            raise ValueError('No such table: %s' % (repr(table),))

def bayesdb_table_has_column(bdb, table, name):
    sql = 'SELECT COUNT(*) FROM bayesdb_column WHERE tabname = ? AND name = ?'
    cursor = bdb.sql_execute(sql, (table, name))
    return cursor.next()[0]

def bayesdb_has_generator(bdb, name):
    sql = 'SELECT COUNT(*) FROM bayesdb_generator WHERE name = ?'
    cursor = bdb.sql_execute(sql, (name,))
    return cursor.next()[0] != 0

def bayesdb_get_generator(bdb, name):
    sql = 'SELECT id FROM bayesdb_generator WHERE name = ?'
    cursor = bdb.sql_execute(sql, (name,))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such generator: %s' % (repr(name),))
    else:
        assert isinstance(row[0], int)
        return row[0]

def bayesdb_generator_name(bdb, id):
    sql = 'SELECT name FROM bayesdb_generator WHERE id = ?'
    cursor = bdb.sql_execute(sql, (id,))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such generator id: %d' % (repr(id),))
    else:
        return row[0]

def bayesdb_generator_metamodel(bdb, id):
    sql = 'SELECT metamodel FROM bayesdb_generator WHERE id = ?'
    cursor = bdb.sql_execute(sql, (id,))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such generator: %s' % (repr(id),))
    else:
        if row[0] not in bdb.metamodels:
            raise ValueError('Metamodel of generator %s not registered: %s' %
                (repr(name), repr(row[0])))
        return bdb.metamodels[row[0]]

def bayesdb_generator_table(bdb, id):
    sql = 'SELECT tabname FROM bayesdb_generator WHERE id = ?'
    cursor = bdb.sql_execute(sql, (id,))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such generator: %s' % (repr(id),))
    else:
        assert len(row) == 1
        return row[0]

def bayesdb_generator_column_names(bdb, generator_id):
    sql = '''
        SELECT c.name
            FROM bayesdb_column AS c,
                bayesdb_generator AS g,
                bayesdb_generator_column AS gc
            WHERE g.id = ?
                AND gc.generator_id = g.id
                AND c.tabname = g.tabname
                AND c.colno = gc.colno
            ORDER BY c.colno ASC
    '''
    return [row[0] for row in bdb.sql_execute(sql, (generator_id,))]

def bayesdb_generator_column_stattype(bdb, generator_id, colno):
    sql = '''
        SELECT stattype FROM bayesdb_generator_column
            WHERE generator_id = ? AND colno = ?
    '''
    cursor = bdb.sql_execute(sql, (generator_id, colno))
    try:
        row = cursor.next()
    except StopIteration:
        generator = bayesdb_generator_name(bdb, generator_id)
        sql = '''
            SELECT COUNT(*)
                FROM bayesdb_generator AS g, bayesdb_column AS c
                WHERE g.id = ? AND g.tabname = c.tabname AND c.colno = ?
        '''
        cursor = bdb.sql_execute(sql, (generator_id, colno))
        if cursor.next()[0] == 0:
            raise ValueError('No such column in generator %s: %d' %
                (generator, colno))
        else:
            raise ValueError('Column not modelled in generator %s: %d' %
                (generator, colno))
    else:
        assert len(row) == 1
        return row[0]

def bayesdb_generator_column_name(bdb, generator_id, colno):
    sql = '''
        SELECT c.name
            FROM bayesdb_generator AS g,
                bayesdb_generator_column AS gc,
                bayesdb_column AS c
            WHERE g.id = ?
                AND gc.colno = ?
                AND g.id = gc.generator_id
                AND g.tabname = c.tabname
                AND gc.colno = c.colno
    '''
    cursor = bdb.sql_execute(sql, (generator_id, colno))
    try:
        row = cursor.next()
    except StopIteration:
        generator = bayesdb_generator_name(bdb, generator_id)
        raise ValueError('No such column number in generator %s: %d' %
            (repr(generator), colno))
    else:
        assert len(row) == 1
        return row[0]

def bayesdb_generator_column_number(bdb, generator_id, column_name):
    sql = '''
        SELECT c.colno
            FROM bayesdb_generator AS g,
                bayesdb_generator_column as gc,
                bayesdb_column AS c
            WHERE g.id = ? AND c.name = ?
                AND g.id = gc.generator_id
                AND g.tabname = c.tabname
                AND gc.colno = c.colno
    '''
    cursor = bdb.sql_execute(sql, (generator_id, column_name))
    try:
        row = cursor.next()
    except StopIteration:
        generator = bayesdb_generator_name(bdb, generator_id)
        raise ValueError('No such column in generator %s: %s' %
            (repr(generator), repr(column_name)))
    else:
        assert len(row) == 1
        assert isinstance(row[0], int)
        return row[0]

def bayesdb_generator_column_numbers(bdb, generator_id):
    sql = '''
        SELECT colno FROM bayesdb_generator_column
            WHERE generator_id = ?
            ORDER BY colno ASC
    '''
    return [row[0] for row in bdb.sql_execute(sql, (generator_id,))]

def bayesdb_generator_has_model(bdb, generator_id, modelno):
    sql = '''
        SELECT COUNT(*) FROM bayesdb_generator_model AS m
            WHERE generator_id = ? AND modelno = ?
    '''
    cursor = bdb.sql_execute(sql, (generator_id, modelno))
    return cursor.next()[0] != 0
