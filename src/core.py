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

"""Miscellaneous utilities for managing BayesDB entities.

Tables, generators, and columns are named with strs.  Only US-ASCII is
allowed, no Unicode.

Each table has a nonempty sequence of named columns.  As in sqlite3,
tables may be renamed and do not necessarily have numeric ids, so
there is no way to have a handle on a table that is persistent outside
a savepoint.

Each table may optionally be modeled by any number of generators,
representing a parametrized generative model for the table's data,
according to a named generator.

Each generator models a subset of the columns in its table, which are
called the modeled columns of that generator.  Each column in a
generator has an associated statistical type.  Like tables, generators
may be renamed.  Unlike tables, each generator has a numeric id, which
is never reused and therefore persistent across savepoints.

Each generator may have any number of different models, each
representing a particular choice of parameters for the parametrized
generative model.  Models are numbered consecutively for the
generator, and may be identified uniquely by ``(generator_id,
modelno)`` or ``(generator_name, modelno)``.
"""

from bayeslite.exception import BQLError
from bayeslite.sqlite3_util import sqlite3_quote_name
from bayeslite.util import casefold
from bayeslite.util import cursor_value

def bayesdb_has_table(bdb, name):
    """True if there is a table named `name` in `bdb`.

    The table need not be modeled.
    """
    qt = sqlite3_quote_name(name)
    cursor = bdb.sql_execute('PRAGMA table_info(%s)' % (qt,))
    try:
        cursor.next()
    except StopIteration:
        return False
    else:
        return True

def bayesdb_table_column_names(bdb, table):
    """Return a list of names of columns in the table named `table`.

    The results strs and are ordered by column number.

    `bdb` must have a table named `table`.  If you're not sure, call
    :func:`bayesdb_has_table` first.

    WARNING: This may modify the database by populating the
    ``bayesdb_column`` table if it has not yet been populated.
    """
    bayesdb_table_guarantee_columns(bdb, table)
    sql = '''
        SELECT name FROM bayesdb_column WHERE tabname = ?
            ORDER BY colno ASC
    '''
    # str because column names can't contain Unicode in sqlite3.
    return [str(row[0]) for row in bdb.sql_execute(sql, (table,))]

def bayesdb_table_has_column(bdb, table, name):
    """True if the table named `table` has a column named `name`.

    `bdb` must have a table named `table`.  If you're not sure, call
    :func:`bayesdb_has_table` first.

    WARNING: This may modify the database by populating the
    ``bayesdb_column`` table if it has not yet been populated.
    """
    bayesdb_table_guarantee_columns(bdb, table)
    sql = 'SELECT COUNT(*) FROM bayesdb_column WHERE tabname = ? AND name = ?'
    return cursor_value(bdb.sql_execute(sql, (table, name)))

def bayesdb_table_column_name(bdb, table, colno):
    """Return the name of the column numbered `colno` in `table`.

    `bdb` must have a table named `table`.  If you're not sure, call
    :func:`bayesdb_has_table` first.

    WARNING: This may modify the database by populating the
    ``bayesdb_column`` table if it has not yet been populated.
    """
    bayesdb_table_guarantee_columns(bdb, table)
    sql = '''
        SELECT name FROM bayesdb_column WHERE tabname = ? AND colno = ?
    '''
    cursor = bdb.sql_execute(sql, (table, colno))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such column number in table %s: %d' %
            (repr(table), colno))
    else:
        return row[0]

def bayesdb_table_column_number(bdb, table, name):
    """Return the number of column named `name` in `table`.

    `bdb` must have a table named `table`.  If you're not sure, call
    :func:`bayesdb_has_table` first.

    WARNING: This may modify the database by populating the
    ``bayesdb_column`` table if it has not yet been populated.
    """
    bayesdb_table_guarantee_columns(bdb, table)
    sql = '''
        SELECT colno FROM bayesdb_column WHERE tabname = ? AND name = ?
    '''
    cursor = bdb.sql_execute(sql, (table, name))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such column in table %s: %s' %
            (repr(table), repr(name)))
    else:
        return row[0]

def bayesdb_table_guarantee_columns(bdb, table):
    """Make sure ``bayesdb_column`` is populated with columns of `table`.

    `bdb` must have a table named `table`.  If you're not sure, call
    :func:`bayesdb_has_table` first.
    """
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

def bayesdb_table_has_implicit_population(bdb, table):
    """True if the table named `table` has an implicit population.

    `bdb` must have a table named `table`.  If you are not sure, call
    :func:`bayesdb_has_table` first.
    """
    sql = 'SELECT implicit FROM bayesdb_population WHERE tabname = ?'
    cursor = bdb.sql_execute(sql, (table,))
    try:
        row = cursor.next()
    except StopIteration:
        return False
    (result,) = row
    assert result in [0, 1]
    if result == 1:
        try:
            row = cursor.next()
        except StopIteration:
            return True
        assert False
    return False

def bayesdb_table_populations(bdb, table):
    """Return list of populations for `table`.

    `bdb` must have a table named `table`.  If you're not sure, call
    :func:`bayesdb_has_table` first.
    """
    cursor = bdb.sql_execute('''
        SELECT id FROM bayesdb_population WHERE tabname = ?
    ''', (table,))
    return [population_id for (population_id,) in cursor]

def bayesdb_table_has_rowid(bdb, table, rowid):
    """True if the table named `table` has record with given rowid.

    `bdb` must have a table named `table`.  If you're not sure, call
    :func:`bayesdb_has_table` first.
    """
    qt = sqlite3_quote_name(table)
    sql = 'SELECT COUNT(*) FROM %s WHERE oid = ?'
    cursor = bdb.sql_execute(sql % (qt,), (rowid,))
    return cursor_value(cursor) != 0

def bayesdb_has_population(bdb, name):
    """True if there is a population named `name` in `bdb`."""
    sql = 'SELECT COUNT(*) FROM bayesdb_population WHERE name = ?'
    return 0 != cursor_value(bdb.sql_execute(sql, (name,)))

def bayesdb_get_population(bdb, name):
    """Return the id of the population named `name` in `bdb`.

    The id is persistent across savepoints: ids are 64-bit integers
    that increase monotonically and are never reused.

    `bdb` must have a population named `name`.  If you're not sure,
    call :func:`bayesdb_has_population` first.
    """
    sql = 'SELECT id FROM bayesdb_population WHERE name = ?'
    cursor = bdb.sql_execute(sql, (name,))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such population: %r' % (repr(name),))
    else:
        assert isinstance(row[0], int)
        return row[0]

def bayesdb_population_name(bdb, population_id):
    """Return the name of the population with given `population_id`."""
    sql = 'SELECT name FROM bayesdb_population WHERE id = ?'
    cursor = bdb.sql_execute(sql, (population_id,))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such population id: %r' % (repr(population_id),))
    else:
        return row[0]

def bayesdb_population_table(bdb, population_id):
    """Return the name of table of the population with id `id`."""
    sql = 'SELECT tabname FROM bayesdb_population WHERE id = ?'
    cursor = bdb.sql_execute(sql, (population_id,))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such population id: %r' % (repr(population_id),))
    else:
        return row[0]

def bayesdb_population_generators(bdb, population_id):
    """Return list of generators for population_id."""
    cursor = bdb.sql_execute('''
        SELECT id FROM bayesdb_generator WHERE population_id = ?
    ''', (population_id,))
    return [generator_id for (generator_id,) in cursor]

def bayesdb_population_is_implicit(bdb, population_id):
    """True if the population with id `id` is implicit."""
    sql = 'SELECT implicit FROM bayesdb_population WHERE id = ?'
    cursor = bdb.sql_execute(sql, (population_id,))
    try:
        (result,) = cursor.next()
    except StopIteration:
        raise ValueError('No such population id: %r' % (repr(population_id),))
    else:
        assert result in [0, 1]
        return result == 1

def bayesdb_population_has_implicit_generator(bdb, population_id):
    """True if `population_id` has an implicit generator."""
    sql = '''
        SELECT bayesdb_generator.implicit FROM
            bayesdb_population
            LEFT OUTER JOIN bayesdb_generator
                ON (bayesdb_population.id = bayesdb_generator.population_id)
            WHERE bayesdb_population.id = ?
    '''
    cursor = bdb.sql_execute(sql, (population_id,))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such population id: %r' % (repr(population_id),))
    (result,) = row
    assert result in [0, 1, None]
    # result = None -> population has no generators.
    # result = 1    -> population has an implicit generator.
    # In both cases, confirm query yields a cursor with single row.
    if result in [None, 1]:
        try:
            row = cursor.next()
        except StopIteration:
            return False if result is None else True
        assert False
    return False

def bayesdb_add_variable(bdb, population_id, name, stattype):
    """Adds a variable to the population, with colno from the base table."""
    table_name = bayesdb_population_table(bdb, population_id)
    colno = bayesdb_table_column_number(bdb, table_name, name)
    bdb.sql_execute('''
        INSERT INTO bayesdb_variable
            (population_id, name, colno, stattype)
            VALUES (?, ?, ?, ?)
    ''', (population_id, name, colno, stattype))

def bayesdb_has_variable(bdb, population_id, generator_id, name):
    """True if the population has a given variable.

    generator_id is None for manifest variables and the id of a
    generator for variables that may be latent.
    """
    cursor = bdb.sql_execute('''
        SELECT COUNT(*) FROM bayesdb_variable
            WHERE population_id = ?
                AND (generator_id IS NULL OR generator_id = ?)
                AND name = ?
    ''', (population_id, generator_id, name))
    return cursor_value(cursor) != 0

def bayesdb_variable_number(bdb, population_id, generator_id, name):
    """Return the column number of a population variable."""
    cursor = bdb.sql_execute('''
        SELECT colno FROM bayesdb_variable
            WHERE population_id = ?
                AND (generator_id IS NULL OR generator_id = ?)
                AND name = ?
    ''', (population_id, generator_id, name))
    return cursor_value(cursor)

def bayesdb_variable_names(bdb, population_id, generator_id):
    """Return a list of the names of columns modeled in `population_id`."""
    colnos = bayesdb_variable_numbers(bdb, population_id, generator_id)
    return [bayesdb_variable_name(bdb, population_id, generator_id, colno)
        for colno in colnos]

def bayesdb_variable_numbers(bdb, population_id, generator_id):
    """Return a list of the numbers of columns modeled in `population_id`."""
    cursor = bdb.sql_execute('''
        SELECT colno FROM bayesdb_variable
            WHERE population_id = ?
                AND (generator_id IS NULL OR generator_id = ?)
            ORDER BY colno ASC
    ''', (population_id, generator_id))
    return [colno for (colno,) in cursor]

def bayesdb_variable_name(bdb, population_id, generator_id, colno):
    """Return the name of a population variable."""
    cursor = bdb.sql_execute('''
        SELECT name FROM bayesdb_variable
            WHERE population_id = ?
                AND (generator_id IS NULL OR generator_id = ?)
                AND colno = ?
    ''', (population_id, generator_id, colno))
    return cursor_value(cursor)

def bayesdb_colno_to_variable_names(bdb, population_id, generator_id):
    """Return a dictionary that maps column number to variable name in population."""
    cursor = bdb.sql_execute('''
        SELECT colno, name FROM bayesdb_variable
            WHERE population_id = ?
                AND (generator_id IS NULL OR generator_id = ?)
    ''', (population_id, generator_id))
    return {colno: name for (colno, name) in cursor}

def bayesdb_variable_stattype(bdb, population_id, generator_id, colno):
    """Return the statistical type of a population variable."""
    sql = '''
        SELECT stattype FROM bayesdb_variable
            WHERE population_id = ?
                AND (generator_id IS NULL OR generator_id = ?)
                AND colno = ?
    '''
    cursor = bdb.sql_execute(sql, (population_id, generator_id, colno))
    try:
        row = cursor.next()
    except StopIteration:
        population = bayesdb_population_name(bdb, population_id)
        sql = '''
            SELECT COUNT(*)
                FROM bayesdb_population AS p, bayesdb_column AS c
                WHERE p.id = :population_id
                    AND p.tabname = c.tabname
                    AND c.colno = :colno
        '''
        cursor = bdb.sql_execute(sql, {
            'population_id': population_id,
            'colno': colno,
        })
        if cursor_value(cursor) == 0:
            raise ValueError('No such variable in population %s: %d'
                % (population, colno))
        else:
            raise ValueError('Variable not modeled in population %s: %d'
                % (population, colno))
    else:
        assert len(row) == 1
        return row[0]

def bayesdb_add_latent(bdb, population_id, generator_id, var, stattype):
    """Add a generator's latent variable to a population.

    NOTE: To be used ONLY by a backend's create_generator method
    when establishing any latent variables of that generator.
    """
    with bdb.savepoint():
        cursor = bdb.sql_execute('''
            SELECT MIN(colno) FROM bayesdb_variable WHERE population_id = ?
        ''', (population_id,))
        colno = min(-1, cursor_value(cursor) - 1)
        bdb.sql_execute('''
            INSERT INTO bayesdb_variable
                (population_id, generator_id, colno, name, stattype)
                VALUES (?, ?, ?, ?, ?)
        ''', (population_id, generator_id, colno, var, stattype))
        return colno

def bayesdb_has_latent(bdb, population_id, var):
    """True if the population has a latent variable by the given name."""
    cursor = bdb.sql_execute('''
        SELECT COUNT(*) FROM bayesdb_variable
            WHERE population_id = ? AND name = ? AND generator_id IS NOT NULL
    ''', (population_id, var))
    return cursor_value(cursor)

def bayesdb_population_cell_value(bdb, population_id, rowid, colno):
    """Return value stored in `rowid` and `colno` of given `population_id`."""
    if colno < 0:
        # Latent variables do not appear in the table.
        return None
    table_name = bayesdb_population_table(bdb, population_id)
    var = bayesdb_variable_name(bdb, population_id, None, colno)
    qt = sqlite3_quote_name(table_name)
    qv = sqlite3_quote_name(var)
    value_sql = 'SELECT %s FROM %s WHERE _rowid_ = ?' % (qv, qt)
    value_cursor = bdb.sql_execute(value_sql, (rowid,))
    value = None
    try:
        row = value_cursor.next()
    except StopIteration:
        population = bayesdb_population_name(bdb, population_id)
        raise BQLError(bdb, 'No such individual in population %r: %d'
            % (population, rowid))
    else:
        assert len(row) == 1
        value = row[0]
    return value

def bayesdb_population_fresh_row_id(bdb, population_id):
    """Return one plus maximum rowid in base table of given `population_id`."""
    table_name = bayesdb_population_table(bdb, population_id)
    qt = sqlite3_quote_name(table_name)
    cursor = bdb.sql_execute('SELECT MAX(_rowid_) FROM %s' % (qt,))
    max_rowid = cursor_value(cursor)
    if max_rowid is None:
        max_rowid = 0
    return max_rowid + 1   # Synthesize a non-existent SQLite row id

def bayesdb_has_generator(bdb, population_id, name):
    """True if there is a generator named `name` in `bdb`.

    If `population_id` is specified, then the generator with `name` needs to be
    defined for that population. Otherwise, when `population_id` is None, the
    `name` may be of any generator.
    """
    if population_id is None:
        sql = 'SELECT COUNT(*) FROM bayesdb_generator WHERE name = ?'
        cursor = bdb.sql_execute(sql, (name,))
    else:
        sql = '''
            SELECT COUNT(*) FROM bayesdb_generator
                WHERE name = ? AND population_id = ?
        '''
        cursor = bdb.sql_execute(sql, (name, population_id))
    return 0 != cursor_value(cursor)

def bayesdb_get_generator(bdb, population_id, name):
    """Return the id of the generator named `name` in `bdb`.

    The generator id is persistent across savepoints: ids are 64-bit integers
    that increase monotonically and are never reused.

    `bdb` must have a generator named `name`.  If you're not sure,
    call :func:`bayesdb_has_generator` first.
    """
    if population_id is None:
        sql = 'SELECT id FROM bayesdb_generator WHERE name = ?'
        cursor = bdb.sql_execute(sql, (name,))
    else:
        sql = '''
            SELECT id FROM bayesdb_generator
                WHERE name = ? AND population_id = ?
        '''
        cursor = bdb.sql_execute(sql, (name, population_id))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such generator: %s' % (repr(name),))
    else:
        assert isinstance(row[0], int)
        return row[0]

def bayesdb_generator_name(bdb, generator_id):
    """Return the name of the generator with given `generator_id`."""
    sql = 'SELECT name FROM bayesdb_generator WHERE id = ?'
    cursor = bdb.sql_execute(sql, (generator_id,))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such generator id: %r' % (repr(generator_id),))
    else:
        return row[0]

def bayesdb_generator_backend(bdb, generator_id):
    """Return the backend of the generator with given `generator_id`."""
    sql = 'SELECT backend FROM bayesdb_generator WHERE id = ?'
    cursor = bdb.sql_execute(sql, (generator_id,))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such generator: %s' % (repr(generator_id),))
    else:
        if row[0] not in bdb.backends:
            name = bayesdb_generator_name(bdb, generator_id)
            raise ValueError('Backend of generator %s not registered: %s' %
                (repr(name), repr(row[0])))
        return bdb.backends[row[0]]

def bayesdb_generator_table(bdb, generator_id):
    """Return name of table of the generator with given `generator_id`."""
    population_id = bayesdb_generator_population(bdb, generator_id)
    return bayesdb_population_table(bdb, population_id)

def bayesdb_generator_population(bdb, generator_id):
    """Return id of population of the generator with given `generator_id`."""
    sql = 'SELECT population_id FROM bayesdb_generator WHERE id = ?'
    cursor = bdb.sql_execute(sql, (generator_id,))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such generator: %s' % (repr(generator_id),))
    else:
        assert len(row) == 1
        return row[0]

def bayesdb_generator_is_implicit(bdb, generator_id):
    """True if the generator with given `generator_id` is implicit."""
    sql = 'SELECT implicit FROM bayesdb_generator WHERE id = ?'
    cursor = bdb.sql_execute(sql, (generator_id,))
    try:
        (result,) = cursor.next()
    except StopIteration:
        raise ValueError('No such generator id: %r' % (repr(generator_id),))
    else:
        assert result in [0, 1]
        return result == 1

def bayesdb_generator_has_model(bdb, generator_id, modelno):
    """True if `generator_id` has a model numbered `modelno`."""
    sql = '''
        SELECT COUNT(*) FROM bayesdb_generator_model AS m
            WHERE generator_id = ? AND modelno = ?
    '''
    return cursor_value(bdb.sql_execute(sql, (generator_id, modelno)))

def bayesdb_generator_modelnos(bdb, generator_id):
    """Return list of model numbers associated with given `generator_id`."""
    sql = '''
        SELECT modelno FROM bayesdb_generator_model AS m
            WHERE generator_id = ?
            ORDER BY modelno ASC
    '''
    return [row[0] for row in bdb.sql_execute(sql, (generator_id,))]

def bayesdb_population_row_values(bdb, population_id, rowid):
    """Return values stored in `rowid` of given `population_id`."""
    table_name = bayesdb_population_table(bdb, population_id)
    column_names = bayesdb_variable_names(bdb, population_id, None)
    qt = sqlite3_quote_name(table_name)
    qcns = ','.join(map(sqlite3_quote_name, column_names))
    select_sql = ('SELECT %s FROM %s WHERE oid = ?' % (qcns, qt))
    cursor = bdb.sql_execute(select_sql, (rowid,))
    row = None
    try:
        row = cursor.next()
    except StopIteration:
        population = bayesdb_population_table(bdb, population_id)
        raise BQLError(bdb, 'No such row in table %s for population %s: %d'
            % (repr(table_name), repr(population), rowid))
    try:
        cursor.next()
    except StopIteration:
        pass
    else:
        population = bayesdb_population_table(bdb, population_id)
        raise BQLError(bdb,
            'More than one such row in table %s for population %s: %d'
            % (repr(table_name), repr(population), rowid))
    return row

def bayesdb_rowid_tokens(bdb):
    """Return list of built-in tokens that identify rowids (e.g. oid)."""
    tokens = bdb.sql_execute('''
        SELECT token FROM bayesdb_rowid_tokens
    ''').fetchall()
    return [t[0] for t in tokens]

def bayesdb_has_stattype(bdb, stattype):
    """True if `stattype` is registered in `bdb` instance."""
    sql = 'SELECT COUNT(*) FROM bayesdb_stattype WHERE name = :stattype'
    cursor = bdb.sql_execute(sql, {'stattype': casefold(stattype)})
    return cursor_value(cursor) > 0
