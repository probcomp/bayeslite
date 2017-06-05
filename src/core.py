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

Each table may optionally be modelled by any number of generators,
representing a parametrized generative model for the table's data,
according to a named metamodel.

Each generator models a subset of the columns in its table, which are
called the modelled columns of that generator.  Each column in a
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

    The table need not be modelled.
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
        raise ValueError('No such population: %r' % (name,))
    else:
        assert isinstance(row[0], int)
        return row[0]

def bayesdb_population_name(bdb, id):
    """Return the name of the population with id `id`."""
    sql = 'SELECT name FROM bayesdb_population WHERE id = ?'
    cursor = bdb.sql_execute(sql, (id,))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such population id: %r' % (id,))
    else:
        return row[0]

def bayesdb_population_table(bdb, id):
    """Return the name of table of the population with id `id`."""
    sql = 'SELECT tabname FROM bayesdb_population WHERE id = ?'
    cursor = bdb.sql_execute(sql, (id,))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such population id: %r' % (id,))
    else:
        return row[0]

def bayesdb_population_generators(bdb, population_id):
    cursor = bdb.sql_execute('''
        SELECT id FROM bayesdb_generator WHERE population_id = ?
    ''', (population_id,))
    return [generator_id for (generator_id,) in cursor]

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
    """Return a list of the names of columns modelled in `population_id`."""
    colnos = bayesdb_variable_numbers(bdb, population_id, generator_id)
    return [bayesdb_variable_name(bdb, population_id, colno)
        for colno in colnos]

def bayesdb_variable_numbers(bdb, population_id, generator_id):
    """Return a list of the numbers of columns modelled in `population_id`."""
    cursor = bdb.sql_execute('''
        SELECT colno FROM bayesdb_variable
            WHERE population_id = ?
                AND (generator_id IS NULL OR generator_id = ?)
            ORDER BY colno ASC
    ''', (population_id, generator_id))
    return [colno for (colno,) in cursor]

def bayesdb_variable_name(bdb, population_id, colno):
    """Return the name a population variable."""
    cursor = bdb.sql_execute('''
        SELECT name FROM bayesdb_variable WHERE population_id = ? AND colno = ?
    ''', (population_id, colno))
    return cursor_value(cursor)

def bayesdb_variable_stattype(bdb, population_id, colno):
    """Return the statistical type of a population variable."""
    sql = '''
        SELECT stattype FROM bayesdb_variable
            WHERE population_id = ? AND colno = ?
    '''
    cursor = bdb.sql_execute(sql, (population_id, colno))
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
            raise ValueError('No such variable in population %s: %d' %
                (population, colno))
        else:
            raise ValueError('Variable not modelled in population %s: %d' %
                (population, colno))
    else:
        assert len(row) == 1
        return row[0]

def bayesdb_add_latent(bdb, population_id, generator_id, var, stattype):
    """Add a generator's latent variable to a population.

    NOTE: To be used ONLY by a metamodel's create_generator method
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
    if colno < 0:
        # Latent variables do not appear in the table.
        return None
    table_name = bayesdb_population_table(bdb, population_id)
    var = bayesdb_variable_name(bdb, population_id, colno)
    qt = sqlite3_quote_name(table_name)
    qv = sqlite3_quote_name(var)
    value_sql = 'SELECT %s FROM %s WHERE _rowid_ = ?' % (qv, qt)
    value_cursor = bdb.sql_execute(value_sql, (rowid,))
    value = None
    try:
        row = value_cursor.next()
    except StopIteration:
        population = bayesdb_population_name(bdb, population_id)
        raise BQLError(bdb, 'No such invidual in population %r: %d' %
            (population, rowid))
    else:
        assert len(row) == 1
        value = row[0]
    return value

def bayesdb_population_fresh_row_id(bdb, population_id):
    table_name = bayesdb_population_table(bdb, population_id)
    qt = sqlite3_quote_name(table_name)
    cursor = bdb.sql_execute('SELECT MAX(_rowid_) FROM %s' % (qt,))
    max_rowid = cursor_value(cursor)
    if max_rowid is None:
        max_rowid = 0
    return max_rowid + 1   # Synthesize a non-existent SQLite row id

def bayesdb_has_generator(bdb, population_id, name):
    """True if there is a generator named `name` in `bdb`."""
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

    The id is persistent across savepoints: ids are 64-bit integers
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

def bayesdb_generator_name(bdb, id):
    """Return the name of the generator with id `id`."""
    sql = 'SELECT name FROM bayesdb_generator WHERE id = ?'
    cursor = bdb.sql_execute(sql, (id,))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such generator id: %r' % (id,))
    else:
        return row[0]

def bayesdb_generator_metamodel(bdb, id):
    """Return the metamodel of the generator with id `id`."""
    sql = 'SELECT metamodel FROM bayesdb_generator WHERE id = ?'
    cursor = bdb.sql_execute(sql, (id,))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such generator: %s' % (repr(id),))
    else:
        if row[0] not in bdb.metamodels:
            name = bayesdb_generator_name(bdb, id)
            raise ValueError('Metamodel of generator %s not registered: %s' %
                (repr(name), repr(row[0])))
        return bdb.metamodels[row[0]]

def bayesdb_generator_table(bdb, id):
    """Return the name of the table of the generator with id `id`."""
    sql = 'SELECT tabname FROM bayesdb_generator WHERE id = ?'
    cursor = bdb.sql_execute(sql, (id,))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such generator: %s' % (repr(id),))
    else:
        assert len(row) == 1
        return row[0]

def bayesdb_generator_population(bdb, id):
    """Return the id of the population of the generator with id `id`."""
    sql = 'SELECT population_id FROM bayesdb_generator WHERE id = ?'
    cursor = bdb.sql_execute(sql, (id,))
    try:
        row = cursor.next()
    except StopIteration:
        raise ValueError('No such generator: %s' % (repr(id),))
    else:
        assert len(row) == 1
        return row[0]

def bayesdb_generator_column_names(bdb, generator_id):
    """Return a list of names of columns modelled by `generator_id`."""
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
    # str because column names can't contain Unicode in sqlite3.
    return [str(row[0]) for row in bdb.sql_execute(sql, (generator_id,))]

def bayesdb_generator_column_stattype(bdb, generator_id, colno):
    """Return the statistical type of the column `colno` in `generator_id`."""
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
                WHERE g.id = :generator_id
                    AND g.tabname = c.tabname
                    AND c.colno = :colno
        '''
        cursor = bdb.sql_execute(sql, {
            'generator_id': generator_id,
            'colno': colno,
        })
        if cursor_value(cursor) == 0:
            raise ValueError('No such column in generator %s: %d' %
                (generator, colno))
        else:
            raise ValueError('Column not modelled in generator %s: %d' %
                (generator, colno))
    else:
        assert len(row) == 1
        return row[0]

def bayesdb_generator_has_column(bdb, generator_id, column_name):
    """True if `generator_id` models a column named `name`."""
    sql = '''
        SELECT COUNT(*)
            FROM bayesdb_generator AS g,
                bayesdb_generator_column as gc,
                bayesdb_column AS c
            WHERE g.id = :generator_id AND c.name = :column_name
                AND g.id = gc.generator_id
                AND g.tabname = c.tabname
                AND gc.colno = c.colno
    '''
    cursor = bdb.sql_execute(sql, {
        'generator_id': generator_id,
        'column_name': column_name,
    })
    return cursor_value(cursor)

def bayesdb_generator_column_name(bdb, generator_id, colno):
    """Return the name of the column numbered `colno` in `generator_id`."""
    sql = '''
        SELECT c.name
            FROM bayesdb_generator AS g,
                bayesdb_generator_column AS gc,
                bayesdb_column AS c
            WHERE g.id = :generator_id
                AND gc.colno = :colno
                AND g.id = gc.generator_id
                AND g.tabname = c.tabname
                AND gc.colno = c.colno
    '''
    cursor = bdb.sql_execute(sql, {
        'generator_id': generator_id,
        'colno': colno,
    })
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
    """Return the number of the column `column_name` in `generator_id`."""
    sql = '''
        SELECT c.colno
            FROM bayesdb_generator AS g,
                bayesdb_generator_column AS gc,
                bayesdb_column AS c
            WHERE g.id = :generator_id AND c.name = :column_name
                AND g.id = gc.generator_id
                AND g.tabname = c.tabname
                AND gc.colno = c.colno
    '''
    cursor = bdb.sql_execute(sql, {
        'generator_id': generator_id,
        'column_name': column_name,
    })
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
    """Return a list of the numbers of columns modelled in `generator_id`."""
    sql = '''
        SELECT colno FROM bayesdb_generator_column
            WHERE generator_id = ?
            ORDER BY colno ASC
    '''
    return [row[0] for row in bdb.sql_execute(sql, (generator_id,))]

def bayesdb_generator_has_model(bdb, generator_id, modelno):
    """True if `generator_id` has a model numbered `modelno`."""
    sql = '''
        SELECT COUNT(*) FROM bayesdb_generator_model AS m
            WHERE generator_id = ? AND modelno = ?
    '''
    return cursor_value(bdb.sql_execute(sql, (generator_id, modelno)))

def bayesdb_generator_modelnos(bdb, generator_id):
    sql = '''
        SELECT modelno FROM bayesdb_generator_model AS m
            WHERE generator_id = ?
            ORDER BY modelno ASC
    '''
    return [row[0] for row in bdb.sql_execute(sql, (generator_id,))]

def bayesdb_generator_cell_value(bdb, generator_id, rowid, colno):
    table_name = bayesdb_generator_table(bdb, generator_id)
    colname = bayesdb_generator_column_name(bdb, generator_id, colno)
    qt = sqlite3_quote_name(table_name)
    qcn = sqlite3_quote_name(colname)
    value_sql = 'SELECT %s FROM %s WHERE _rowid_ = ?' % (qcn, qt)
    value_cursor = bdb.sql_execute(value_sql, (rowid,))
    value = None
    try:
        row = value_cursor.next()
    except StopIteration:
        generator = bayesdb_generator_name(bdb, generator_id)
        raise BQLError(bdb, 'No such row in %s: %d' %
            (repr(generator), rowid))
    else:
        assert len(row) == 1
        value = row[0]
    return value

def bayesdb_population_row_values(bdb, population_id, rowid):
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
        raise BQLError(bdb,
            'No such row in table %s for population %d: %d' %
            (repr(table_name), repr(population), repr(rowid)))
    try:
        cursor.next()
    except StopIteration:
        pass
    else:
        population = bayesdb_population_table(bdb, population_id)
        raise BQLError(bdb,
            'More than one such row in table %s for population %s: %d' %
            (repr(table_name), repr(population), repr(rowid)))
    return row

def bayesdb_generator_row_values(bdb, generator_id, rowid):
    table_name = bayesdb_generator_table(bdb, generator_id)
    column_names = bayesdb_generator_column_names(bdb, generator_id)
    qt = sqlite3_quote_name(table_name)
    qcns = ','.join(map(sqlite3_quote_name, column_names))
    select_sql = ('SELECT %s FROM %s WHERE _rowid_ = ?' % (qcns, qt))
    cursor = bdb.sql_execute(select_sql, (rowid,))
    row = None
    try:
        row = cursor.next()
    except StopIteration:
        generator = bayesdb_generator_table(bdb, generator_id)
        raise BQLError(bdb, 'No such row in table %s'
            ' for generator %d: %d' %
            (repr(table_name), repr(generator), repr(rowid)))
    try:
        cursor.next()
    except StopIteration:
        pass
    else:
        generator = bayesdb_generator_table(bdb, generator_id)
        raise BQLError(bdb, 'More than one such row'
            ' in table %s for generator %s: %d' %
            (repr(table_name), repr(generator), repr(rowid)))
    return row

def bayesdb_generator_fresh_row_id(bdb, generator_id):
    table_name = bayesdb_generator_table(bdb, generator_id)
    qt = sqlite3_quote_name(table_name)
    cursor = bdb.sql_execute('SELECT MAX(_rowid_) FROM %s' % (qt,))
    max_rowid = cursor_value(cursor)
    if max_rowid is None:
        max_rowid = 0
    return max_rowid + 1   # Synthesize a non-existent SQLite row id

def bayesdb_rowid_tokens(bdb):
    tokens = bdb.sql_execute('''
        SELECT token FROM bayesdb_rowid_tokens
    ''').fetchall()
    return [t[0] for t in tokens]

def bayesdb_has_stattype(bdb, stattype):
    sql = 'SELECT COUNT(*) FROM bayesdb_stattype WHERE name = :stattype'
    cursor = bdb.sql_execute(sql, {'stattype': casefold(stattype)})
    return cursor_value(cursor) > 0

# XXX This should be stored in the database by adding a column to the
# bayesdb_stattype table -- when we are later willing to contemplate
# adding statistical types, e.g. COUNT, SCALE, or NONNEGATIVE REAL.
_STATTYPE_TO_AFFINITY = dict((casefold(st), casefold(af)) for st, af in (
    ('categorical', 'text'),
    ('cyclic', 'real'),
    ('numerical', 'real'),
    ('counts', 'real'),
    ('magnitude', 'real'),
    ('nominal', 'text'),
    ('numericalranged', 'real'),
))
def bayesdb_stattype_affinity(_bdb, stattype):
    assert bayesdb_has_stattype(_bdb, stattype)
    return _STATTYPE_TO_AFFINITY[casefold(stattype)]
