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

"""Heuristic guessing of statistical types based on data.

The heuristics implemented here are ad-hoc, and do not implement any
sort of Bayesian model selection.  They are based on crude attempts to
parse data as numbers, and on fixed parameters for distinguishing
categorical and numerical data.  No columns are ever guessed to be
cyclic.
"""

import math

import bayeslite.core as core

from bayeslite.sqlite3_util import sqlite3_quote_name
from bayeslite.util import casefold
from bayeslite.util import unique

def bayesdb_guess_generator(bdb, generator, table, metamodel,
        ifnotexists=None, count_cutoff=None, ratio_cutoff=None,
        default=None, overrides=None):
    """Heuristically guess a generator for `table` using `metamodel`.

    Based on the data in `table`, create a generator named `generator`
    using `metamodel` for it.

    :param bool ifnotexists: if true or ``None`` and `generator`
        already exists, do nothing.
    :param int count_cutoff: number of distinct values below which
        columns whose values can all be parsed as numbers will be
        considered categorical anyway
    :param real ratio_cutoff: ratio of distinct values to total values
        below which columns whose values can all be parsed as numbers
        will be considered categorical anyway
    :param list overrides: list of ``(name, stattype)``, overriding
        any guessed statistical type for columns by those names

    In addition to statistical types, the overrides may specify
    ``key`` or ``ignore``, in which case those columns will not be
    modelled at all.
    """

    # Fill in default arguments.
    if ifnotexists is None:
        ifnotexists = False
    if default is None:
        default = False

    with bdb.savepoint():
        if core.bayesdb_has_generator(bdb, generator):
            if ifnotexists:
                return
            else:
                raise ValueError('Generator already exists: %s' %
                    (repr(generator),))
        qt = sqlite3_quote_name(table)
        cursor = bdb.sql_execute('SELECT * FROM %s' % (qt,))
        column_names = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        stattypes = bayesdb_guess_stattypes(column_names, rows,
            count_cutoff=count_cutoff, ratio_cutoff=ratio_cutoff,
            overrides=overrides)
        # Skip the key column.
        column_names, stattypes = \
            unzip([(cn, st) for cn, st in zip(column_names, stattypes)
                if st != 'key' and st != 'ignore'])
        if len(column_names) == 0:
            raise ValueError('Table has no modelled columns: %s' %
                (repr(table),))
        qg = sqlite3_quote_name(generator)
        qmm = sqlite3_quote_name(metamodel)
        qcns = map(sqlite3_quote_name, column_names)
        qsts = map(sqlite3_quote_name, stattypes)
        qs = ','.join(qcn + ' ' + qst for qcn, qst in zip(qcns, qsts))
        bdb.execute('CREATE %sGENERATOR %s FOR %s USING %s(%s)' %
            ('DEFAULT ' if default else '', qg, qt, qmm, qs))

def unzip(l):                   # ???
    xs = []
    ys = []
    for x, y in l:
        xs.append(x)
        ys.append(y)
    return xs, ys

def bayesdb_guess_stattypes(column_names, rows,
        count_cutoff=None, ratio_cutoff=None, overrides=None):
    """Heuristically guess statistical types for the data in `rows`.

    Return a list of statistical types corresponding to the columns
    named in the list `column_names`.

    :param int count_cutoff: number of distinct values below which
        columns whose values can all be parsed as numbers will be
        considered categorical anyway
    :param real ratio_cutoff: ratio of distinct values to total values
        below which columns whose values can all be parsed as numbers
        will be considered categorical anyway
    :param list overrides: list of ``(name, stattype)``, overriding
        any guessed statistical type for columns by those names

    In addition to statistical types, the overrides may specify
    ``key`` or ``ignore``.
    """

    # Fill in default arguments.
    if count_cutoff is None:
        count_cutoff = 20
    if ratio_cutoff is None:
        ratio_cutoff = 0.02
    if overrides is None:
        overrides = []

    # Build a set of the column names.
    column_name_set = set()
    duplicates = set()
    for name in column_names:
        if casefold(name) in column_name_set:
            duplicates.add(name)
        column_name_set.add(casefold(name))
    if 0 < len(duplicates):
        raise ValueError('Duplicate column names: %s' %
            (repr(list(duplicates),)))

    # Build a map for the overrides.
    #
    # XXX Support more than just stattype: allow arbitrary column
    # descriptions.
    override_map = {}
    unknown = set()
    duplicates = set()
    for name, stattype in overrides:
        if casefold(name) not in column_name_set:
            unknown.add(name)
            continue
        if casefold(name) in override_map:
            duplicates.add(name)
            continue
        override_map[casefold(name)] = casefold(stattype)
    if 0 < len(unknown):
        raise ValueError('Unknown columns overridden: %s' %
            (repr(list(unknown)),))
    if 0 < len(duplicates):
        raise ValueError('Duplicate columns overridden: %s' %
            (repr(list(duplicates)),))

    # Sanity-check the inputs.
    ncols = len(column_names)
    assert ncols == len(unique(map(casefold, column_names)))
    for ri, row in enumerate(rows):
        if len(row) < ncols:
            raise ValueError('Row %d: Too few columns: %d < %d' %
                (ri, len(row), ncols))
        if len(row) > ncols:
            raise ValueError('Row %d: Too many columns: %d > %d' %
                (ri, len(row), ncols))

    # Find a key first, if it has been specified as an override.
    key = None
    duplicate_keys = set()
    for ci, column_name in enumerate(column_names):
        if casefold(column_name) in override_map:
            if override_map[casefold(column_name)] == 'key':
                if key is not None:
                    duplicate_keys.add(column_name)
                    continue
                column = integerify(rows, ci)
                if not column:
                    column = [row[ci] for row in rows]
                if not keyable_p(column):
                    raise ValueError('Column non-unique but specified as key'
                        ': %s' % (repr(column_name),))
                key = column_name
    if 0 < len(duplicate_keys):
        raise ValueError('Multiple columns overridden as keys: %s' %
            (repr(list(duplicate_keys)),))

    # Now go through and guess the other column stattypes or use the
    # override.
    stattypes = []
    for ci, column_name in enumerate(column_names):
        if casefold(column_name) in override_map:
            stattype = override_map[casefold(column_name)]
        else:
            numericable = True
            column = integerify(rows, ci)
            if not column:
                column = floatify(rows, ci)
                if not column:
                    column = [row[ci] for row in rows]
                    numericable = False
            if key is None and keyable_p(column):
                stattype = 'key'
                key = column_name
            elif numericable and \
                 numerical_p(column, count_cutoff, ratio_cutoff):
                stattype = 'numerical'
            else:
                stattype = 'categorical'
        stattypes.append(stattype)
    return stattypes

def integerify(rows, ci):
    column = [0] * len(rows)
    try:
        for ri, row in enumerate(rows):
            if isinstance(row[ci], float):
                return None
            column[ri] = int(row[ci])
    except (ValueError, TypeError):
        return None
    return column

def floatify(rows, ci):
    column = [0] * len(rows)
    try:
        for ri, row in enumerate(rows):
            if row[ci] is None or row[ci] == '':
                column[ri] = float('NaN')
            else:
                column[ri] = float(row[ci])
    except (ValueError, TypeError):
        return None
    return column

def keyable_p(column):
    # `unique' can't cope with NaNs, so reject them early.
    if any(v is None or (isinstance(v, float) and math.isnan(v))
           for v in column):
        return False
    return len(column) == len(unique(column))

def numerical_p(column, count_cutoff, ratio_cutoff):
    nu = len(unique([v for v in column if not math.isnan(v)]))
    if nu <= count_cutoff:
        return False
    if float(nu) / float(len(column)) <= ratio_cutoff:
        return False
    return True
