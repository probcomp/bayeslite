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

"""Heuristic guessing of statistical types based on data.

The heuristics implemented here are ad-hoc, and do not implement any
sort of Bayesian model selection.  They are based on crude attempts to
parse data as numbers, and on fixed parameters for distinguishing
nominal and numerical data.  No columns are ever guessed to be
cyclic.
"""

import collections
import math
import os

import bayeslite.core as core

from bayeslite.exception import BQLError
from bayeslite.sqlite3_util import sqlite3_quote_name
from bayeslite.util import casefold
from bayeslite.util import unique

def bayesdb_guess_population(bdb, population, table,
        ifnotexists=None, **kwargs):
    """Heuristically guess a population schema for `table`.

    Based on the data in `table`, create a population named
    `population`.

    :param bool ifnotexists: if true or ``None`` and `population`
        already exists, do nothing.
    :param dict kwargs: options to pass through to bayesdb_guess_stattypes.

    In addition to statistical types, the overrides may specify
    ``key`` or ``ignore``, in which case those columns will not be
    modelled at all.

    """

    # Fill in default arguments.
    if ifnotexists is None:
        ifnotexists = False

    with bdb.savepoint():
        if core.bayesdb_has_population(bdb, population):
            if ifnotexists:
                return
            else:
                raise ValueError('Population exists: %r' % (population,))
        qt = sqlite3_quote_name(table)
        cursor = bdb.sql_execute('SELECT * FROM %s' % (qt,))
        column_names = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        stattypes = [st[0] for st in
            bayesdb_guess_stattypes(column_names, rows, **kwargs)]
        # Convert the `key` column to an `ignore`.
        replace = lambda s: 'ignore' if s == 'key' else s
        column_names, stattypes = unzip([
            (cn, replace(st)) for cn, st in zip(
                column_names, stattypes)
        ])
        if len([s for s in stattypes if s != 'ignore']) == 0:
            raise ValueError(
                'Table has no modelled columns: %s'
                % (repr(table),))
        qp = sqlite3_quote_name(population)
        qcns = map(sqlite3_quote_name, column_names)
        qsts = map(sqlite3_quote_name, stattypes)
        qs = ';'.join(qcn + ' ' + qst for qcn, qst in zip(qcns, qsts))
        bdb.execute('CREATE POPULATION %s FOR %s(%s)' % (qp, qt, qs))

def unzip(l):                   # ???
    xs = []
    ys = []
    for x, y in l:
        xs.append(x)
        ys.append(y)
    return xs, ys

def bayesdb_guess_stattypes(column_names, rows, null_values=None,
        numcat_count=None, numcat_ratio=None, distinct_ratio=None,
        nullify_ratio=None, overrides=None):
    """Heuristically guess statistical types for the data in `rows`.

    Return a list of (statistical type, reason) corresponding to the columns
    named in the list `column_names`.

    :param set null_values: values to nullify.
    :param int numcat_count: number of distinct values below which
        columns whose values can all be parsed as numbers will be
        considered nominal anyway
    :param real numcat_ratio: ratio of distinct values to total values
        below which columns whose values can all be parsed as numbers
        will be considered nominal anyway
    :param real distinct_ratio: ratio of distinct values to total values
        above which a column will be ignored as a pseudo-key
        (only if count > numcat_count).
    :param real nullify_ratio: ratio of count of the most numerous value to
        total number of values above which the most numerous value should be
        nullified (set to 1 to turn off).
    :param list overrides: list of ``(name, stattype)``, overriding
        any guessed statistical type for columns by those names

    In addition to statistical types, the overrides may specify
    ``key`` or ``ignore``.
    """

    # Fill in default arguments.
    if null_values is None:
        null_values = set(("", "N/A", "none", "None"))
    if numcat_count is None:
        numcat_count = 20
    if numcat_ratio is None:
        numcat_ratio = 0.02
    if distinct_ratio is None:
        distinct_ratio = 0.9
    if nullify_ratio is None:
        nullify_ratio = 0.9
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
        raise ValueError(
            'Duplicate column names: %s'
            % (repr(list(duplicates),)))

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
        raise ValueError(
            'Unknown columns overridden: %s'
            % (repr(list(unknown)),))
    if 0 < len(duplicates):
        raise ValueError(
            'Duplicate columns overridden: %s'
            % (repr(list(duplicates)),))

    # Sanity-check the inputs.
    ncols = len(column_names)
    assert ncols == len(unique(map(casefold, column_names)))
    for ri, row in enumerate(rows):
        if len(row) < ncols:
            raise ValueError(
                'Row %d: Too few columns: %d < %d'
                % (ri, len(row), ncols))
        if len(row) > ncols:
            raise ValueError(
                'Row %d: Too many columns: %d > %d'
                % (ri, len(row), ncols))

    # Find a key first, if it has been specified as an override.
    key = None
    duplicate_keys = set()
    for ci, column_name in enumerate(column_names):
        if casefold(column_name) in override_map:
            if override_map[casefold(column_name)] == 'key':
                if key is not None:
                    duplicate_keys.add(column_name)
                    continue
                column = [row[ci] for row in rows]
                ints = integerify(column)
                if ints:
                   column = ints
                if not keyable_p(column):
                    raise ValueError(
                        'Column non-unique but specified as key: %s'
                        % (repr(column_name),))
                key = column_name
    if 0 < len(duplicate_keys):
        raise ValueError(
            'Multiple columns overridden as keys: %s'
            % (repr(list(duplicate_keys)),))

    # Now go through and guess the other column stattypes or use the override.
    stattypes = []
    for ci, column_name in enumerate(column_names):
        if casefold(column_name) in override_map:
            stattype = override_map[casefold(column_name)]
            reason = 'User override.'
        else:
            column = nullify(null_values, rows, ci)
            [stattype, reason] = guess_column_stattype(
                column,
                distinct_ratio=distinct_ratio,
                nullify_ratio=nullify_ratio,
                numcat_count=numcat_count,
                numcat_ratio=numcat_ratio,
                have_key=(key is not None)
            )
            if stattype == 'key':
                key = column_name
        stattypes.append([stattype, reason])
    return stattypes

def guess_column_stattype(column, reason='', **kwargs):
    counts = count_values(column)
    if None in counts:
        del counts[None]
    if len(counts) < 2:
        return [
            'ignore',
            '%s There is only one unique value.' % (reason,)
        ]
    (most_numerous_key, most_numerous_count) = sorted(
        counts.items(), key=lambda item: item[1], reverse=True)[0]
    if most_numerous_count / float(len(column)) > kwargs['nullify_ratio']:
        column = [None if v == most_numerous_key else v for v in column]
        return guess_column_stattype(
            column,
            '%s More than %d percent of the values are the same, so the '
                'statistical type was guessed based on the remainder of the '
                'values.' % (reason, int(100 * kwargs['nullify_ratio']),),
            **kwargs
        )
    numericable = True
    ints = integerify(column)
    if ints:
        column = ints
    else:
        floats = floatify(column)
        if floats:
            column = floats
        else:
            numericable = False
    if not kwargs['have_key'] and keyable_p(column):
        return [
            'key',
            '%s This was the first column in the table with all distinct '
            'integers or strings.' % (reason,)
        ]
    elif numericable and \
        numerical_p(column, kwargs['numcat_count'], kwargs['numcat_ratio']):
        return [
            'numerical',
            '%s There are at least %d unique numerical values, '
                'and they account for at least %d percent of all '
                'values in the column.'
                % (reason, kwargs['numcat_count'],
                    int(100 * kwargs['numcat_ratio']))
        ]
    elif (len(counts) > kwargs['numcat_count'] and
        len(counts) / float(len(column)) > kwargs['distinct_ratio']):
        return [
            'ignore',
            '%s There are more than %d distinct values and they account '
                'for more than %d percent of the values in the column, so the '
                'column is ignored as a pseudo-key.'
                % (reason, kwargs['numcat_count'],
                    int(100 * kwargs['distinct_ratio']))
        ]
    else:
        if numericable:
            return [
                'nominal',
                '%s There are fewer than %d distinct numerical '
                    'values, or the ratio of distinct values to total values '
                    'is less than %d percent.'
                    % (reason, kwargs['numcat_count'],
                        int(100 * kwargs['numcat_ratio']),)
            ]
        else:
            return [
                'nominal',
                '%s The values are nonnumerical.' % (reason,)
            ]


def nullify(null_values, rows, ci):
    return [row[ci] if row[ci] not in null_values else None for row in rows]

def integerify(column):
    result = []
    if float in [v.__class__ for v in column ]:
        return None
    try:
        result = [int(v) for v in column]
    except (ValueError, TypeError):
        return None
    return result

def floatify(column):
    result = []
    try:
        result = [float(v) if v is not None else float('NaN') for v in column]
    except (ValueError, TypeError):
        return None
    return result

def keyable_p(column):
    # `unique' can't cope with NaNs, so reject them early.
    if any(v is None or (isinstance(v, float) and math.isnan(v))
           for v in column):
        return False
    try:
        column_floats = [float(v) for v in column]
        if not all(v.is_integer() for v in column_floats):
            return False
        return len(column) == len(unique(column))
    except ValueError:
        return len(column) == len(unique(column))

def numerical_p(column, count_cutoff, ratio_cutoff):
    nu = len(unique([v for v in column if not math.isnan(v)]))
    if nu <= count_cutoff:
        return False
    if float(nu) / float(len(column)) <= ratio_cutoff:
        return False
    return True

def count_values(column):
    counts = collections.defaultdict(int)
    for v in column:
        counts[v] += 1
    return counts
