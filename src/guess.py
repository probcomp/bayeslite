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

import math

import bayeslite.core as core

from bayeslite.sqlite3_util import sqlite3_quote_name
from bayeslite.util import casefold
from bayeslite.util import unique

def bayesdb_guess_generator(bdb, generator, table, metamodel,
        ifnotexists=False, count_cutoff=None, ratio_cutoff=None,
        default=None):
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
        rows = list(cursor)
        stattypes = bayesdb_guess_stattypes(column_names, rows)
        # Skip the key column.
        column_names, stattypes = \
            unzip([(cn, st) for cn, st in zip(column_names, stattypes)
                if st != 'key'])
        if len(column_names) == 0:
            raise ValueError('Table has only key column: %s' % (repr(table),))
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

def bayesdb_guess_stattypes(column_names, rows, count_cutoff=None,
        ratio_cutoff=None):
    if count_cutoff is None:
        count_cutoff = 20
    if ratio_cutoff is None:
        ratio_cutoff = 0.02
    ncols = len(column_names)
    assert ncols == len(unique(map(casefold, column_names)))
    for ri, row in enumerate(rows):
        if len(row) < ncols:
            raise ValueError('Row %d: Too few columns: %d < %d' %
                (ri, len(row), ncols))
        if len(row) > ncols:
            raise ValueError('Row %d: Too many columns: %d > %d' %
                (ri, len(row), ncols))
    key = None
    stattypes = []
    for ci, column_name in enumerate(column_names):
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
        elif numericable and numerical_p(column, count_cutoff, ratio_cutoff):
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
