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
import time

import bayeslite.core as core
import bayeslite.stats as stats

from bayeslite.sqlite3_util import sqlite3_quote_name

from bayeslite.util import casefold
from bayeslite.util import unique
from bayeslite.util import unique_indices

def bayesdb_install_bql(db, cookie):
    def function(name, nargs, fn):
        db.create_function(name, nargs,
            lambda *args: bayesdb_bql(fn, cookie, *args))
    function("bql_column_correlation", 3, bql_column_correlation)
    function("bql_column_dependence_probability", 3,
        bql_column_dependence_probability)
    function("bql_column_mutual_information", 4, bql_column_mutual_information)
    function("bql_column_typicality", 2, bql_column_typicality)
    function("bql_column_value_probability", 3, bql_column_value_probability)
    function("bql_row_similarity", -1, bql_row_similarity)
    function("bql_row_typicality", 2, bql_row_typicality)
    function("bql_row_column_predictive_probability", 3,
        bql_row_column_predictive_probability)
    function("bql_infer", 4, bql_infer)
    function("bql_infer_confidence", 3, bql_infer_confidence)
    function("bql_json_get", 3, bql_json_get)

# XXX XXX XXX Temporary debugging kludge!
import sys
import traceback

def bayesdb_bql(fn, cookie, *args):
    try:
        return fn(cookie, *args)
    except Exception as e:
        print >>sys.stderr, traceback.format_exc()
        raise e

### BayesDB column functions

# Two-column function:  CORRELATION [OF <col0> WITH <col1>]
def bql_column_correlation(bdb, generator_id, colno0, colno1):
    st0 = core.bayesdb_generator_column_stattype(bdb, generator_id, colno0)
    st1 = core.bayesdb_generator_column_stattype(bdb, generator_id, colno1)
    if (st0, st1) not in correlation_methods:
        raise NotImplementedError('No correlation method for %s/%s.' %
            (st0, st1))
    table_name = core.bayesdb_generator_table(bdb, generator_id)
    qt = sqlite3_quote_name(table_name)
    colname0 = core.bayesdb_generator_column_name(bdb, generator_id, colno0)
    colname1 = core.bayesdb_generator_column_name(bdb, generator_id, colno1)
    qcn0 = sqlite3_quote_name(colname0)
    qcn1 = sqlite3_quote_name(colname1)
    data_sql = '''
        SELECT %s, %s FROM %s WHERE %s IS NOT NULL AND %s IS NOT NULL
    ''' % (qcn0, qcn1, qt, qcn0, qcn1)
    data = list(bdb.sql_execute(data_sql))
    data0 = [row[0] for row in data]
    data1 = [row[1] for row in data]
    return correlation_methods[st0, st1](data0, data1)

def correlation_pearsonr2(data0, data1):
    return stats.pearsonr(data0, data1)**2

def correlation_cramerphi(data0, data1):
    n = len(data0)
    assert n == len(data1)
    unique0 = unique_indices(data0)
    unique1 = unique_indices(data1)
    min_levels = min(len(unique0), len(unique1))
    if min_levels <= 1:
        return float('NaN')
    ct = [0] * len(unique0)
    for i0, j0 in enumerate(unique0):
        ct[i0] = [0] * len(unique1)
        for i1, j1 in enumerate(unique1):
            c = 0
            for i in range(n):
                if data0[i] == data0[j0] and data1[i] == data1[j1]:
                    c += 1
            ct[i0][i1] = c
    chisq = stats.chi2_contingency(ct, correction=False)
    return math.sqrt(chisq / (n * (min_levels - 1)))

def correlation_anovar2(data_group, data_y):
    n = len(data_group)
    assert n == len(data_y)
    group_values = unique(data_group)
    n_groups = len(group_values)
    if n_groups == len(data_group):
        return float('NaN')
    samples = []
    for v in group_values:
        sample = []
        for i in range(n):
            if data_group[i] == v:
                sample.append(data_y[i])
        samples.append(sample)
    F = stats.f_oneway(samples)
    return 1 - 1/(1 + F*((n_groups - 1) / (n - n_groups)))

def correlation_anovar2_dc(discrete_data, continuous_data):
    return correlation_anovar2(discrete_data, continuous_data)
def correlation_anovar2_cd(continuous_data, discrete_data):
    return correlation_anovar2(discrete_data, continuous_data)

correlation_methods = {}

def define_correlation(stattype0, stattype1, method):
    assert casefold(stattype0) == stattype0
    assert casefold(stattype1) == stattype1
    assert (stattype0, stattype1) not in correlation_methods
    correlation_methods[stattype0, stattype1] = method

define_correlation('categorical', 'categorical', correlation_cramerphi)
define_correlation('categorical', 'numerical', correlation_anovar2_dc)
define_correlation('numerical', 'categorical', correlation_anovar2_cd)
define_correlation('numerical', 'numerical', correlation_pearsonr2)

# Two-column function:  DEPENDENCE PROBABILITY [OF <col0> WITH <col1>]
def bql_column_dependence_probability(bdb, generator_id, colno0, colno1):
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    return metamodel.column_dependence_probability(bdb, generator_id, colno0,
        colno1)

# Two-column function:  MUTUAL INFORMATION [OF <col0> WITH <col1>]
def bql_column_mutual_information(bdb, generator_id, colno0, colno1,
        numsamples=None):
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    return metamodel.column_mutual_information(bdb, generator_id, colno0,
        colno1, numsamples=numsamples)

# One-column function:  TYPICALITY OF <col>
def bql_column_typicality(bdb, generator_id, colno):
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    return metamodel.column_typicality(bdb, generator_id, colno)

# One-column function:  PROBABILITY OF <col>=<value>
def bql_column_value_probability(bdb, generator_id, colno, value):
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    return metamodel.column_value_probability(bdb, generator_id, colno, value)

### BayesDB row functions

# Row function:  SIMILARITY TO <target_row> [WITH RESPECT TO <columns>]
def bql_row_similarity(bdb, generator_id, rowid, target_rowid, *colnos):
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    if len(colnos) == 0:
        colnos = core.bayesdb_generator_column_numbers(bdb, generator_id)
    return metamodel.row_similarity(bdb, generator_id, rowid, target_rowid,
        colnos)

# Row function:  TYPICALITY
def bql_row_typicality(bdb, generator_id, rowid):
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    return metamodel.row_typicality(bdb, generator_id, rowid)

# Row function:  PREDICTIVE PROBABILITY OF <column>
def bql_row_column_predictive_probability(bdb, generator_id, rowid, colno):
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    return metamodel.row_column_predictive_probability(bdb, generator_id,
        rowid, colno)

### Infer and simulate

def bql_infer(bdb, generator_id, colno, rowid, threshold, numsamples=None):
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    return metamodel.infer(bdb, generator_id, colno, rowid, threshold,
        numsamples=numsamples)

def bql_infer_confidence(bdb, generator_id, colno, rowid, numsamples=None):
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    value, confidence = metamodel.infer_confidence(bdb, generator_id, colno,
        rowid, numsamples=numsamples)
    # XXX Whattakludge!
    return json.dumps({'value': value, 'confidence': confidence})

# XXX Whattakludge!
def bql_json_get(bdb, json, key):
    return json.loads(json)[key]

def bayesdb_simulate(bdb, generator_id, constraints, colnos, numpredictions=1):
    """Simulate rows from a generative model, subject to constraints.

    Returns a list of `numpredictions` tuples, with a value for each
    column specified in the list `colnos`, conditioned on the
    constraints in the list `constraints` of tuples ``(colno,
    value)``.
    """
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    return metamodel.simulate(bdb, generator_id, constraints, colnos,
        numpredictions=numpredictions)

def bayesdb_insert(bdb, generator_id, row):
    bayesdb_insertmany(bdb, generator_id, [row])

def bayesdb_insertmany(bdb, generator_id, rows):
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    metamodel.insertmany(bdb, generator_id, rows)
