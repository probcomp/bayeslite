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

import json
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
    function("bql_column_dependence_probability", 4,
        bql_column_dependence_probability)
    function("bql_column_mutual_information", 5, bql_column_mutual_information)
    function("bql_column_typicality", 3, bql_column_typicality)
    function("bql_column_value_probability", 4, bql_column_value_probability)
    function("bql_row_similarity", -1, bql_row_similarity)
    function("bql_row_typicality", 3, bql_row_typicality)
    function("bql_row_column_predictive_probability", 4,
        bql_row_column_predictive_probability)
    function("bql_predict", 5, bql_predict)
    function("bql_predict_confidence", 4, bql_predict_confidence)
    function("bql_json_get", 2, bql_json_get)

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

def bql_column_stattypes_and_data(bdb, generator_id, colno0, colno1):
    st0 = core.bayesdb_generator_column_stattype(bdb, generator_id, colno0)
    st1 = core.bayesdb_generator_column_stattype(bdb, generator_id, colno1)
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
    return (st0, st1, data0, data1)

# Two-column function:  CORRELATION [OF <col0> WITH <col1>]
def bql_column_correlation(bdb, generator_id, colno0, colno1):
    (st0, st1, data0, data1) = bql_column_stattypes_and_data(bdb, generator_id,
        colno0, colno1)
    if (st0, st1) not in correlation_methods:
        raise NotImplementedError('No correlation method for %s/%s.' %
            (st0, st1))
    return correlation_methods[st0, st1](data0, data1)[0]

# Two-column function:  CORRELATION PVALUE [OF <col0> WITH <col1>]
def bql_column_correlation_pvalue(bdb, generator_id, colno0, colno1):
    (st0, st1, data0, data1) = bql_column_stattypes_and_data(bdb, generator_id,
        colno0, colno1)
    if (st0, st1) not in correlation_pvalue_methods:
        raise NotImplementedError('No correlation pvalue method for %s/%s.' %
            (st0, st1))
    return correlation_methods[st0, st1](data0, data1)[1]

def correlation_pearsonr2(data0, data1):
    # Compute the observed correlation.
    corr = stats.pearsonr(data0, data1)**2
    if math.isnan(corr):
        return (corr, float('NaN'))
    if corr == 1.:
        return (corr, 0.)
    # Computeip observed t-stat.
    N = len(data0)
    t = corr * math.sqrt((N-2) / (1 - corr**2))
    # Compute p-value for two sided t-test.
    t = t if t <= 0 else -t
    pvalue = 2 * stats.t_cdf(t, N-2)
    return (corr, pvalue)

def correlation_cramerphi(data0, data1):
    n = len(data0)
    assert n == len(data1)
    if n == 0:
        return (float('NaN'), float('NaN'))
    unique0 = unique_indices(data0)
    unique1 = unique_indices(data1)
    n0 = len(unique0)
    n1 = len(unique1)
    if n0 == 1 and n1 == 1:
        return (1., 0.)
    min_levels = min(n0, n1)
    if min_levels == 1:
        return (0., 1.)
    ct = [0] * n0
    for i0, j0 in enumerate(unique0):
        ct[i0] = [0] * n1
        for i1, j1 in enumerate(unique1):
            c = 0
            for i in range(n):
                if data0[i] == data0[j0] and data1[i] == data1[j1]:
                    c += 1
            ct[i0][i1] = c
    # Compute observed Chi2 stat.
    chisq = stats.chi2_contingency(ct, correction=False)
    # Compute the observed correlation
    corr = math.sqrt(chisq / (n * (min_levels - 1)))
    # Compute p-value for Chi2 test of independence.
    pvalue = stats.chi2_sf(chisq, (n0-1)*(n1-1))
    return (corr, pvalue)

def correlation_anovar2(data_group, data_y):
    n = len(data_group)
    assert n == len(data_y)
    group_index = {}
    for x in data_group:
        if x not in group_index:
            group_index[x] = len(group_index)
    n_groups = len(group_index)
    if n_groups == n or n_groups == 0:
        return (float('NaN'), float('NaN'))
    if n_groups == 1:
        if all(data_y[i] == data_y[0] for i in xrange(1, len(data_y))):
            return (1., 0.)
        else:
            return (0., 1)
    groups = [None] * n_groups
    for i in xrange(n_groups):
        groups[i] = []
    for x, y in zip(data_group, data_y):
        groups[group_index[x]].append(y)
    
    # Compute observed F stat.
    F = stats.f_oneway(groups)
    # Compute observed correlation.
    corr = 1 - 1/(1 + F*(float(n_groups - 1) / float(n - n_groups)))
    # Compute p-value for F-test.
    pvalue = stats.f_sf(F, n_groups-1, n-n_groups)
    return (corr, pvalue)

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
def bql_column_dependence_probability(bdb, generator_id, modelno, colno0,
        colno1):
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    return metamodel.column_dependence_probability(bdb, generator_id, modelno,
        colno0, colno1)

# Two-column function:  MUTUAL INFORMATION [OF <col0> WITH <col1>]
def bql_column_mutual_information(bdb, generator_id, modelno, colno0, colno1,
        numsamples=None):
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    return metamodel.column_mutual_information(bdb, generator_id, modelno,
        colno0, colno1, numsamples=numsamples)

# One-column function:  TYPICALITY OF <col>
def bql_column_typicality(bdb, generator_id, modelno, colno):
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    return metamodel.column_typicality(bdb, generator_id, modelno, colno)

# One-column function:  PROBABILITY OF <col>=<value>
def bql_column_value_probability(bdb, generator_id, modelno, colno, value):
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    return metamodel.column_value_probability(bdb, generator_id, modelno,
        colno, value)

### BayesDB row functions

# Row function:  SIMILARITY TO <target_row> [WITH RESPECT TO <columns>]
def bql_row_similarity(bdb, generator_id, modelno, rowid, target_rowid,
        *colnos):
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    if len(colnos) == 0:
        colnos = core.bayesdb_generator_column_numbers(bdb, generator_id)
    return metamodel.row_similarity(bdb, generator_id, modelno, rowid,
        target_rowid, colnos)

# Row function:  TYPICALITY
def bql_row_typicality(bdb, generator_id, modelno, rowid):
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    return metamodel.row_typicality(bdb, generator_id, modelno, rowid)

# Row function:  PREDICTIVE PROBABILITY OF <column>
def bql_row_column_predictive_probability(bdb, generator_id, modelno, rowid,
        colno):
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    return metamodel.row_column_predictive_probability(bdb, generator_id,
        modelno, rowid, colno)

### Predict and simulate

def bql_predict(bdb, generator_id, modelno, colno, rowid, threshold,
        numsamples=None):
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    return metamodel.predict(bdb, generator_id, modelno, colno, rowid,
        threshold, numsamples=numsamples)

def bql_predict_confidence(bdb, generator_id, modelno, colno, rowid,
        numsamples=None):
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    value, confidence = metamodel.predict_confidence(bdb, generator_id,
        modelno, colno, rowid, numsamples=numsamples)
    # XXX Whattakludge!
    return json.dumps({'value': value, 'confidence': confidence})

# XXX Whattakludge!
def bql_json_get(bdb, blob, key):
    return json.loads(blob)[key]

def bayesdb_simulate(bdb, generator_id, constraints, colnos,
        modelno=None, numpredictions=1):
    """Simulate rows from a generative model, subject to constraints.

    Returns a list of `numpredictions` tuples, with a value for each
    column specified in the list `colnos`, conditioned on the
    constraints in the list `constraints` of tuples ``(colno,
    value)``.
    """
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    return metamodel.simulate(bdb, generator_id, modelno, constraints, colnos,
        numpredictions=numpredictions)

def bayesdb_insert(bdb, generator_id, row):
    """Notify a generator that a row has been inserted into its table."""
    bayesdb_insertmany(bdb, generator_id, [row])

def bayesdb_insertmany(bdb, generator_id, rows):
    """Notify a generator that rows have been inserted into its table."""
    metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
    metamodel.insertmany(bdb, generator_id, rows)
