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

import json
import math
import numpy

import bayeslite.core as core
import bayeslite.stats as stats

from bayeslite.exception import BQLError

from bayeslite.sqlite3_util import sqlite3_quote_name

from bayeslite.math_util import ieee_exp
from bayeslite.math_util import logmeanexp
from bayeslite.math_util import logavgexp_weighted
from bayeslite.util import casefold

def bayesdb_install_bql(db, cookie):
    def function(name, nargs, fn):
        db.createscalarfunction(name, (lambda *args: fn(cookie, *args)), nargs)
    function("bql_column_correlation", 5, bql_column_correlation)
    function("bql_column_correlation_pvalue", 5, bql_column_correlation_pvalue)
    function("bql_column_dependence_probability", 5,
        bql_column_dependence_probability)
    function("bql_column_mutual_information", -1, bql_column_mutual_information)
    function("bql_column_value_probability", -1, bql_column_value_probability)
    function("bql_rand", 0, bql_rand)
    function("bql_row_similarity", 6, bql_row_similarity)
    function("bql_row_predictive_relevance", -1, bql_row_predictive_relevance)
    function("bql_row_column_predictive_probability", 6,
        bql_row_column_predictive_probability)
    function("bql_predict", 7, bql_predict)
    function("bql_predict_confidence", 6, bql_predict_confidence)
    function("bql_json_get", 2, bql_json_get)
    function("bql_pdf_joint", -1, bql_pdf_joint)

### BayesDB column functions

def bql_variable_stattypes_and_data(bdb, population_id, colno0, colno1):
    st0 = core.bayesdb_variable_stattype(bdb, population_id, None, colno0)
    st1 = core.bayesdb_variable_stattype(bdb, population_id, None, colno1)
    table_name = core.bayesdb_population_table(bdb, population_id)
    qt = sqlite3_quote_name(table_name)
    varname0 = core.bayesdb_variable_name(bdb, population_id, None, colno0)
    varname1 = core.bayesdb_variable_name(bdb, population_id, None, colno1)
    qvn0 = sqlite3_quote_name(varname0)
    qvn1 = sqlite3_quote_name(varname1)
    data_sql = '''
        SELECT %s, %s FROM %s WHERE %s IS NOT NULL AND %s IS NOT NULL
    ''' % (qvn0, qvn1, qt, qvn0, qvn1)
    data = bdb.sql_execute(data_sql).fetchall()
    data0 = [row[0] for row in data]
    data1 = [row[1] for row in data]
    return (st0, st1, data0, data1)

# Two-column function:  CORRELATION [OF <col0> WITH <col1>]
def bql_column_correlation(bdb, population_id, generator_id, _modelnos,
        colno0, colno1):
    if colno0 < 0:
        varname = core.bayesdb_variable_name(bdb, population_id,
            generator_id, colno0)
        raise BQLError(bdb, 'No correlation for latent variable: %r'
            % (varname,))
    if colno1 < 0:
        varname = core.bayesdb_variable_name(bdb, population_id,
            generator_id, colno1)
        raise BQLError(bdb, 'No correlation for latent variable: %r'
            % (varname,))
    (st0, st1, data0, data1) = bql_variable_stattypes_and_data(bdb,
        population_id, colno0, colno1)
    if (st0, st1) not in correlation_methods:
        raise NotImplementedError('No correlation method for %s/%s.'
            % (st0, st1))
    return correlation_methods[st0, st1](data0, data1)

# Two-column function:  CORRELATION PVALUE [OF <col0> WITH <col1>]
def bql_column_correlation_pvalue(bdb, population_id, generator_id, _modelnos,
        colno0, colno1):
    if colno0 < 0:
        varname = core.bayesdb_variable_name(bdb, population_id,
            generator_id, colno0)
        raise BQLError(bdb, 'No correlation p-value for latent variable: %r'
            % (varname,))
    if colno1 < 0:
        varname = core.bayesdb_variable_name(bdb, population_id,
            generator_id, colno1)
        raise BQLError(bdb, 'No correlation p-value for latent variable: %r'
            % (varname,))
    (st0, st1, data0, data1) = bql_variable_stattypes_and_data(
        bdb, population_id, colno0, colno1)
    if (st0, st1) not in correlation_p_methods:
        raise NotImplementedError(
            'No correlation pvalue method for %s/%s.' % (st0, st1))
    return correlation_p_methods[st0, st1](data0, data1)

def correlation_pearsonr2(data0, data1):
    r = stats.pearsonr(data0, data1)
    return r**2

def correlation_p_pearsonr2(data0, data1):
    n = len(data0)
    assert n == len(data1)
    if n <= 2:
        return float('NaN')
    r = stats.pearsonr(data0, data1)
    if math.isnan(r):
        return float('NaN')
    if r == 1. or r == -1:
        return 0.
    # Compute observed t statistic.
    t = r * math.sqrt((n - 2)/(1 - r**2))
    # Compute p-value for two-sided t-test.
    return 2 * stats.t_cdf(-abs(t), n - 2)

def correlation_cramerphi(data0, data1):
    # Compute observed chi^2 statistic.
    chi2, n0, n1 = cramerphi_chi2(data0, data1)
    if math.isnan(chi2):
        return float('NaN')
    n = len(data0)
    assert n == len(data1)
    # Compute observed correlation.
    return math.sqrt(chi2 / (n * (min(n0, n1) - 1)))

def correlation_p_cramerphi(data0, data1):
    # Compute observed chi^2 statistic.
    chi2, n0, n1 = cramerphi_chi2(data0, data1)
    if math.isnan(chi2):
        return float('NaN')
    # Compute p-value for chi^2 test of independence.
    df = (n0 - 1)*(n1 - 1)
    if df <= 0:
        return float('NaN')
    return stats.chi2_sf(chi2, df)

def cramerphi_chi2(data0, data1):
    n = len(data0)
    assert n == len(data1)
    if n == 0:
        return float('NaN'), 0, 0
    index0 = dict((x, i) for i, x in enumerate(sorted(set(data0))))
    index1 = dict((x, i) for i, x in enumerate(sorted(set(data1))))
    data0 = numpy.array([index0[d] for d in data0])
    data1 = numpy.array([index1[d] for d in data1])
    assert data0.ndim == 1
    assert data1.ndim == 1
    unique0 = numpy.unique(data0)
    unique1 = numpy.unique(data1)
    n0 = len(unique0)
    n1 = len(unique1)
    min_levels = min(n0, n1)
    if min_levels == 1:
        # No variation in at least one column, so no notion of
        # correlation.
        return float('NaN'), n0, n1
    ct = numpy.zeros((n0, n1), dtype=int)
    for i0, x0 in enumerate(unique0):
        for i1, x1 in enumerate(unique1):
            matches0 = numpy.array(data0 == x0, dtype=int)
            matches1 = numpy.array(data1 == x1, dtype=int)
            ct[i0][i1] = numpy.dot(matches0, matches1)
    # Compute observed chi^2 statistic.
    chi2 = stats.chi2_contingency(ct)
    return chi2, n0, n1

def correlation_anovar2(data_group, data_y):
    # Compute observed F-test statistic.
    F, n_groups = anovar2(data_group, data_y)
    if math.isnan(F):
        return float('NaN')
    n = len(data_group)
    assert n == len(data_y)
    # Compute observed correlation.
    return 1 - 1/(1 + F*(float(n_groups - 1) / float(n - n_groups)))

def correlation_p_anovar2(data_group, data_y):
    # Compute observed F-test statistic.
    F, n_groups = anovar2(data_group, data_y)
    if math.isnan(F):
        return float('NaN')
    n = len(data_group)
    assert n == len(data_y)
    # Compute p-value for F-test.
    return stats.f_sf(F, n_groups - 1, n - n_groups)

def anovar2(data_group, data_y):
    n = len(data_group)
    assert n == len(data_y)
    group_index = {}
    for x in data_group:
        if x not in group_index:
            group_index[x] = len(group_index)
    n_groups = len(group_index)
    if n_groups == 0:
        # No data, so no notion of correlation.
        return float('NaN'), n_groups
    if n_groups == n:
        # No variation in any group, so no notion of correlation.
        return float('NaN'), n_groups
    if n_groups == 1:
        # Only one group means we can draw no information from the
        # choice of group, so no notion of correlation.
        return float('NaN'), n_groups
    groups = [None] * n_groups
    for i in xrange(n_groups):
        groups[i] = []
    for x, y in zip(data_group, data_y):
        groups[group_index[x]].append(y)
    # Compute observed F-test statistic.
    F = stats.f_oneway(groups)
    return F, n_groups

def correlation_anovar2_dc(discrete_data, continuous_data):
    return correlation_anovar2(discrete_data, continuous_data)

def correlation_anovar2_cd(continuous_data, discrete_data):
    return correlation_anovar2(discrete_data, continuous_data)

def correlation_p_anovar2_dc(discrete_data, continuous_data):
    return correlation_p_anovar2(discrete_data, continuous_data)

def correlation_p_anovar2_cd(continuous_data, discrete_data):
    return correlation_p_anovar2(discrete_data, continuous_data)

correlation_methods = {}
correlation_p_methods = {}

def define_correlation(stattype0, stattype1, method):
    assert casefold(stattype0) == stattype0
    assert casefold(stattype1) == stattype1
    assert (stattype0, stattype1) not in correlation_methods
    correlation_methods[stattype0, stattype1] = method

def define_correlation_p(stattype0, stattype1, method):
    assert casefold(stattype0) == stattype0
    assert casefold(stattype1) == stattype1
    assert (stattype0, stattype1) not in correlation_p_methods
    correlation_p_methods[stattype0, stattype1] = method

define_correlation('nominal', 'cyclic', correlation_anovar2_dc)
define_correlation('nominal', 'nominal', correlation_cramerphi)
define_correlation('nominal', 'numerical', correlation_anovar2_dc)

define_correlation_p('nominal', 'cyclic', correlation_p_anovar2_dc)
define_correlation_p('nominal', 'nominal', correlation_p_cramerphi)
define_correlation_p('nominal', 'numerical', correlation_p_anovar2_dc)

define_correlation('numerical', 'cyclic', correlation_pearsonr2)
define_correlation('numerical', 'nominal', correlation_anovar2_cd)
define_correlation('numerical', 'numerical', correlation_pearsonr2)

define_correlation_p('numerical', 'cyclic', correlation_p_pearsonr2)
define_correlation_p('numerical', 'nominal', correlation_p_anovar2_cd)
define_correlation_p('numerical', 'numerical', correlation_p_pearsonr2)

# XXX Pretend CYCLIC is NUMERICAL for the purposes of correlation.  To
# do this properly we ought to implement a standard statistical notion
# of circular/linear correlation, as noted in Github issue #146
# <https://github.com/probcomp/bayeslite/issues/146>.
define_correlation('cyclic', 'cyclic', correlation_pearsonr2)
define_correlation('cyclic', 'nominal', correlation_anovar2_cd)
define_correlation('cyclic', 'numerical', correlation_pearsonr2)

define_correlation_p('cyclic', 'cyclic', correlation_p_pearsonr2)
define_correlation_p('cyclic', 'nominal', correlation_p_anovar2_cd)
define_correlation_p('cyclic', 'numerical', correlation_p_pearsonr2)

# Two-column function:  DEPENDENCE PROBABILITY [OF <col0> WITH <col1>]
def bql_column_dependence_probability(
        bdb, population_id, generator_id, modelnos, colno0, colno1):
    modelnos = _retrieve_modelnos(modelnos)
    def generator_depprob(generator_id):
        backend = core.bayesdb_generator_backend(bdb, generator_id)
        depprob_list = backend.column_dependence_probability(
            bdb, generator_id, modelnos, colno0, colno1)
        return stats.arithmetic_mean(depprob_list)
    generator_ids = _retrieve_generator_ids(bdb, population_id, generator_id)
    depprobs = map(generator_depprob, generator_ids)
    return stats.arithmetic_mean(depprobs)

# Two-column function:  MUTUAL INFORMATION [OF <col0> WITH <col1>]
def bql_column_mutual_information(
        bdb, population_id, generator_id, modelnos, colnos0, colnos1,
        numsamples, *constraint_args):
    colnos0 = json.loads(colnos0)
    colnos1 = json.loads(colnos1)
    modelnos = _retrieve_modelnos(modelnos)
    mutinfs = _bql_column_mutual_information(
        bdb, population_id, generator_id, modelnos, colnos0, colnos1,
        numsamples, *constraint_args)
    # XXX This integral of the CMI returned by each model of all generators in
    # in the population is wrong! At least, it does not directly correspond to
    # any meaningful probabilistic quantity, other than literally the mean CMI
    # averaged over all population models.
    return stats.arithmetic_mean([stats.arithmetic_mean(m) for m in mutinfs])

def _bql_column_mutual_information(
        bdb, population_id, generator_id, modelnos, colnos0, colnos1,
        numsamples, *constraint_args):
    if len(constraint_args) % 2 == 1:
        raise ValueError('Odd constraint arguments: %s.' % (constraint_args))
    constraints = zip(constraint_args[::2], constraint_args[1::2]) \
        if constraint_args else None
    def generator_mutinf(generator_id):
        backend = core.bayesdb_generator_backend(bdb, generator_id)
        return backend.column_mutual_information(
            bdb, generator_id, modelnos, colnos0, colnos1,
            constraints=constraints, numsamples=numsamples)
    generator_ids = _retrieve_generator_ids(bdb, population_id, generator_id)
    mutinfs = map(generator_mutinf, generator_ids)
    return mutinfs

# One-column function: PROBABILITY DENSITY OF <col>=<value> GIVEN <constraints>
def bql_column_value_probability(
        bdb, population_id, generator_id, modelnos, colno, value,
        *constraint_args):
    modelnos = _retrieve_modelnos(modelnos)
    constraints = []
    i = 0
    while i < len(constraint_args):
        if i + 1 == len(constraint_args):
            raise ValueError(
                'Odd constraint arguments: %s' % (constraint_args,))
        constraint_colno = constraint_args[i]
        constraint_value = constraint_args[i + 1]
        constraints.append((constraint_colno, constraint_value))
        i += 2
    targets = [(colno, value)]
    logp = _bql_logpdf(bdb, population_id, generator_id, modelnos, targets,
        constraints)
    return ieee_exp(logp)

# XXX This is silly.  We should return log densities, not densities.
# This is Github issue #360:
# https://github.com/probcomp/bayeslite/issues/360
def bql_pdf_joint(bdb, population_id, generator_id, modelnos, *args):
    modelnos = _retrieve_modelnos(modelnos)
    i = 0
    targets = []
    while i < len(args):
        if args[i] is None:
            i += 1
            break
        if i + 1 == len(args):
            raise ValueError('Missing logpdf target value: %r' % (args[i],))
        t_colno = args[i]
        t_value = args[i + 1]
        targets.append((t_colno, t_value))
        i += 2
    constraints = []
    while i < len(args):
        if i + 1 == len(args):
            raise ValueError('Missing logpdf constraint value: %r' %
                (args[i],))
        c_colno = args[i]
        c_value = args[i + 1]
        constraints.append((c_colno, c_value))
        i += 2
    logp = _bql_logpdf(bdb, population_id, generator_id, modelnos, targets,
        constraints)
    return ieee_exp(logp)

def _bql_logpdf(bdb, population_id, generator_id, modelnos, targets,
        constraints):
    # P(T | C) = \sum_M P(T, M | C)
    # = \sum_M P(T | C, M) P(M | C)
    # = \sum_M P(T | C, M) P(M) P(C | M) / P(C)
    # = \sum_M P(T | C, M) P(M) P(C | M) / \sum_M' P(C, M')
    # = \sum_M P(T | C, M) P(M) P(C | M) / \sum_M' P(C | M') P(M')
    #
    # For a generator M, logpdf(M) computes P(T | C, M), and
    # loglikelihood(M) computes P(C | M).  For now, we weigh each
    # generator uniformly; eventually, we ought to allow the user to
    # specify a prior weight (XXX and update some kind of posterior
    # weight?).
    rowid, constraints = _retrieve_rowid_constraints(
        bdb, population_id, constraints)
    def logpdf(generator_id, backend):
        return backend.logpdf_joint(
            bdb, generator_id, modelnos, rowid, targets, constraints)
    def loglikelihood(generator_id, backend):
        if not constraints:
            return 0
        return backend.logpdf_joint(
            bdb, generator_id, modelnos, rowid, constraints, [])
    generator_ids = _retrieve_generator_ids(bdb, population_id, generator_id)
    backends = [
        core.bayesdb_generator_backend(bdb, g)
        for g in generator_ids
    ]
    loglikelihoods = map(loglikelihood, generator_ids, backends)
    logpdfs = map(logpdf, generator_ids, backends)
    return logavgexp_weighted(loglikelihoods, logpdfs)

### BayesDB row functions

# Row function:  SIMILARITY TO <target_row> IN THE CONTEXT OF <column>
def bql_row_similarity(
        bdb, population_id, generator_id, modelnos, rowid, target_rowid, colno):
    if target_rowid is None:
        raise BQLError(bdb, 'No such target row for SIMILARITY')
    modelnos = _retrieve_modelnos(modelnos)
    def generator_similarity(generator_id):
        backend = core.bayesdb_generator_backend(bdb, generator_id)
        # XXX Change [colno] to colno by updating BayesDB_Backend.
        similarity_list = backend.row_similarity(
            bdb, generator_id, modelnos, rowid, target_rowid, [colno])
        return stats.arithmetic_mean(similarity_list)
    generator_ids = _retrieve_generator_ids(bdb, population_id, generator_id)
    similarities = map(generator_similarity, generator_ids)
    return stats.arithmetic_mean(similarities)

# Row function:  PREDICTIVE RELEVANCE TO (<target_row>)
#  [<AND HYPOTHETICAL ROWS WITH VALUES ((...))] IN THE CONTEXT OF <column>
def bql_row_predictive_relevance(
        bdb, population_id, generator_id, modelnos, rowid_target, rowid_query,
        colno, *constraint_args):
    if rowid_target is None:
        raise BQLError(bdb, 'No such target row for SIMILARITY')
    rowid_query = json.loads(rowid_query)
    modelnos = _retrieve_modelnos(modelnos)
    # Build the list of hypothetical values.
    # Each sequence of values is separated by None to demarcate between rows.
    splits = [-1] + [i for i, x in enumerate(constraint_args) if x is None]
    assert splits[-1] == len(constraint_args) - 1
    rows_list = [
        constraint_args[splits[i]+1:splits[i+1]]
        for i in range(len(splits)-1)
    ]
    assert all(len(row)%2 == 0 for row in rows_list)
    hypotheticals = [zip(row[::2], row[1::2]) for row in rows_list]
    if len(rowid_query) == 0 and len(hypotheticals) == 0:
        raise BQLError(bdb, 'No matching rows for PREDICTIVE RELEVANCE.')
    def generator_similarity(generator_id):
        backend = core.bayesdb_generator_backend(bdb, generator_id)
        return backend.predictive_relevance(
            bdb, generator_id, modelnos, rowid_target, rowid_query,
            hypotheticals, colno)
    generator_ids = _retrieve_generator_ids(bdb, population_id, generator_id)
    sims = map(generator_similarity, generator_ids)
    return stats.arithmetic_mean([stats.arithmetic_mean(s) for s in sims])

# Row function:  PREDICTIVE PROBABILITY OF <targets> [GIVEN <constraints>]
def bql_row_column_predictive_probability(
        bdb, population_id, generator_id, modelnos, rowid, targets,
        constraints):
    targets = json.loads(targets)
    constraints = json.loads(constraints)
    modelnos = _retrieve_modelnos(modelnos)
    # Build the constraints and query from rowid, using a fresh rowid.
    fresh_rowid = core.bayesdb_population_fresh_row_id(bdb, population_id)
    def retrieve_values(colnos):
        values = [
            core.bayesdb_population_cell_value(bdb, population_id, rowid, colno)
            for colno in colnos
        ]
        return [(c,v) for (c,v) in zip (colnos, values) if v is not None]
    cgpm_targets = retrieve_values(targets)
    # If all targets have NULL values, return None.
    if len(cgpm_targets) == 0:
        return None
    cgpm_constraints = retrieve_values(constraints)
    def generator_predprob(generator_id):
        backend = core.bayesdb_generator_backend(bdb, generator_id)
        return backend.logpdf_joint(
            bdb, generator_id, modelnos, fresh_rowid, cgpm_targets,
            cgpm_constraints)
    generator_ids = _retrieve_generator_ids(bdb, population_id, generator_id)
    predprobs = map(generator_predprob, generator_ids)
    r = logmeanexp(predprobs)
    return ieee_exp(r)

### Predict and simulate

def bql_predict(
        bdb, population_id, generator_id, modelnos, rowid, colno, threshold,
        numsamples):
    # XXX Randomly sample 1 generator from the population, until we figure out
    # how to aggregate imputations across different hypotheses.
    modelnos = _retrieve_modelnos(modelnos)
    if generator_id is None:
        generator_ids = core.bayesdb_population_generators(bdb, population_id)
        index = bdb.np_prng.randint(0, high=len(generator_ids))
        generator_id = generator_ids[index]
    backend = core.bayesdb_generator_backend(bdb, generator_id)
    return backend.predict(
        bdb, generator_id, modelnos, rowid, colno, threshold,
        numsamples=numsamples)

def bql_predict_confidence(
        bdb, population_id, generator_id, modelnos, rowid, colno, numsamples):
    # XXX Do real imputation here!
    # XXX Randomly sample 1 generator from the population, until we figure out
    # how to aggregate imputations across different hypotheses.
    if generator_id is None:
        generator_ids = core.bayesdb_population_generators(bdb, population_id)
        index = bdb.np_prng.randint(0, high=len(generator_ids))
        generator_id = generator_ids[index]
    modelnos = _retrieve_modelnos(modelnos)
    backend = core.bayesdb_generator_backend(bdb, generator_id)
    value, confidence = backend.predict_confidence(
        bdb, generator_id, modelnos, rowid, colno, numsamples=numsamples)
    # XXX Whattakludge!
    return json.dumps({'value': value, 'confidence': confidence})

# XXX Whattakludge!
def bql_json_get(bdb, blob, key):
    return json.loads(blob)[key]

def bayesdb_simulate(
        bdb, population_id, generator_id, modelnos, constraints, colnos,
        numpredictions=1, accuracy=None):
    """Simulate rows from a generative model, subject to constraints.

    Returns a list of `numpredictions` tuples, with a value for each
    column specified in the list `colnos`, conditioned on the
    constraints in the list `constraints` of tuples ``(colno,
    value)``.

    The results are simulated from the predictive distribution on
    fresh rows.
    """
    modelnos = _retrieve_modelnos(modelnos)
    rowid, constraints = _retrieve_rowid_constraints(
        bdb, population_id, constraints)
    def loglikelihood(generator_id, backend):
        if not constraints:
            return 0
        return backend.logpdf_joint(
            bdb, generator_id, modelnos, rowid, constraints, [])
    def simulate(generator_id, backend, n):
        return backend.simulate_joint(
            bdb, generator_id, modelnos, rowid, colnos, constraints,
            num_samples=n, accuracy=accuracy)
    generator_ids = _retrieve_generator_ids(bdb, population_id, generator_id)
    backends = [
        core.bayesdb_generator_backend(bdb, generator_id)
        for generator_id in generator_ids
    ]
    if len(generator_ids) > 1:
        loglikelihoods = map(loglikelihood, generator_ids, backends)
        likelihoods = map(math.exp, loglikelihoods)
        total_likelihood = sum(likelihoods)
        if total_likelihood == 0:
            # XXX Show the constraints with symbolic names.
            raise BQLError(bdb, 'Impossible constraints: %r' % (constraints,))
        probabilities = [
            likelihood / total_likelihood
            for likelihood in likelihoods
        ]
        countses = bdb.np_prng.multinomial(
            numpredictions, probabilities, size=1)
        counts = countses[0]
    elif len(generator_ids) == 1:
        counts = [numpredictions]
    else:
        counts = []
    rowses = map(simulate, generator_ids, backends, counts)
    all_rows = [row for rows in rowses for row in rows]
    assert all(isinstance(row, (tuple, list)) for row in all_rows)
    return all_rows

### Seeded random number generation

def bql_rand(bdb):
    return bdb.np_prng.uniform()

### Helper functions functions

def _retrieve_rowid_constraints(bdb, population_id, constraints):
    rowid = core.bayesdb_population_fresh_row_id(bdb, population_id)
    if constraints:
        user_rowid = [
            v for c, v in constraints
            if c in core.bayesdb_rowid_tokens(bdb)
        ]
        if len(user_rowid) == 1:
            rowid = user_rowid[0]
        elif len(user_rowid) > 1:
            raise BQLError(bdb, 'Multiple rowids given: %s.' % (constraints,))
        constraints = [
            (c, v) for c, v in constraints
            if c not in core.bayesdb_rowid_tokens(bdb)
        ]
    return rowid, constraints

def _retrieve_generator_ids(bdb, population_id, generator_id):
    if generator_id is None:
        return core.bayesdb_population_generators(bdb, population_id)
    return [generator_id]

def _retrieve_modelnos(modelnos):
    return None if modelnos is None else json.loads(modelnos)
