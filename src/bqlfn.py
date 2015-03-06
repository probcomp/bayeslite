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

from bayeslite.sqlite3_util import sqlite3_quote_name

from bayeslite.util import arithmetic_mean
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
    function("bql_infer", 5, bql_infer)

# XXX XXX XXX Temporary debugging kludge!
import traceback

def bayesdb_bql(fn, cookie, *args):
    try:
        return fn(cookie, *args)
    except Exception as e:
        print traceback.format_exc()
        raise e

### BayesDB model initialization and analysis

def bayesdb_models_initialize(bdb, table_id, modelnos, model_config=None,
        ifnotexists=False):
    if ifnotexists:
        # Find whether all model numbers are filled.  If so, don't
        # bother with initialization.
        #
        # XXX Are the models dependent on one another, or can we just
        # ask the engine to initialize as many models as we don't have
        # and fill in the gaps?
        done = True
        for modelno in modelnos:
            if not core.bayesdb_has_model(bdb, table_id, modelno):
                done = False
                break
        if done:
            return
    assert model_config is None         # XXX For now.
    assert 0 < len(modelnos)
    engine = core.bayesdb_table_engine(bdb, table_id)
    model_config = {
        "kernel_list": (),
        "initialization": "from_the_prior",
        "row_initialization": "from_the_prior",
    }
    X_L_list, X_D_list = engine.initialize(
        M_c=core.bayesdb_metadata(bdb, table_id),
        M_r=None,            # XXX
        T=list(core.bayesdb_data(bdb, table_id)),
        n_chains=len(modelnos),
        initialization=model_config["initialization"],
        row_initialization=model_config["row_initialization"]
    )
    if len(modelnos) == 1:      # XXX Ugh.  Fix crosscat so it doesn't do this.
        X_L_list = [X_L_list]
        X_D_list = [X_D_list]
    with bdb.savepoint():
        for modelno, (X_L, X_D) in zip(modelnos, zip(X_L_list, X_D_list)):
            theta = {
                "X_L": X_L,
                "X_D": X_D,
                "iterations": 0,
                "column_crp_alpha": [],
                "logscore": [],
                "num_views": [],
                "model_config": model_config,
            }
            core.bayesdb_init_model(bdb, table_id, modelno, theta,
                ifnotexists=ifnotexists)

# XXX Background, &c.
def bayesdb_models_analyze(bdb, table_id, modelnos=None, iterations=1,
        max_seconds=None):
    assert iterations is None or 0 <= iterations
    assert iterations is not None or max_seconds is not None
    # Get a list of model numbers.  It must be a list so that we can
    # map the results from the engine back into the database.  Sort it
    # for consistency.
    if modelnos is None:
        modelnos = list(core.bayesdb_modelnos(bdb, table_id))
    else:
        modelnos = sorted(list(modelnos))
        for modelno in modelnos:
            if not core.bayesdb_has_model(bdb, table_id, modelno):
                raise ValueError("No such model: %d" % (modelno,))

    # Get the models and the metamodel engine.
    thetas = [core.bayesdb_model(bdb, table_id, modelno)
        for modelno in modelnos]
    engine = core.bayesdb_table_engine(bdb, table_id)
    X_L_list = [theta["X_L"] for theta in thetas]
    X_D_list = [theta["X_D"] for theta in thetas]

    # Iterate analysis.  If we have a deadline, do one step at a time
    # until we pass the deadline; otherwise, ask the metamodel engine
    # to do all the iterations for us.
    #
    # When counting time, we use time.time() rather than time.clock()
    # to count actual elapsed time, not just CPU time of the Python
    # process, in case the metamodel engine runs anything in another
    # process.
    #
    # XXX Using time.time() is wrong too -- we ought to use a
    # monotonic clock.  But &@^#!$& Python doesn't have one.
    iterations_completed = 0
    if max_seconds is not None:
        deadline = time.time() + max_seconds
    while (iterations is None or 0 < iterations) and \
          (max_seconds is None or time.time() < deadline):
        n_steps = 1
        if iterations is not None and max_seconds is None:
            n_steps = iterations
        X_L_list, X_D_list, diagnostics = engine.analyze(
            M_c=core.bayesdb_metadata(bdb, table_id),
            T=list(core.bayesdb_data(bdb, table_id)),
            do_diagnostics=True,
            # XXX Require the models share a common kernel_list.
            kernel_list=thetas[0]["model_config"]["kernel_list"],
            X_L=X_L_list,
            X_D=X_D_list,
            n_steps=n_steps,
        )
        iterations_completed += n_steps
        if iterations is not None:
            iterations -= n_steps
        # XXX Cargo-culted from old persistence layer's update_model.
        for modelno, theta in zip(modelnos, thetas):
            for diag_key in "column_crp_alpha", "logscore", "num_views":
                diag_list = [l[modelno] for l in diagnostics[diag_key]]
                if diag_key in theta and type(theta[diag_key]) == list:
                    theta[diag_key] += diag_list
                else:
                    theta[diag_key] = diag_list

    # Put the new models in the database.
    for (modelno, theta, X_L, X_D) \
            in zip(modelnos, thetas, X_L_list, X_D_list):
        # XXX For some reason, crosscat fails this assertion.
        # XXX assert theta == bayesdb_model(bdb, table_id, modelno)
        theta["iterations"] += iterations_completed
        theta["X_L"] = X_L
        theta["X_D"] = X_D
        core.bayesdb_set_model(bdb, table_id, modelno, theta)

### BayesDB column functions

# Two-column function:  CORRELATION [OF <col0> WITH <col1>]
def bql_column_correlation(bdb, table_id, colno0, colno1):
    import scipy.stats          # pearsonr, chi2_contingency, f_oneway
    M_c = core.bayesdb_metadata(bdb, table_id)
    qt = sqlite3_quote_name(core.bayesdb_table_name(bdb, table_id))
    qc0 = sqlite3_quote_name(core.bayesdb_column_name(bdb, table_id, colno0))
    qc1 = sqlite3_quote_name(core.bayesdb_column_name(bdb, table_id, colno1))
    data0 = []
    data1 = []
    n = 0
    try:
        data_sql = """
            SELECT %s, %s FROM %s WHERE %s IS NOT NULL AND %s IS NOT NULL
        """ % (qc0, qc1, qt, qc0, qc1)
        for row in bdb.sql_execute(data_sql):
            data0.append(core.bayesdb_value_to_code(M_c, colno0, row[0]))
            data1.append(core.bayesdb_value_to_code(M_c, colno1, row[1]))
            n += 1
    except KeyError:
        # XXX Ugh!  What to do?  If we allow importing any SQL table
        # after its schema has been specified, we can't add a foreign
        # key constraint to that table's schema (unless we recreate
        # the table, which is no good for remote read-only tables).
        # We could deal exclusively in heuristic imports of CSV tables
        # and not bother to support importing SQL tables.
        return 0
    assert n == len(data0)
    assert n == len(data1)
    # XXX Push this into the metamodel.
    modeltype0 = M_c["column_metadata"][colno0]["modeltype"]
    modeltype1 = M_c["column_metadata"][colno1]["modeltype"]
    correlation = float("NaN")  # Default result.
    if core.bayesdb_modeltype_numerical_p(modeltype0) and \
       core.bayesdb_modeltype_numerical_p(modeltype1):
        # Both numerical: Pearson R^2
        sqrt_correlation, _p_value = scipy.stats.pearsonr(data0, data1)
        correlation = sqrt_correlation ** 2
    elif core.bayesdb_modeltype_discrete_p(modeltype0) and \
         core.bayesdb_modeltype_discrete_p(modeltype1):
        # Both categorical: Cramer's phi
        unique0 = unique_indices(data0)
        unique1 = unique_indices(data1)
        min_levels = min(len(unique0), len(unique1))
        if 1 < min_levels:
            ct = [0] * len(unique0)
            for i0, j0 in enumerate(unique0):
                ct[i0] = [0] * len(unique1)
                for i1, j1 in enumerate(unique1):
                    c = 0
                    for i in range(n):
                        if data0[i] == data0[j0] and data1[i] == data1[j1]:
                            c += 1
                    ct[i0][i1] = c
            chisq, _p, _dof, _expected = scipy.stats.chi2_contingency(ct,
                correction=False)
            correlation = math.sqrt(chisq / (n * (min_levels - 1)))
    else:
        # Numerical/categorical: ANOVA R^2
        if core.bayesdb_modeltype_discrete_p(modeltype0):
            assert core.bayesdb_modeltype_numerical_p(modeltype1)
            data_group = data0
            data_y = data1
        else:
            assert core.bayesdb_modeltype_numerical_p(modeltype0)
            assert core.bayesdb_modeltype_discrete_p(modeltype1)
            data_group = data1
            data_y = data0
        group_values = unique(data_group)
        n_groups = len(group_values)
        if n_groups < n:
            samples = []
            for v in group_values:
                sample = []
                for i in range(n):
                    if data_group[i] == v:
                        sample.append(data_y[i])
                samples.append(sample)
            F, _p = scipy.stats.f_oneway(*samples)
            correlation = 1 - 1/(1 + F*((n_groups - 1) / (n - n_groups)))
    return correlation

# Two-column function:  DEPENDENCE PROBABILITY [OF <col0> WITH <col1>]
def bql_column_dependence_probability(bdb, table_id, colno0, colno1):
    # XXX Push this into the metamodel.
    if colno0 == colno1:
        return 1
    count = 0
    nmodels = 0
    for X_L, X_D in core.bayesdb_latent_stata(bdb, table_id):
        nmodels += 1
        assignments = X_L["column_partition"]["assignments"]
        if assignments[colno0] != assignments[colno1]:
            continue
        if len(unique(X_D[assignments[colno0]])) <= 1:
            continue
        count += 1
    return float("NaN") if nmodels == 0 else (float(count) / float(nmodels))

# Two-column function:  MUTUAL INFORMATION [OF <col0> WITH <col1>]
def bql_column_mutual_information(bdb, table_id, colno0, colno1,
        numsamples=None):
    if numsamples is None:
        numsamples = 100
    engine = core.bayesdb_table_engine(bdb, table_id)
    X_L_list = list(core.bayesdb_latent_state(bdb, table_id))
    X_D_list = list(core.bayesdb_latent_data(bdb, table_id))
    r = engine.mutual_information(
        M_c=core.bayesdb_metadata(bdb, table_id),
        X_L_list=X_L_list,
        X_D_list=X_D_list,
        Q=[(colno0, colno1)],
        n_samples=int(math.ceil(float(numsamples) / len(X_L_list)))
    )
    # r has one answer per element of Q, so take the first one.
    r0 = r[0]
    # r0 is (mi, linfoot), and we want mi.
    mi = r0[0]
    # mi is [result for model 0, result for model 1, ...], and we want
    # the mean.
    return arithmetic_mean(mi)

# One-column function:  TYPICALITY OF <col>
def bql_column_typicality(bdb, table_id, colno):
    engine = core.bayesdb_table_engine(bdb, table_id)
    return engine.column_structural_typicality(
        X_L_list=list(core.bayesdb_latent_state(bdb, table_id)),
        col_id=colno
    )

# One-column function:  PROBABILITY OF <col>=<value>
def bql_column_value_probability(bdb, table_id, colno, value):
    engine = core.bayesdb_table_engine(bdb, table_id)
    M_c = core.bayesdb_metadata(bdb, table_id)
    try:
        code = core.bayesdb_value_to_code(M_c, colno, value)
    except KeyError:
        return 0
    X_L_list = list(core.bayesdb_latent_state(bdb, table_id))
    X_D_list = list(core.bayesdb_latent_data(bdb, table_id))
    # Fabricate a nonexistent (`unobserved') row id.
    fake_row_id = len(X_D_list[0][0])
    r = engine.simple_predictive_probability_multistate(
        M_c=M_c,
        X_L_list=X_L_list,
        X_D_list=X_D_list,
        Y=[],
        Q=[(fake_row_id, colno, code)]
    )
    return math.exp(r)

### BayesDB row functions

# Row function:  SIMILARITY TO <target_row> [WITH RESPECT TO <columns>]
def bql_row_similarity(bdb, table_id, rowid, target_rowid, *columns):
    if len(columns) == 0:
        columns = core.bayesdb_column_numbers(bdb, table_id)
    engine = core.bayesdb_table_engine(bdb, table_id)
    return engine.similarity(
        M_c=core.bayesdb_metadata(bdb, table_id),
        X_L_list=list(core.bayesdb_latent_state(bdb, table_id)),
        X_D_list=list(core.bayesdb_latent_data(bdb, table_id)),
        given_row_id=core.sqlite3_rowid_to_engine_row_id(rowid),
        target_row_id=core.sqlite3_rowid_to_engine_row_id(target_rowid),
        target_columns=list(columns)
    )

# Row function:  TYPICALITY
def bql_row_typicality(bdb, table_id, rowid):
    engine = core.bayesdb_table_engine(bdb, table_id)
    return engine.row_structural_typicality(
        X_L_list=list(core.bayesdb_latent_state(bdb, table_id)),
        X_D_list=list(core.bayesdb_latent_data(bdb, table_id)),
        row_id=core.sqlite3_rowid_to_engine_row_id(rowid)
    )

# Row function:  PREDICTIVE PROBABILITY OF <column>
def bql_row_column_predictive_probability(bdb, table_id, rowid, colno):
    engine = core.bayesdb_table_engine(bdb, table_id)
    M_c = core.bayesdb_metadata(bdb, table_id)
    value = core.bayesdb_cell_value(bdb, table_id, rowid, colno)
    code = core.bayesdb_value_to_code(M_c, colno, value)
    r = engine.simple_predictive_probability_multistate(
        M_c=M_c,
        X_L_list=list(core.bayesdb_latent_state(bdb, table_id)),
        X_D_list=list(core.bayesdb_latent_data(bdb, table_id)),
        Y=[],
        Q=[(core.sqlite3_rowid_to_engine_row_id(rowid), colno, code)]
    )
    return math.exp(r)

### Infer and simulate

def bql_infer(bdb, table_id, colno, rowid, value, confidence_threshold,
        numsamples=1):
    if value is not None:
        return value
    engine = core.bayesdb_table_engine(bdb, table_id)
    M_c = core.bayesdb_metadata(bdb, table_id)
    column_names = core.bayesdb_column_names(bdb, table_id)
    qt = sqlite3_quote_name(core.bayesdb_table_name(bdb, table_id))
    qcns = ",".join(map(sqlite3_quote_name, column_names))
    select_sql = "SELECT %s FROM %s WHERE _rowid_ = ?" % (qcns, qt)
    c = bdb.sql_execute(select_sql, (rowid,))
    row = c.fetchone()
    assert row is not None
    assert c.fetchone() is None
    row_id = core.sqlite3_rowid_to_engine_row_id(rowid)
    code, confidence = engine.impute_and_confidence(
        M_c=M_c,
        X_L=list(core.bayesdb_latent_state(bdb, table_id)),
        X_D=list(core.bayesdb_latent_data(bdb, table_id)),
        Y=[(row_id, colno_, core.bayesdb_value_to_code(M_c, colno_, value))
            for colno_, value in enumerate(row) if value is not None],
        Q=[(row_id, colno)],
        n=numsamples
    )
    if confidence >= confidence_threshold:
        return core.bayesdb_code_to_value(M_c, colno, code)
    else:
        return None

# XXX Create a virtual table that simulates results?
def bayesdb_simulate(bdb, table_id, constraints, colnos, numpredictions=1):
    engine = core.bayesdb_table_engine(bdb, table_id)
    M_c = core.bayesdb_metadata(bdb, table_id)
    qt = sqlite3_quote_name(core.bayesdb_table_name(bdb, table_id))
    max_rowid = core.bayesdb_sql_execute1(bdb,
        "SELECT max(_rowid_) FROM %s" % (qt,))
    fake_rowid = max_rowid + 1
    fake_row_id = core.sqlite3_rowid_to_engine_row_id(fake_rowid)
    # XXX Why special-case empty constraints?
    Y = None
    if constraints is not None:
        Y = [(fake_row_id, colno,
              core.bayesdb_value_to_code(M_c, colno, value))
             for colno, value in constraints]
    raw_outputs = engine.simple_predictive_sample(
        M_c=M_c,
        X_L=list(core.bayesdb_latent_state(bdb, table_id)),
        X_D=list(core.bayesdb_latent_data(bdb, table_id)),
        Y=Y,
        Q=[(fake_row_id, colno) for colno in colnos],
        n=numpredictions
    )
    return [[core.bayesdb_code_to_value(M_c, colno, code)
            for (colno, code) in zip(colnos, raw_output)]
        for raw_output in raw_outputs]

# XXX Create a virtual table that automatically does this on insert?
def bayesdb_insert(bdb, table_id, row):
    bayesdb_insertmany(bdb, table_id, [row])

def bayesdb_insertmany(bdb, table_id, rows):
    with bdb.savepoint():
        # Insert the data into the table.
        table_name = core.bayesdb_table_name(bdb, table_id)
        qt = sqlite3_quote_name(table_name)
        cursor = bdb.sql_execute("PRAGMA table_info(%s)" % (qt,))
        sql_column_names = [row[1] for row in cursor]
        qcns = map(sqlite3_quote_name, sql_column_names)
        sql = """
            INSERT INTO %s (%s) VALUES (%s)
        """ % (qt, ", ".join(qcns), ", ".join("?" for _qcn in qcns))
        for row in rows:
            bdb.sql_execute(sql, row)

        # Find the indices of the modelled columns.
        modelled_column_names = list(core.bayesdb_column_names(bdb, table_id))
        remap = []
        for i, name in enumerate(sql_column_names):
            colno = len(remap)
            if len(modelled_column_names) <= colno:
                break
            if casefold(name) == casefold(modelled_column_names[colno]):
                remap.append(i)
        assert len(remap) == len(modelled_column_names)
        M_c = core.bayesdb_metadata(bdb, table_id)
        modelled_rows = [[core.bayesdb_value_to_code(M_c, colno, row[i])
                for colno, i in enumerate(remap)]
            for row in rows]

        # Update the models.
        T = list(core.bayesdb_data(bdb, table_id))
        engine = core.bayesdb_table_engine(bdb, table_id)
        modelnos = core.bayesdb_modelnos(bdb, table_id)
        models = list(core.bayesdb_models(bdb, table_id, modelnos))
        X_L_list, X_D_list, T = engine.insert(
            M_c=M_c,
            T=T,
            X_L_list=[theta["X_L"] for theta in models],
            X_D_list=[theta["X_D"] for theta in models],
            new_rows=modelled_rows,
        )
        assert T == list(core.bayesdb_data(bdb, table_id)) + modelled_rows
        for (modelno, theta), (X_L, X_D) \
                in zip(zip(modelnos, models), zip(X_L_list, X_D_list)):
            theta["X_L"] = X_L
            theta["X_D"] = X_D
            core.bayesdb_set_model(bdb, table_id, modelno, theta)
