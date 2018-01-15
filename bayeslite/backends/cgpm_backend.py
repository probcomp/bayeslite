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

import itertools
import json
import math

from collections import Counter
from collections import defaultdict

from cgpm.crosscat.engine import Engine

import bayeslite.core as core

from bayeslite.exception import BQLError
from bayeslite.backend import BayesDB_Backend
from bayeslite.backend import bayesdb_backend_version
from bayeslite.sqlite3_util import sqlite3_quote_name
from bayeslite.util import casefold
from bayeslite.util import cursor_value
from bayeslite.util import json_dumps

import cgpm_alter.alterations

import cgpm_alter.parse
import cgpm_analyze.parse
import cgpm_schema.parse

CGPM_SCHEMA_1 = '''
INSERT INTO bayesdb_backend (name, version) VALUES ('cgpm', 1);

CREATE TABLE bayesdb_cgpm_generator (
    generator_id        INTEGER NOT NULL PRIMARY KEY
                            REFERENCES bayesdb_generator(id),
    schema_json         BLOB NOT NULL,
    engine_json         BLOB
);

CREATE TABLE bayesdb_cgpm_individual (
    generator_id        INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    table_rowid         INTEGER NOT NULL,
    cgpm_rowid          INTEGER NOT NULL,
    UNIQUE(generator_id, table_rowid),
    UNIQUE(generator_id, cgpm_rowid)
);

CREATE TABLE bayesdb_cgpm_category (
    generator_id        INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    colno               INTEGER NOT NULL CHECK (0 <= colno),
    value               TEXT NOT NULL,
    code                INTEGER NOT NULL,
    PRIMARY KEY(generator_id, colno, value),
    UNIQUE(generator_id, colno, code)
);
'''

CGPM_SCHEMA_2 = '''
UPDATE bayesdb_backend SET version = 2 WHERE name = 'cgpm';

ALTER TABLE bayesdb_cgpm_generator
    ADD COLUMN engine_stamp
    INTEGER NOT NULL DEFAULT 0;
'''

CGPM_SCHEMA_3 = '''
UPDATE bayesdb_backend SET version = 3 WHERE name = 'cgpm';

CREATE TABLE bayesdb_cgpm_modelno (
    generator_id        INTEGER NOT NULL,
    modelno             INTEGER NOT NULL,
    cgpm_modelno        INTEGER NOT NULL CHECK (0 <= cgpm_modelno),

    FOREIGN KEY (generator_id, modelno)
        REFERENCES bayesdb_generator_model(generator_id, modelno),
    UNIQUE(generator_id, modelno, cgpm_modelno)
);

INSERT INTO bayesdb_cgpm_modelno (generator_id, modelno, cgpm_modelno)
    SELECT generator_id, modelno, modelno
    FROM bayesdb_generator_model
    WHERE generator_id IN (
        SELECT id FROM bayesdb_generator WHERE backend = 'cgpm'
    );
'''


class CGPM_Backend(BayesDB_Backend):

    def __init__(self, cgpm_registry, multiprocess=None):
        self._cgpm_registry = cgpm_registry
        self._multiprocess = multiprocess
        # The cache is a dictionary whose keys are bayeslite.BayesDB objects,
        # and whose values are dictionaries (one cache per bdb). We need
        # self._cache to have separate caches for each bdb because the same
        # instance of CGPM_Backend may be used across multiple bdb instances.
        # This situation occurs when CGPM_Backend is used as a default
        # backend (refer to __init__.py, where the bayeslite module, upon
        # import, creates a single CGPM_Backend object to be used throughout
        # the python session).
        self._cache = dict()

    def name(self):
        return 'cgpm'

    def register(self, bdb):
        with bdb.savepoint():
            # Get the current version, if there is one.
            version = bayesdb_backend_version(bdb, self.name())
            # Check the version.
            if version is None:
                # No version -- CGPM schema not instantaited.
                # Instantiate it.
                bdb.sql_execute(CGPM_SCHEMA_1)
                version = 1
            if version == 1:
                # Install CGPM version 2.
                bdb.sql_execute(CGPM_SCHEMA_2)
                version = 2
            if version == 2:
                # Install CGPM version 3.
                bdb.sql_execute(CGPM_SCHEMA_3)
                version = 3
            if version != 3:
                # Unrecognized version.
                raise BQLError(bdb, 'CGPM already installed'
                    ' with unknown schema version: %d' % (version,))

    def set_multiprocess(self, switch):
        old = self._multiprocess
        self._multiprocess = switch
        return old

    def create_generator(self, bdb, generator_id, schema_tokens, **kwargs):
        schema_ast = cgpm_schema.parse.parse(schema_tokens)
        schema = _create_schema(bdb, generator_id, schema_ast, **kwargs)

        # Store the schema.
        bdb.sql_execute('''
            INSERT INTO bayesdb_cgpm_generator
                (generator_id, schema_json, engine_json) VALUES (?, ?, NULL)
        ''', (generator_id, json_dumps(schema)))

        # Get the underlying population and table.
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        table = core.bayesdb_population_table(bdb, population_id)
        qt = sqlite3_quote_name(table)

        # Assign latent variable numbers.
        for var, stattype in sorted(schema['latents'].iteritems()):
            core.bayesdb_add_latent(
                bdb, population_id, generator_id, var, stattype)

        # Assign codes to categories and consecutive column numbers to
        # the modeled variables.
        vars_cursor = bdb.sql_execute('''
            SELECT colno, name, stattype FROM bayesdb_variable
                WHERE population_id = ? AND 0 <= colno
        ''', (population_id,))
        for colno, name, stattype in vars_cursor:
            if _is_nominal(stattype):
                qn = sqlite3_quote_name(name)
                cursor = bdb.sql_execute('''
                    SELECT DISTINCT %s
                    FROM %s WHERE %s IS NOT NULL
                ''' % (qn, qt, qn))
                for code, (value,) in enumerate(cursor):
                    bdb.sql_execute('''
                        INSERT INTO bayesdb_cgpm_category
                            (generator_id, colno, value, code)
                            VALUES (?, ?, ?, ?)
                    ''', (generator_id, colno, value, code))

        # Assign contiguous 0-indexed ids to the individuals in the
        # table.
        if schema['subsample']:
            k = schema['subsample']
            n = cursor_value(
                bdb.sql_execute('SELECT COUNT(*) FROM %s' % (qt,)))
            cursor = bdb.sql_execute(
                'SELECT _rowid_ FROM %s ORDER BY _rowid_ ASC' % (qt,))
            uniform = bdb._prng.weakrandom_uniform
            # https://en.wikipedia.org/wiki/Reservoir_sampling
            samples = []
            for i, row in enumerate(cursor):
                if i < k:
                    samples.append(row)
                else:
                    r = uniform(i + 1)
                    if r < k:
                        samples[r] = row
            cursor = samples
        else:
            cursor = bdb.sql_execute('SELECT _rowid_ FROM %s' % (qt,))
        for cgpm_rowid, (table_rowid,) in enumerate(cursor):
            bdb.sql_execute('''
                INSERT INTO bayesdb_cgpm_individual
                    (generator_id, table_rowid, cgpm_rowid)
                    VALUES (?, ?, ?)
            ''', (generator_id, table_rowid, cgpm_rowid))

    def drop_generator(self, bdb, generator_id):
        # Remove the cache for this generator_id.
        self._del_cache_entry(bdb, generator_id, None)

        # Delete categories.
        bdb.sql_execute('''
            DELETE FROM bayesdb_cgpm_category WHERE generator_id = ?
        ''', (generator_id,))

        # Delete individual rowid mappings.
        bdb.sql_execute('''
            DELETE FROM bayesdb_cgpm_individual WHERE generator_id = ?
        ''', (generator_id,))

        # Delete modelno mappings.
        # Delete individual rowid mappings.
        bdb.sql_execute('''
            DELETE FROM bayesdb_cgpm_modelno WHERE generator_id = ?
        ''', (generator_id,))

        # Delete generator.
        bdb.sql_execute('''
            DELETE FROM bayesdb_cgpm_generator WHERE generator_id = ?
        ''', (generator_id,))

    def add_column(self, bdb, generator_id, colno):

        population_id = core.bayesdb_generator_population(bdb, generator_id)
        varname = core.bayesdb_variable_name(bdb, population_id, generator_id, colno)

        # Ensure variable exists in population.
        if not core.bayesdb_has_variable(bdb, population_id, None, varname):
            raise BQLError(bdb, 'No such column in population: %d' % (varname,))

        # Retrieve the stattype and default distribution.
        stattype = core.bayesdb_variable_stattype(
            bdb, population_id, generator_id, colno)
        if stattype not in _DEFAULT_DIST:
            raise BQLError(bdb, 'No distribution for stattype: %s' % (stattype))
        dist, params = _DEFAULT_DIST[stattype](bdb, generator_id, varname)

        # Update variable value mapping if nominal.
        if _is_nominal(stattype):
            table_name = core.bayesdb_population_table(bdb, population_id)
            qt = sqlite3_quote_name(table_name)
            qv = sqlite3_quote_name(varname)
            cursor = bdb.sql_execute('''
                SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL
            ''' % (qv, qt, qv))
            for code, (value,) in enumerate(cursor):
                bdb.sql_execute('''
                    INSERT INTO bayesdb_cgpm_category
                        (generator_id, colno, value, code)
                        VALUES (?, ?, ?, ?)
                ''', (generator_id, colno, value, code))

        # Retrieve the rows from the table.
        rows = list(itertools.chain.from_iterable(
            self._data(bdb, generator_id, [varname])))

        # Retrieve the engine.
        engine = self._engine(bdb, generator_id)

        # Go!
        engine.incorporate_dim(
            rows, [colno], cctype=dist, distargs=params,
            multiprocess=self._multiprocess)

        # Serialize the engine.
        self._serialize_engine(bdb, generator_id, engine, True)

    def initialize_models(self, bdb, generator_id, modelnos):
        # Caller should guarantee a nondegenerate request.
        n = len(modelnos)
        assert 0 < n

        # Retrieve existing modelnos.
        existing = bdb.sql_execute('''
            SELECT modelno FROM bayesdb_cgpm_modelno WHERE generator_id = ?
        ''', (generator_id,)).fetchall()

        # Initializing an engine for the first time.
        if not existing:
            # Get the schema.
            schema = self._schema(bdb, generator_id)

            # Initialize an engine.
            variables = schema['variables']
            engine = self._initialize_engine(bdb, generator_id, n, variables)

            # Initialize CGPMs for each state.
            for cgpm_ext in schema['cgpm_composition']:
                cgpms = [self._initialize_cgpm(bdb, generator_id, cgpm_ext)
                    for _ in xrange(n)]
                engine.compose_cgpm(cgpms, multiprocess=self._multiprocess)

            # Update bayesdb_cgpm_modelno table.
            bdb.sql_execute('''
                INSERT INTO bayesdb_cgpm_modelno (
                    generator_id, modelno, cgpm_modelno
                )
                SELECT generator_id, modelno, modelno
                FROM bayesdb_generator_model
                WHERE generator_id = ?
            ''', (generator_id,))
        # Appending models to an existing engine.
        else:
            # Retrieve the engine.
            engine = self._engine(bdb, generator_id)

            # Confirm requested modelnos do not include existing models.
            intersection = [m for m in existing if m[0] in modelnos]
            if intersection:
                raise BQLError(bdb,
                    'Cannot initialize existing models: %s.' % (intersection,))

            # Add the states.
            engine.add_state(
                count=len(modelnos), multiprocess=self._multiprocess)

            # Update bayesdb_cgpm_modelno table.
            cgpm_modelnos = range(len(existing), len(existing) + len(modelnos))
            for modelno, cgpm_modelno in zip(modelnos, cgpm_modelnos):
                bdb.sql_execute('''
                    INSERT INTO bayesdb_cgpm_modelno
                        (generator_id, modelno, cgpm_modelno)
                        VALUES (?, ?, ?)
                ''', (generator_id, modelno, cgpm_modelno))

        # Serialize the engine without caching.
        self._serialize_engine(bdb, generator_id, engine, False)

    def drop_models(self, bdb, generator_id, modelnos=None):
        # Retrieve currently initialized modelnos.
        cursor = bdb.sql_execute('''
            SELECT modelno FROM bayesdb_cgpm_modelno
            WHERE generator_id = ?
        ''', (generator_id,))
        modelnos_existing = [m[0] for m in cursor]

        # Drop all models?
        if modelnos is None or sorted(modelnos_existing) == sorted(modelnos):
            # Set engine JSON to null.
            bdb.sql_execute('''
                UPDATE bayesdb_cgpm_generator SET engine_json = NULL
                WHERE generator_id = ?
            ''', (generator_id,))
            # Clear mapping of modelnos.
            bdb.sql_execute('''
                DELETE FROM bayesdb_cgpm_modelno
                WHERE generator_id = ?
            ''', (generator_id,))
            # Delete the engine from the cache.
            self._del_cache_entry(bdb, generator_id, 'engine')
        # Drop some models.
        else:
            engine = self._engine(bdb, generator_id)
            cgpm_modelnos = self._get_modelnos(bdb, generator_id, modelnos)
            for m in cgpm_modelnos:
                del engine.states[m]
                # Delete the modelno entry.
                bdb.sql_execute('''
                    DELETE FROM bayesdb_cgpm_modelno
                    WHERE generator_id = ? AND cgpm_modelno = ?
                ''', (generator_id, m,))
                # Decrement all greater cgpm_modelnos by 1.
                bdb.sql_execute('''
                    UPDATE bayesdb_cgpm_modelno
                    SET cgpm_modelno = cgpm_modelno - 1
                    WHERE generator_id = ? AND cgpm_modelno > ?
                ''', (generator_id, m,))
            # Assert that the cgpm_modelnos are sequential.
            cursor = bdb.sql_execute('''
                SELECT cgpm_modelno FROM bayesdb_cgpm_modelno
                WHERE generator_id = ? ORDER BY cgpm_modelno ASC
            ''', (generator_id,))
            modelnos_cgpm_new = [m[0] for m in cursor]
            assert modelnos_cgpm_new == range(engine.num_states())
            # Serialize the engine.
            self._serialize_engine(bdb, generator_id, engine, True)

    def alter(self, bdb, generator_id, modelnos, commands):
        # Get the population_id.
        population_id = core.bayesdb_generator_population(bdb, generator_id)

        # Get the modelnos.
        cgpm_modelnos = self._get_modelnos(bdb, generator_id, modelnos)

        # Retrieve the engine.
        engine = self._engine(bdb, generator_id)

        # Find baseline variable numbers for error checking.
        vars_baseline = engine.states[0].outputs

        # Retrieve the AST.
        alter_ast =  cgpm_alter.parse.parse(commands)

        # Prepare alteration functions.
        alter_funcs = []

        # Reduce verbosity with helper functions.
        get_varno = lambda variable: core.bayesdb_variable_number(
            bdb, population_id, generator_id, variable)
        has_var = lambda variable: core.bayesdb_has_variable(
            bdb, population_id, generator_id, variable)

        # Interpret the AST.
        for clause in alter_ast:

            # ENSURE VARIABLES * DEPENDENT|INDEPENDENT.
            if isinstance(clause, cgpm_alter.parse.SetVarDependency):
                # Get the columns to migrate. Reject anything other than *.
                if clause.columns != cgpm_alter.parse.SqlAll:
                    raise BQLError(bdb,
                        'Only all variables can be made (in)dependent, use *.')
                func = cgpm_alter.alterations.make_set_var_dependency(
                    clause.dependency)

                # Make the alteration function.
                alter_funcs.append(func)

            # ENSURE VARIABLES <columns0> IN VIEW OF <column1>
            # ENSURE VARIABLES <columns0> IN SINGLETON VIEW.
            elif isinstance(clause, cgpm_alter.parse.SetVarCluster):
                # Get the columns to migrate.
                if clause.columns0 == cgpm_alter.parse.SqlAll:
                    varnos0 = vars_baseline
                else:
                    unknown = [v for v in clause.columns0 if not has_var(v)]
                    if unknown:
                        raise BQLError(bdb,
                            'Unknown variables: %r' % (sorted(unknown),))
                    varnos0 = map(get_varno, clause.columns0)
                    # Reject non-CrossCat variables.
                    columns_invalid = [
                        column
                        for (column, varno) in zip(clause.columns0, varnos0)
                        if varno not in vars_baseline
                    ]
                    if columns_invalid:
                        raise BQLError(bdb,
                            'Only baseline variables can be altered: %s'
                            % (columns_invalid,))

                # Get the column indexing the view.
                if clause.column1 == cgpm_alter.parse.SingletonCluster:
                    varno1 = clause.column1
                else:
                    if not core.bayesdb_has_variable(
                            bdb, population_id, generator_id, clause.column1):
                        raise BQLError(bdb,
                            'Unknown variable: %s' % (clause.column1,))
                    varno1 = get_varno(clause.column1)
                    if varno1 not in vars_baseline:
                        raise BQLError(bdb,
                            'Only baseline variables can be specified: %s'
                            % (clause.column1))

                # Make the alteration function.
                func = cgpm_alter.alterations.make_set_var_cluster(
                    set(varnos0), varno1)
                alter_funcs.append(func)

            # SET VIEW CONCENTRATION PARAMETER TO <concentration>
            elif isinstance(clause, cgpm_alter.parse.SetVarClusterConc):
                func = cgpm_alter.alterations.make_set_var_cluster_conc(
                    clause.concentration)
                alter_funcs.append(func)

            # ENSURE ROWS <rows0> IN CLUSTER OF ROW <row1> WITHIN VIEW OF <col>.
            # ENSURE ROWS <rows0> IN SINGLETON CLUSTER WITHIN VIEW OF <col>.
            elif isinstance(clause, cgpm_alter.parse.SetRowCluster):
                # Get the context view.
                if not has_var(clause.column):
                    raise BQLError(bdb,
                        'Unknown columns: %s' % (clause.column,))
                varno = get_varno(clause.column)
                if varno not in vars_baseline:
                    raise BQLError(bdb,
                        'Only baseline variables can be specified: %s'
                        % (clause.column))

                # Get rows to move.
                if clause.rows0 == cgpm_alter.parse.SqlAll:
                    cursor = bdb.sql_execute('''
                        SELECT cgpm_rowid
                            FROM bayesdb_cgpm_individual
                            WHERE generator_id = ?
                    ''', (generator_id,))
                    rows0 = [c[0] for c in cursor]
                else:
                    unknown_rowids = []
                    def get_cgpm_rowid(rowid_user):
                        try:
                            return self._cgpm_rowid(
                                bdb, generator_id, rowid_user, nullok=False)
                        except ValueError:
                            unknown_rowids.append(rowid_user)
                            return None
                    rows0 = map(get_cgpm_rowid, clause.rows0)
                    if unknown_rowids:
                        raise BQLError(bdb,
                            'Unknown rows: %s' % (unknown_rowids,))

                # Get the reference row.
                if clause.row1 == cgpm_alter.parse.SingletonCluster:
                    row1 = clause.row1
                else:
                    try:
                        row1 = self._cgpm_rowid(
                            bdb, generator_id, clause.row1, nullok=False)
                    except ValueError:
                        raise BQLError(bdb, 'Unknown row: %s' % (clause.row1))

                # Make the alteration function.
                func = cgpm_alter.alterations.make_set_row_cluster(
                    rows0, row1, varno)
                alter_funcs.append(func)

            # SET ROW CLUSTER CONCENTRATION WITHIN VIEW OF <column> TO <conc>.
            elif isinstance(clause, cgpm_alter.parse.SetRowClusterConc):
                # Get the context column.
                if not has_var(clause.column):
                    raise BQLError(bdb,
                        'Unknown columns: %s' % (clause.column,))
                varno = get_varno(clause.column)
                if varno not in vars_baseline:
                    raise BQLError(bdb,
                        'Only baseline variables can be specified: %s'
                        % (clause.column,))

                # Make the alteration function.
                func = cgpm_alter.alterations.make_set_row_cluster_conc(
                    varno, clause.concentration)
                alter_funcs.append(func)

        # Execute alteration functions.
        engine.alter(alter_funcs, statenos=cgpm_modelnos,
            multiprocess=self._multiprocess)

        # Serialize the engine.
        self._serialize_engine(bdb, generator_id, engine, True)

    def analyze_models(
            self, bdb, generator_id, modelnos=None, iterations=None,
            max_seconds=None, ckpt_iterations=None, ckpt_seconds=None,
            program=None):
        # No analysis specified.
        if not iterations and not max_seconds:
            return

        # Checkpoint by seconds disabled.
        if ckpt_seconds:
            raise NotImplementedError('Checkpoint by seconds in CGPM analyze.')

        if program is None:
            program = []

        # Get the modelnos.
        cgpm_modelnos = self._get_modelnos(bdb, generator_id, modelnos)

        # Retrieve the engine.
        engine = self._engine(bdb, generator_id)

        # Retrieve user-specified target variables to transition.
        analyze_ast = cgpm_analyze.parse.parse(program)
        vars_user, rowids_user, subproblems, optimized, quiet = \
            _retrieve_analyze_variables(bdb, generator_id, analyze_ast)

        # Explicitly suppress progress bar if quiet, otherwise use default.
        progress = False if quiet else None

        vars_baseline = engine.states[0].outputs
        vars_foreign = list(itertools.chain.from_iterable([
            cgpm.outputs for cgpm in engine.states[0].hooked_cgpms.itervalues()
        ]))

        # By default transition all baseline variables only.
        vars_target_baseline = vars_baseline
        vars_target_foreign = None

        # Partition user-specified variables into baseline and foreign.
        if vars_user:
            intersection = lambda a,b: [x for x in a if x in b]
            vars_target_baseline = intersection(vars_user, vars_baseline)
            vars_target_foreign = intersection(vars_user, vars_foreign)

        assert vars_target_baseline or vars_target_foreign

        # Convert the user rowids to cgpm rowids.
        rowids_cgpm = None
        if rowids_user:
            unknown_rowids = []
            def get_cgpm_rowid(rowid_user):
                try:
                    return self._cgpm_rowid(
                        bdb, generator_id, rowid_user, nullok=False)
                except ValueError:
                    unknown_rowids.append(rowid_user)
                    return None
            rowids_cgpm = map(get_cgpm_rowid, rowids_user)
            if unknown_rowids:
                raise BQLError(bdb, 'Unknown ROWS: %s' % (rowids_user,))

        # Convert the user subproblems into kernels.
        kernels = None
        if subproblems:
            backend = optimized.backend if optimized else 'cgpm'
            kernels = self._convert_subproblems_to_kernel(
                bdb, subproblems, backend)

        # Error: Timed analysis is incompatible with mixed baseline and foreign.
        if max_seconds and (vars_target_baseline and vars_target_foreign):
            raise BQLError(bdb,
                'Timed analysis accepts foreign xor baseline variables.')

        # Error: Targeted analysis with loom backend is not supported.
        if optimized and optimized.backend == 'loom':
            if vars_user:
                raise BQLError(bdb, 'No VARIABLES or SKIP in Loom.')
            if rowids_user:
                raise BQLError(bdb, 'No ROWS in Loom.')

        # Run transitions on baseline variables.
        if vars_target_baseline:
            if optimized and optimized.backend == 'loom':
                engine.transition_loom(
                    N=iterations,
                    S=max_seconds,
                    progress=progress,
                    checkpoint=ckpt_iterations,
                    multiprocess=self._multiprocess,
                )
            elif optimized and optimized.backend == 'lovecat':
                engine.transition_lovecat(
                    N=iterations,
                    S=max_seconds,
                    kernels=kernels,
                    cols=vars_target_baseline,
                    rowids=rowids_cgpm,
                    progress=progress,
                    checkpoint=ckpt_iterations,
                    statenos=cgpm_modelnos,
                    multiprocess=self._multiprocess,
                )
            else:
                engine.transition(
                    N=iterations,
                    S=max_seconds,
                    kernels=kernels,
                    cols=vars_target_baseline,
                    rowids=rowids_cgpm,
                    progress=progress,
                    checkpoint=ckpt_iterations,
                    statenos=cgpm_modelnos,
                    multiprocess=self._multiprocess,
                )

        # Run transitions on foreign variables.
        if vars_target_foreign:
            engine.transition_foreign(
                N=iterations,
                S=max_seconds,
                cols=vars_target_foreign,
                progress=progress,
                statenos=cgpm_modelnos,
                multiprocess=self._multiprocess,
            )

        # Serialize the engine.
        self._serialize_engine(bdb, generator_id, engine, True)


    def column_dependence_probability(
            self, bdb, generator_id, modelnos, colno0, colno1):
        # Optimize special-case vacuous case of self-dependence.
        # XXX Caller should avoid this. We also should really return a list
        # of ones equal to the number of user-specified models (or all).
        if colno0 == colno1:
            return [1]

        # Get the modelnos.
        cgpm_modelnos = self._get_modelnos(bdb, generator_id, modelnos)

        # Get the engine.
        engine = self._engine(bdb, generator_id)

        # Engine gives us a list of dependence probabilities which it is our
        # responsibility to integrate over.
        depprob_list = engine.dependence_probability(
            colno0, colno1, statenos=cgpm_modelnos,
            multiprocess=self._multiprocess)

        return depprob_list

    def column_mutual_information(
            self, bdb, generator_id, modelnos, colnos0, colnos1,
            constraints=None, numsamples=None):
        # XXX Default number of samples drawn from my arse.
        if numsamples is None:
            numsamples = 1000

        # Get the modelnos.
        cgpm_modelnos = self._get_modelnos(bdb, generator_id, modelnos)

        # Get the engine.
        engine = self._engine(bdb, generator_id)

        # Build the evidence, ignoring nan values and converting nominals.
        evidence = constraints and {
            colno: (self._to_numeric(bdb, generator_id, colno, value)
                if value is not None else None)
            for colno, value in constraints
        }

        # Engine gives us a list of samples which it is our
        # responsibility to integrate over.
        mi_list = engine.mutual_information(
            colnos0, colnos1, constraints=evidence, N=numsamples,
            progress=True, statenos=cgpm_modelnos,
            multiprocess=self._multiprocess)

        # Pass through the distribution of CMI to BayesDB without aggregation.
        return mi_list

    def row_similarity(
            self, bdb, generator_id, modelnos, rowid, target_rowid, colnos):
        # Retrieve the modelnos.
        cgpm_modelnos = self._get_modelnos(bdb, generator_id, modelnos)

        # Map the variable and individual indexing.
        cgpm_rowid = self._cgpm_rowid(bdb, generator_id, rowid)
        cgpm_target_rowid = self._cgpm_rowid(bdb, generator_id, target_rowid)

        # XXX TODO: If neither rowids are incorporated, return None.
        if cgpm_rowid == -1 or cgpm_target_rowid == -1:
            return [float('nan')]

        # Get the engine.
        engine = self._engine(bdb, generator_id)

        # Engine gives us a list of similarities which it is our
        # responsibility to integrate over.
        similarity_list = engine.row_similarity(
            cgpm_rowid, cgpm_target_rowid, colnos, statenos=cgpm_modelnos,
            multiprocess=self._multiprocess)

        return similarity_list

    def predictive_relevance(
            self, bdb, generator_id, modelnos, rowid_target, rowid_query,
            hypotheticals, colno):
        # Retrieve cgpm modelnos.
        cgpm_modelnos = self._get_modelnos(bdb, generator_id, modelnos)

        # Convert target rowid
        cgpm_rowid_target = self._cgpm_rowid(bdb, generator_id, rowid_target)

        # If the target rowid is not incorporated, return nan.
        if cgpm_rowid_target == -1:
            return [float('nan')]

        # Convert query table rowids to cgpm rowids.
        # XXX TODO: Move any items of cgpm_query_rowid which are not yet
        # incorporated into the `hypotheticals` list. For now, we will just
        # drop any rowids which are not incorporated.
        cgpm_rowid_query = filter(
            lambda r: r != -1,
            [self._cgpm_rowid(bdb, generator_id, r) for r in rowid_query]
        )

        # If the query rowids are all not incorporated and no hypotheticals,
        # return nan.
        if len(cgpm_rowid_query) + len(hypotheticals) == 0:
            return [float('nan')]

        # Build list of hypotheticals dictionaries.
        hypotheticals_numeric = [
            {c: self._to_numeric(bdb, generator_id, c, v) for c, v in row}
            for row in hypotheticals
        ]

        # Check for invalid user-specified values in the hypothetical rows.
        # XXX TODO: Report offending values.
        unknown = any(math.isnan(v) for d in hypotheticals_numeric
            for v in d.itervalues())
        if unknown:
            raise BQLError(bdb,
                'Unknown nominal values in predictive relevance: %s'
                % (hypotheticals,))

        # Get the engine.
        engine = self._engine(bdb, generator_id)

        # Go!
        similarity_list = engine.relevance_probability(
            cgpm_rowid_target, cgpm_rowid_query, colno, hypotheticals_numeric,
            statenos=cgpm_modelnos, multiprocess=self._multiprocess)

        return similarity_list

    def predict_confidence(
            self, bdb, generator_id, modelnos, rowid, colno, numsamples=None):
        if not numsamples:
            numsamples = 2
        assert numsamples > 0

        def _impute_nominal(sample):
            counts = Counter(s[0] for s in sample)
            mode_count = max(counts[v] for v in counts)
            pred = iter(v for v in counts if counts[v] == mode_count).next()
            conf = float(mode_count) / numsamples
            return pred, conf

        def _impute_numerical(sample):
            pred = sum(s[0] for s in sample) / float(len(sample))
            conf = 0 # XXX Punt confidence for now
            return pred, conf

        # Retrieve the samples. Specifying `rowid` ensures that relevant
        # constraints are retrieved by `simulate`, so provide empty constraints.
        sample = self.simulate_joint(
            bdb, generator_id, modelnos, rowid, [colno], [], numsamples)

        # Determine the imputation strategy (mode or mean).
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        stattype = core.bayesdb_variable_stattype(
            bdb, population_id, generator_id, colno)
        if _is_nominal(stattype):
            return _impute_nominal(sample)
        else:
            return _impute_numerical(sample)

    def simulate_joint(
            self, bdb, generator_id, modelnos, rowid, targets, constraints,
            num_samples=None, accuracy=None):
        if num_samples is None:
            num_samples = 1
        cgpm_modelnos = self._get_modelnos(bdb, generator_id, modelnos)
        full_constraints = self._merge_user_table_constraints(
            bdb, generator_id, rowid, targets, constraints)
        # Perpare the rowid, query, and evidence for cgpm.
        cgpm_rowid = self._cgpm_rowid(bdb, generator_id, rowid)
        cgpm_targets = targets
        cgpm_constraints = {}
        for colno, value in full_constraints:
            value_numeric = self._to_numeric(bdb, generator_id, colno, value)
            if not math.isnan(value_numeric):
                cgpm_constraints.update({colno: value_numeric})
        # Retrieve the engine.
        engine = self._engine(bdb, generator_id)
        samples = engine.simulate(
            rowid=cgpm_rowid,
            targets=cgpm_targets,
            constraints=cgpm_constraints,
            inputs=None,
            N=num_samples,
            accuracy=accuracy,
            statenos=cgpm_modelnos,
            multiprocess=self._multiprocess
        )
        weighted_samples = engine._likelihood_weighted_resample(
            samples=samples,
            rowid=cgpm_rowid,
            constraints=cgpm_constraints,
            inputs=None,
            statenos=cgpm_modelnos,
            multiprocess=self._multiprocess
        )
        def map_value(colno, value):
            return self._from_numeric(bdb, generator_id, colno, value)
        return [
            [map_value(colno, row[colno]) for colno in cgpm_targets]
            for row in weighted_samples
        ]

    def logpdf_joint(
            self, bdb, generator_id, modelnos, rowid, targets, constraints):
        cgpm_modelnos = self._get_modelnos(bdb, generator_id, modelnos)
        cgpm_rowid = self._cgpm_rowid(bdb, generator_id, rowid)
        # TODO: Handle nan values in the logpdf query.
        cgpm_targets = {
            colno: self._to_numeric(bdb, generator_id, colno, value)
            for colno, value in targets
        }
        # Build the evidence, ignoring nan values.
        cgpm_constraints = {}
        for colno, value in constraints:
            value_numeric = self._to_numeric(bdb, generator_id, colno, value)
            if not math.isnan(value_numeric):
                cgpm_constraints.update({colno: value_numeric})
        # Retrieve the engine.
        engine = self._engine(bdb, generator_id)
        logpdfs = engine.logpdf(
            rowid=cgpm_rowid,
            targets=cgpm_targets,
            constraints=cgpm_constraints,
            inputs=None,
            accuracy=None,
            statenos=cgpm_modelnos,
            multiprocess=self._multiprocess
        )
        return engine._likelihood_weighted_integrate(
            logpdfs=logpdfs,
            rowid=cgpm_rowid,
            constraints=cgpm_constraints,
            inputs=None,
            statenos=cgpm_modelnos,
            multiprocess=self._multiprocess,
        )

    def _unique_rowid(self, rowids):
        if len(set(rowids)) != 1:
            raise ValueError('Multiple-row query: %r' % (list(set(rowids)),))
        return rowids[0]

    def _data(self, bdb, generator_id, vars):
        # Get the column numbers.
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        colnos = [
            core.bayesdb_variable_number(bdb, population_id,generator_id, var)
            for var in vars
        ]

        # Get the table name, quoted for constructing SQL.
        table_name = core.bayesdb_generator_table(bdb, generator_id)
        qt = sqlite3_quote_name(table_name)

        # Get the variable names, treating latents as NULL.
        qexpressions = ','.join(
            't.%s' % (sqlite3_quote_name(v),) if (0 <= colno) else 'NULL'
            for v, colno in zip(vars, colnos))

        # Get a cursor.
        cursor = bdb.sql_execute('''
            SELECT %s FROM %s AS t, bayesdb_cgpm_individual AS ci
                WHERE ci.generator_id = ?
                    AND ci.table_rowid = t._rowid_
            ORDER BY t._rowid_ ASC
        ''' % (qexpressions, qt), (generator_id,))

        # Map values to codes.
        def map_value(colno, value):
            return self._to_numeric(bdb, generator_id, colno, value)
        return [
            tuple(map_value(colno, x) for colno, x in zip(colnos, row))
            for row in cursor
        ]

    def _initialize_engine(self, bdb, generator_id, n, variables):
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        def map_var(var):
            return core.bayesdb_variable_number(
                bdb, population_id, generator_id, var)
        # If no variables in the population modeled by the gpmcc, then create 1
        # dummy variable with one measurement. The design space for how to
        # refactor cgpm.crosscat.State to initialize without any variables is
        # not simple, so we will live with this workaround for now.
        if not variables:
            (outputs, cctypes, distargs, gpmcc_data) = \
                [7**10], ['bernoulli'], [None], [[0]]
        else:
            outputs = [map_var(var) for var, _st, _cct, _da in variables]
            cctypes = [cctype for _n, _st, cctype, _da in variables]
            distargs = [distargs for _n, _st, _cct, distargs in variables]
            gpmcc_vars = [var for var, _stattype, _dist, _params in variables]
            gpmcc_data = self._data(bdb, generator_id, gpmcc_vars)
            # If gpmcc_data has any column which is all null, then crash early
            # and notify the user of all offending column names.
            n_rows = len(gpmcc_data)
            nulls = [
                v for i, v in enumerate(gpmcc_vars)
                if all(math.isnan(gpmcc_data[r][i]) for r in xrange(n_rows))
            ]
            if nulls:
                raise BQLError(bdb, 'Failed to initialize, '
                    'columns have all null values: %s' % repr(nulls))

        return Engine(
            gpmcc_data, num_states=n, rng=bdb.np_prng,
            multiprocess=self._multiprocess, outputs=outputs, cctypes=cctypes,
            distargs=distargs)

    def _initialize_cgpm(self, bdb, generator_id, cgpm_ext):
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        def map_var(var):
            return core.bayesdb_variable_number(
                bdb, population_id, generator_id, var)
        name = cgpm_ext['name']
        outputs = map(map_var, cgpm_ext['outputs'])
        inputs = map(map_var, cgpm_ext['inputs'])
        args = cgpm_ext.get('args', ())
        kwds = cgpm_ext.get('kwds', {})
        if name not in self._cgpm_registry:
            raise BQLError(bdb, 'Unknown CGPM: %s' % (repr(name),))
        cls = self._cgpm_registry[name]
        cgpm_vars = cgpm_ext['outputs'] + cgpm_ext['inputs']
        cgpm_data = self._data(bdb, generator_id, cgpm_vars)
        cgpm = cls(outputs, inputs, rng=bdb.np_prng, *args, **kwds)
        for cgpm_rowid, row in enumerate(cgpm_data):
            # CGPMs do not uniformly handle null values or missing
            # values sensibly yet, so until we have that sorted
            # out we both (a) omit nulls and (b) ignore errors in
            # incorporate.
            obs_values = {
                colno: row[i]
                for i, colno in enumerate(outputs)
                if not math.isnan(row[i])
            }
            n = len(outputs)
            input_values = {
                colno: row[n + i]
                for i, colno in enumerate(inputs)
                if not math.isnan(row[n + i])
            }
            try:
                cgpm.incorporate(cgpm_rowid, obs_values, input_values)
            except Exception:
                pass
        return cgpm

    def _schema(self, bdb, generator_id):
        # Probe the cache.
        cached_schema = self._get_cache_entry(bdb, generator_id, 'schema')
        if cached_schema is not None:
            return cached_schema

        # Not cached.  Load the schema from the database.
        cursor = bdb.sql_execute('''
            SELECT schema_json FROM bayesdb_cgpm_generator
                WHERE generator_id = ?
        ''', (generator_id,))
        schema_json = cursor_value(cursor, nullok=True)
        if schema_json is None:
            generator = core.bayesdb_generator_name(bdb, generator_id)
            raise BQLError(bdb, 'No such CGPM generator: %r' % (generator,))

        # Deserialize the schema.
        schema = json.loads(schema_json)

        # Cache it.
        self._set_cache_entry(bdb, generator_id, 'schema', schema)

        return schema

    def _engine(self, bdb, generator_id):
        # Retrieve the cache.

        # Probe the cache.
        cached_engine = self._engine_latest(bdb, generator_id)
        if cached_engine is not None:
            return cached_engine

        # Not cached or mismatched stamps. Load the engine from the database.
        cursor = bdb.sql_execute('''
            SELECT engine_json, engine_stamp FROM bayesdb_cgpm_generator
                WHERE generator_id = ?
        ''', (generator_id,)).fetchall()
        engine_json, engine_stamp = cursor[0]

        # Check if the generator has an initialized engine.
        if not engine_json:
            generator = core.bayesdb_generator_name(bdb, generator_id)
            raise BQLError(bdb, 'No models initialized for generator: %r'
                % (generator,))

        # Deserialize the engine.
        engine = Engine.from_metadata(
            json.loads(engine_json), rng=bdb.np_prng,
            multiprocess=self._multiprocess)

        # Cache the engine with its stamp.
        self._set_cache_entry(bdb, generator_id, 'engine', engine)
        self._set_cache_entry(bdb, generator_id, 'stamp', engine_stamp)

        return engine

    def _engine_latest(self, bdb, generator_id):
        # Check whether there is a cached_engine.
        cached_engine = self._get_cache_entry(bdb, generator_id, 'engine')
        if cached_engine is None:
            return None
        # Check whether cached_engine is latest version on disk.
        cached_stamp = self._get_cache_entry(bdb, generator_id, 'stamp')
        latest_stamp = self._engine_stamp(bdb, generator_id)
        # XXX This assertion, which we expected to be true in general, will
        # actually fail if the analyze statement was placed in a rollback, in
        # which case the cached stamp would have incremented but the latest
        # stamp would have been rolled back. Therefore, return an engine if and
        # only if the stamps match.
        # --- incorrect assertion --> assert cached_stamp <= latest_stamp
        return cached_engine if cached_stamp == latest_stamp else None

    def _engine_stamp(self, bdb, generator_id):
        cursor = bdb.sql_execute('''
            SELECT engine_stamp FROM bayesdb_cgpm_generator
                WHERE generator_id = ?
        ''', (generator_id,))
        return cursor_value(cursor)

    def _serialize_engine(self, bdb, generator_id, engine, cache):
        # Write the engine to JSON.
        engine_json = json_dumps(engine.to_metadata())

        # Increment the stamp.
        engine_stamp_old = self._engine_stamp(bdb, generator_id)
        engine_stamp_new = engine_stamp_old + 1

        # Update the engine and stamp.
        bdb.sql_execute('''
            UPDATE bayesdb_cgpm_generator
                SET engine_json = :engine_json,
                    engine_stamp = :engine_stamp
                WHERE generator_id = :generator_id
        ''', {
            'engine_json': engine_json,
            'engine_stamp': engine_stamp_new,
            'generator_id': generator_id,
        })

        # Add it to the cache.
        if cache:
            self._set_cache_entry(bdb, generator_id, 'engine', engine)
            self._set_cache_entry(bdb, generator_id, 'stamp', engine_stamp_new)


    def _retrieve_cache(self, bdb,):
        if bdb in self._cache:
            return self._cache[bdb]
        self._cache[bdb] = dict()
        return self._cache[bdb]

    def _set_cache_entry(self, bdb, generator_id, key, value):
        cache = self._retrieve_cache(bdb)
        if generator_id not in cache:
            cache[generator_id] = dict()
        cache[generator_id][key] = value

    def _get_cache_entry(self, bdb, generator_id, key):
        # Returns None if the generator_id or key do not exist.
        cache = self._retrieve_cache(bdb)
        if generator_id not in cache:
            return None
        if key not in cache[generator_id]:
            return None
        return cache[generator_id][key]

    def _del_cache_entry(self, bdb, generator_id, key):
        # If key is None, wipes bdb[generator_id] in its entirety.
        cache = self._retrieve_cache(bdb)
        if generator_id in cache:
            if key is None:
                del cache[generator_id]
            elif key in cache[generator_id]:
                del cache[generator_id][key]

    def _cgpm_rowid(self, bdb, generator_id, table_rowid, nullok=True):
        cursor = bdb.sql_execute('''
            SELECT cgpm_rowid FROM bayesdb_cgpm_individual
                WHERE generator_id = ? AND table_rowid = ?
        ''', (generator_id, table_rowid))
        cgpm_rowid = cursor_value(cursor, nullok=nullok)
        return cgpm_rowid if cgpm_rowid is not None else -1

    def _to_numeric(self, bdb, generator_id, colno, value):
        """Convert value in bayeslite to equivalent cgpm format."""
        if value is None:
            return float('NaN')
        # XXX Latent variables are not associated with an entry in
        # bayesdb_cgpm_category, so just pass through whatever value
        # the user supplied, as a float.
        if colno < 0:
            return float(value)
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        stattype = core.bayesdb_variable_stattype(
            bdb, population_id, generator_id, colno)
        if _is_nominal(stattype):
            cursor = bdb.sql_execute('''
                SELECT code FROM bayesdb_cgpm_category
                    WHERE generator_id = ? AND colno = ? AND value = ?
            ''', (generator_id, colno, value))
            integer = cursor_value(cursor, nullok=True)
            if integer is None:
                return float('NaN')
                # raise BQLError('Invalid category: %r' % (value,))
            return integer
        else:
            return value

    def _from_numeric(self, bdb, generator_id, colno, value):
        """Convert value in cgpm to equivalent bayeslite format."""
        if math.isnan(value):
            return None
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        stattype = core.bayesdb_variable_stattype(
            bdb, population_id, generator_id, colno)
        if _is_nominal(stattype):
            # XXX Latent variables are not associated with an entry in
            # bayesdb_cgpm_category, so just pass through whatever value cgpm
            # returns as a string.
            if colno < 0:
                return str(value)
            cursor = bdb.sql_execute('''
                SELECT value FROM bayesdb_cgpm_category
                    WHERE generator_id = ? AND colno = ? AND code = ?
            ''', (generator_id, colno, value))
            text = cursor_value(cursor, nullok=True)
            if text is None:
                raise BQLError('Invalid category: %r' % (value,))
            return text
        else:
            return value

    def _retrieve_baseline_variables(self, bdb, generator_id):
        # XXX Store this data in the bdb.
        engine = self._engine(bdb, generator_id)
        return engine.states[0].outputs

    def _retrieve_foreign_variables(self, bdb, generator_id):
        # XXX Store this data in the bdb.
        engine = self._engine(bdb, generator_id)
        return list(itertools.chain.from_iterable([
            cgpm.outputs for cgpm in engine.states[0].hooked_cgpms.itervalues()
        ]))

    def _merge_user_table_constraints(
            self, bdb, generator_id, rowid, targets, constraints):
        """Returns user specified constraints combined with values from table.

        Variables in `targets` will not be in the returned constraints, even
        if they exist in the base table. Howeer, any `targets` which eixst in
        the user specified `constraints` will remain there, and probably result
        in an error by cgpm.
        """
        # Handle `None` constraints.
        constraints = constraints or []
        # Retrieve table constraints.
        table_constraints = self._retrieve_table_constraints(
            bdb, generator_id, rowid)
        # Verify user constraints do not intersect table constraints.
        if table_constraints and constraints:
            user_cols = set(c[0] for c in constraints)
            table_cols = set(c[0] for c in table_constraints)
            intersection = set.intersection(user_cols, table_cols)
            if intersection:
                population_id = core.bayesdb_generator_population(
                    bdb, generator_id)
                names = [
                    (core.bayesdb_variable_name(
                        bdb, population_id, generator_id, c[0]),
                    c[1])
                    for c in constraints if c[0] in intersection
                ]
                raise BQLError(bdb,
                    'Cannot override existing values in table: %s' % (names,))
        # Ignore table_constraints if they are in the targets.
        table_constraints = [c for c in table_constraints if c[0] not in targets]
        return table_constraints + constraints

    def _retrieve_table_constraints(self, bdb, generator_id, rowid):
        """If `rowid` exists in table but is unincorporated, load the data."""
        # If rowid is a hypothetical cell for cgpm (does not exist in
        # bayesdb_cgpm_individual.table_rowid) but exists in the base table (by
        # INSERT INTO or SUBSAMPLE), then retrieve all values for rowid as the
        # constraints. Note that we do not need to populate constraints if the
        # rowid is already observed, which is done by cgpm.
        table = core.bayesdb_generator_table(bdb, generator_id)
        qt = sqlite3_quote_name(table)
        # Does the rowid exist in the base table?
        exists = bdb.sql_execute('''
            SELECT 1 FROM %s WHERE oid = ?
        ''' % (qt,), (rowid,)).fetchall()
        # Is the rowid incorporated into the cgpm?
        incorporated = bdb.sql_execute('''
            SELECT 1 FROM bayesdb_cgpm_individual
            WHERE generator_id = ? AND table_rowid = ?
            LIMIT 1
        ''', (generator_id, rowid,)).fetchall()
        # Populate values if necessary.
        table_constraints = []
        if exists and (not incorporated):
            population_id = core.bayesdb_generator_population(bdb, generator_id)
            row_values = core.bayesdb_population_row_values(
                bdb, population_id, rowid)
            variable_numbers = core.bayesdb_variable_numbers(
                bdb, population_id, None)
            table_constraints = [
                (varno, val)
                for varno, val in zip(variable_numbers, row_values)
                if val is not None
            ]
        return table_constraints

    def _get_modelnos(self, bdb, generator_id, modelnos):
        if modelnos is None:
            return modelnos
        cursor = bdb.sql_execute('''
            SELECT cgpm_modelno FROM bayesdb_cgpm_modelno
            WHERE generator_id = ? AND modelno IN (%s)
            ORDER BY cgpm_modelno DESC
        ''' % (','.join(map(str, modelnos)),), (generator_id,))
        modelnos_cgpm = [m[0] for m in cursor]
        # All modelnos are OK.
        if len(modelnos_cgpm) == len(modelnos):
            return modelnos_cgpm
        # Report bad modelnos.
        unions = str.join(' union all ', ['select %d' % (m,) for m in modelnos])
        qc = sqlite3_quote_name(str(modelnos[0]))
        unknown = bdb.sql_execute('''
            SELECT t.%s FROM (%s) AS t
            LEFT OUTER JOIN (
                SELECT modelno FROM bayesdb_cgpm_modelno
                WHERE generator_id = ?
            ) AS g
            ON t.%s = g.modelno
            WHERE g.modelno IS NULL
        ''' % (qc, unions, qc), (generator_id,)).fetchall()
        generator = core.bayesdb_generator_name(bdb, generator_id)
        raise BQLError(bdb,
            'Unknown modelnos for %s: %s' % (generator, unknown))

    def _convert_subproblems_to_kernel(self, bdb, subproblems, backend):
        # Keys are bayeslite subproblems, entries are cgpm where first element
        # is gpmcc kernel name, and secodn element is lovecat kernel name.
        if subproblems is None:
            return None
        conversions = {
            'variable_hyperparameters'               : {
                'cgpm'      : 'column_hypers',
                # XXX No column hyper transitions in lovecat.
            },
            'variable_clustering'                    : {
                'cgpm'      : 'columns',
                'lovecat'   : 'column_partition_assignments'
            },
            'variable_clustering_concentration'      : {
                'cgpm'      : 'alpha',
                'lovecat'   : 'column_partition_hyperparameter'
            },
            'row_clustering'                         : {
                'cgpm'      : 'rows',
                'lovecat'   : 'row_partition_assignments',
            },
            'row_clustering_concentration'           : {
                'cgpm'      : 'view_alphas',
                'lovecat'   : 'row_partition_hyperparameters',
            }
        }
        kernels = []
        unknown_subproblems = []
        unavailable_subproblems = []
        for subproblem in subproblems:
            if subproblem not in conversions:
                unknown_subproblems.append(subproblem)
            elif backend not in conversions[subproblem]:
                unavailable_subproblems.append(subproblem)
            else:
                kernels.append(conversions[subproblem][backend])
        if unknown_subproblems:
            raise BQLError(bdb,
                'Invalid subproblems: %s' % (unknown_subproblems,))
        if unavailable_subproblems:
            raise BQLError(bdb,
                'Subproblems not available in backend: %s, %s'
                % (unavailable_subproblems, backend,))
        return kernels


def _create_schema(bdb, generator_id, schema_ast):
    # Get some parameters.
    population_id = core.bayesdb_generator_population(bdb, generator_id)
    table = core.bayesdb_population_table(bdb, population_id)

    # State.
    variables = []
    variable_dist = {}
    latents = {}
    cgpm_composition = []
    modeled = set()
    default_modeled = set()
    subsample = None
    deferred_input = defaultdict(lambda: [])
    deferred_output = dict()

    # Error-reporting state.
    duplicate = set()
    unknown = set()
    needed = set()
    existing_latent = set()
    must_exist = []
    unknown_stattype = {}

    # XXX Convert all Foreign.exposed lists to Latent clauses.
    # Retrieve Foreign clauses with exposed variables.
    foreign_clauses = [
        c for c in schema_ast
        if isinstance(c, cgpm_schema.parse.Foreign) and len(c.exposed) > 0
    ]
    # Add the exposed variables to Foreign.outputs
    # Note that this assumes if there are K exposed variables, then they are
    # necessarily the last K outputs of the fc.outputs.
    for fc in foreign_clauses:
        fc.outputs.extend([e[0] for e in fc.exposed])

    # Convert exposed entries into Latent clauses.
    latent_vars = list(itertools.chain.from_iterable(
        c.exposed for c in foreign_clauses))
    latent_clauses = [cgpm_schema.parse.Latent(v,s) for (v,s) in latent_vars]
    # Append the Latent clauses to the ast.
    schema_ast.extend(latent_clauses)

    # Process each clause one by one.
    for clause in schema_ast:

        if isinstance(clause, cgpm_schema.parse.Basic):
            # Basic Crosscat component model: one variable to be put
            # into Crosscat views.
            var = clause.var
            dist = clause.dist
            params = dict(clause.params) # XXX error checking

            # Reject if the variable does not exist.
            if not core.bayesdb_has_variable(bdb, population_id, None, var):
                unknown.add(var)
                continue

            # Reject if the variable has already been modeled.
            if var in modeled:
                duplicate.add(var)
                continue

            # Reject if the variable is latent.
            if core.bayesdb_has_latent(bdb, population_id, var):
                existing_latent.add(var)
                continue

            # Get the column number.
            colno = core.bayesdb_variable_number(bdb, population_id, None, var)
            assert 0 <= colno

            # Add it to the list and mark it modeled by default.
            stattype = core.bayesdb_variable_stattype(
                bdb, population_id, generator_id, colno)
            variables.append([var, stattype, dist, params])
            assert var not in variable_dist
            variable_dist[var] = (stattype, dist, params)
            modeled.add(var)
            default_modeled.add(var)

        elif isinstance(clause, cgpm_schema.parse.Latent):
            var = clause.name
            stattype = clause.stattype

            # Reject if the variable has already been modeled by the
            # default model.
            if var in default_modeled:
                duplicate.add(var)
                continue

            # Reject if the variable even *exists* in the population
            # at all yet.
            if core.bayesdb_has_variable(bdb, population_id, None, var):
                duplicate.add(var)
                continue

            # Reject if the variable is already latent, from another
            # generator.
            if core.bayesdb_has_latent(bdb, population_id, var):
                existing_latent.add(var)
                continue

            # Reject if we've already processed it.
            if var in latents:
                duplicate.add(var)
                continue

            # Add it to the set of latent variables.
            latents[var] = stattype

        elif isinstance(clause, cgpm_schema.parse.Foreign):
            # Foreign model: some set of output variables is to be
            # modeled by foreign logic, possibly conditional on some
            # set of input variables.
            #
            # Gather up the state for a cgpm_composition record, which
            # we may have to do incrementally because it must refer to
            # the distribution types of variables we may not have
            # seen.
            name = clause.name
            outputs = clause.outputs
            inputs = clause.inputs

            output_stattypes = []
            output_statargs = []
            input_stattypes = []
            input_statargs = []
            distargs = {
                'inputs': {
                    'stattypes': input_stattypes,
                    'statargs': input_statargs
                },
                'outputs': {
                    'stattypes': output_stattypes,
                    'statargs': output_statargs,
                }
            }
            kwds = {'distargs': distargs}
            kwds.update(clause.params)

            # First make sure all the output variables exist and have
            # not yet been modeled.
            for var in outputs:
                must_exist.append(var)
                if var in modeled:
                    duplicate.add(var)
                    continue
                modeled.add(var)
                # Add the output statistical type and its parameters.
                i = len(output_stattypes)
                assert i == len(output_statargs)
                output_stattypes.append(None)
                output_statargs.append(None)
                deferred_output[var] = (output_stattypes, output_statargs, i)

            # Next make sure all the input variables exist, mark them
            # needed, and record where to put their distribution type
            # and parameters.
            for var in inputs:
                must_exist.append(var)
                needed.add(var)
                i = len(input_stattypes)
                assert i == len(input_statargs)
                input_stattypes.append(None)
                input_statargs.append(None)
                deferred_input[var].append((input_stattypes, input_statargs, i))

            # Finally, add a cgpm_composition record.
            cgpm_composition.append({
                'name': name,
                'inputs': inputs,
                'outputs': outputs,
                'kwds': kwds,
            })

        elif isinstance(clause, cgpm_schema.parse.Subsample):
            if subsample is not None:
                raise BQLError(bdb, 'Duplicate subsample: %r' % (clause.n,))
            subsample = clause.n

        else:
            raise BQLError(bdb, 'Unknown clause: %r' % (clause,))

    # Make sure all the outputs and inputs exist, either in the
    # population or as latents in this generator.
    for var in must_exist:
        if core.bayesdb_has_variable(bdb, population_id, None, var):
            continue
        if var in latents:
            continue
        unknown.add(var)

    # Raise an exception if there were duplicates or unknown
    # variables.
    if duplicate:
        raise BQLError(bdb,
            'Duplicate model variables: %r' % (sorted(duplicate),))
    if existing_latent:
        raise BQLError(bdb,
            'Latent variables already defined: %r' % (sorted(existing_latent),))
    if unknown:
        raise BQLError(bdb,
            'Unknown model variables: %r' % (sorted(unknown),))

    def default_dist(var, stattype):
        stattype = casefold(stattype)
        if stattype not in _DEFAULT_DIST:
            if var in unknown_stattype:
                assert unknown_stattype[var] == stattype
            else:
                unknown_stattype[var] = stattype
            return None
        dist, params = _DEFAULT_DIST[stattype](bdb, generator_id, var)
        return dist, params

    # Use the default distribution for any variables that remain to be
    # modeled, excluding any that are latent or that have statistical
    # types we don't know about.
    for var in core.bayesdb_variable_names(bdb, population_id, None):
        if var in modeled:
            continue
        colno = core.bayesdb_variable_number(bdb, population_id, None, var)
        assert 0 <= colno
        stattype = core.bayesdb_variable_stattype(
            bdb, population_id, generator_id, colno)
        distparams = default_dist(var, stattype)
        if distparams is None:
            continue
        dist, params = distparams
        variables.append([var, stattype, dist, params])
        assert var not in variable_dist
        variable_dist[var] = (stattype, dist, params)
        modeled.add(var)

    # Fill in the deferred_input statistical type assignments.
    for var in sorted(deferred_input.iterkeys()):
        # Check whether the variable is modeled.  If not, skip -- we
        # will fail later because this variable is guaranteed to also
        # be in needed.
        if var not in modeled:
            assert var in needed
            continue

        # Determine (possibly fictitious) distribution and parameters.
        if var in default_modeled:
            # Manifest variable modeled by default Crosscat model.
            assert var in variable_dist
            stattype, dist, params = variable_dist[var]
        else:
            # Modeled by a foreign model.  Assign a fictitious default
            # distribution because the 27B/6 of CGPM requires this.
            if var in latents:
                # Latent variable modeled by a foreign model.  Use
                # the statistical type specified for it.
                stattype = latents[var]
            else:
                # Manifest variable modeled by a foreign model.  Use
                # the statistical type in the population.
                assert core.bayesdb_has_variable(bdb, population_id, None, var)
                colno = core.bayesdb_variable_number(
                    bdb, population_id, None, var)
                stattype = core.bayesdb_variable_stattype(
                    bdb, population_id, generator_id, colno)
            distparams = default_dist(var, stattype)
            if distparams is None:
                continue
            dist, params = distparams

        # Assign the distribution and parameters.
        for cctypes, ccargs, i in deferred_input[var]:
            assert cctypes[i] is None
            assert ccargs[i] is None
            cctypes[i] = dist
            ccargs[i] = params

    # Fill in the deferred_output statistical type assignments. The need to be
    # in the form NUMERICAL or NOMINAL.
    for var in deferred_output:
        if var in latents:
            # Latent variable modeled by a foreign model.  Use
            # the statistical type specified for it.
            var_stattype = casefold(latents[var])
            if var_stattype not in _DEFAULT_DIST:
                if var in unknown_stattype:
                    assert unknown_stattype[var] == var_stattype
                else:
                    unknown_stattype[var] = var_stattype
            # XXX Cannot specify statargs for a latent variable. Trying to using
            # default_dist might lookup the counts for unique values of the
            # nominal in the base table causing a failure.
            var_statargs = {}
        else:
            # Manifest variable modeled by a foreign model.  Use
            # the statistical type and arguments from the population.
            assert core.bayesdb_has_variable(bdb, population_id, None, var)
            colno = core.bayesdb_variable_number(bdb, population_id, None, var)
            var_stattype = core.bayesdb_variable_stattype(
                bdb, population_id, generator_id, colno)
            distparams = default_dist(var, var_stattype)
            if distparams is None:
                continue
            _, var_statargs = distparams

        stattypes, statargs, i = deferred_output[var]
        assert stattypes[i] is None
        assert statargs[i] is None
        stattypes[i] = var_stattype
        statargs[i] = var_statargs

    if unknown_stattype:
        raise BQLError(bdb,
            'Unknown statistical types for variables: %r' %
            (sorted(unknown_stattype.iteritems(),)))

    # If there remain any variables that we needed to model, because
    # others are conditional on them, fail.
    needed -= modeled
    if needed:
        raise BQLError(bdb, 'Unmodellable variables: %r' % (needed,))

    # Finally, create a CGPM schema.
    return {
        'variables': variables,
        'cgpm_composition': cgpm_composition,
        'subsample': subsample,
        'latents': latents,
    }


def _retrieve_analyze_variables(bdb, generator_id, ast):

    population_id = core.bayesdb_generator_population(bdb, generator_id)

    # Transitions all variables and rows by default.
    variables = None
    rowids = None

    # Cycle through all Gibbs transition kernels by default.
    subproblems = None
    optimized = False
    quiet = False

    # Exactly 1 VARIABLES or SKIP clause supported for simplicity.
    seen_variables = False
    seen_skip = False

    for clause in ast:

        # Transition user specified variables only.
        if isinstance(clause, cgpm_analyze.parse.Variables):
            if seen_variables or seen_skip:
                raise BQLError(bdb,
                    'Only 1 VARIABLES or SKIP clause allowed in ANALYZE')
            seen_variables = True
            included = set()
            unknown = set()
            for var in clause.vars:
                if not core.bayesdb_has_variable(
                        bdb, population_id, generator_id, var):
                    unknown.add(var)
                included.add(var)
            if unknown:
                raise BQLError(bdb,
                    'Unknown variables in ANALYZE: %r'
                    % (sorted(unknown),))
            variables = sorted(included)

        # Transition all variables except user specified skip.
        elif isinstance(clause, cgpm_analyze.parse.Skip):
            if seen_variables or seen_skip:
                raise BQLError(bdb,
                    'Either VARIABLES or SKIP clause allowed in ANALYZE')
            seen_skip = True
            excluded = set()
            unknown = set()
            for var in clause.vars:
                if not core.bayesdb_has_variable(
                        bdb, population_id, generator_id, var):
                    unknown.add(var)
                excluded.add(var)
            if unknown:
                raise BQLError(bdb,
                    'Unknown variables in ANALYZE: %r'
                    % (sorted(unknown),))
            all_vars = core.bayesdb_variable_names(
                bdb, population_id, generator_id)
            variables = sorted(set(all_vars) - excluded)

        # Transition rows specified by user.
        elif isinstance(clause, cgpm_analyze.parse.Rows):
            if rowids is None:
                rowids = []
            rowids.extend(clause.rows)

        # Specify which transition subproblems to run.
        elif isinstance(clause, cgpm_analyze.parse.Subproblem):
            if subproblems is None:
                subproblems = []
            subproblems.extend(clause.subproblems)

        # Optimized non-cgpm analysis.
        elif isinstance(clause, cgpm_analyze.parse.Optimized):
            if clause.backend not in ['loom', 'lovecat']:
                raise BQLError(bdb,
                    'Unknown OPTIMIZED backend: %s' % (clause.backend))
            optimized = clause

        # QUIET suppresses the progress bar.
        elif isinstance(clause, cgpm_analyze.parse.Quiet):
            quiet = True

        # Unknown/impossible clause.
        else:
            raise BQLError(bdb, 'Unknown clause in ANALYZE: %s.' % (ast,))

    variable_numbers = [
        core.bayesdb_variable_number(bdb, population_id, generator_id, v)
        for v in variables
    ] if variables else None

    return (variable_numbers, rowids, subproblems, optimized, quiet)


def _default_nominal(bdb, generator_id, var):
    table = core.bayesdb_generator_table(bdb, generator_id)
    qt = sqlite3_quote_name(table)
    qv = sqlite3_quote_name(var)
    cursor = bdb.sql_execute('SELECT COUNT(DISTINCT %s) FROM %s' % (qv, qt))
    k = cursor_value(cursor)
    return 'categorical', {'k': k}

def _default_numerical(bdb, generator_id, var):
    return 'normal', {}

def _is_nominal(stattype):
    return casefold(stattype) == 'nominal'

_DEFAULT_DIST = {
    'counts':           _default_numerical,     # XXX change to poisson.
    'cyclic':           _default_numerical,     # XXX change to von mises.
    'magnitude':        _default_numerical,     # XXX change to lognormal.
    'nominal':          _default_nominal,
    'numerical':        _default_numerical,
    'numericalranged':  _default_numerical,     # XXX change to beta.
}
