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

# XXX TODO:
#
# [DONE] Teach analysis to update CGPMs too.
# [DONE] Kludge up Kepler's laws in Python.
# [DONE] Test hand-kludged CGPM registry.
# - Write stupid schema parser.  Adapt axch's?
# [DONE] Teach INITIALIZE MODELS to take a model_config argument for real:
#   . INITIALIZE 10 MODELS FOR <population> (x POISSON, y ...)
#   . rename model_config -> model schema
#   . Meaning: prior and likelihood model?  Or, `just SP'...
# [DONE] Introduce named model schemas:
#   . CREATE MODEL SCHEMA <schema> FOR <population> (...)
#   . INITIALIZE 40 MODELS FOR <population> USING <model schema>
# - Make populations metamodel-independent.
# - Change the nomenclature:
#   . generator -> population
#   . model schema -> metamodel
#   . row -> individual
#   . column -> variable
#   . metamodel -> ???
# - Adopt VentureScript CGPM.
#
# XXX Future TODO:
#
# - Rename keyword BY ---> WITHIN?
# - Conjecture more elaborate predicates on models?
#   . ANALYZE <population> MODELS WHERE DEP. PROB. OF X WITH Y > 0.5
#   . (What happens if predicate changes during analysis?)
#   . ESTIMATE PROBABILITY OF x = 1 GIVEN (y = 2) WITHIN <population>
#         USING MODELS WHERE DEP. PROB. OF x WITH y > 0.5
# - Introduce additional statistical types: count, boolean, &c.
#   . Clarify that `statistical type' means `support'.
# - Subsampling, per-model subsampling...

import contextlib
import json
import math

from collections import namedtuple

from cgpm.crosscat.engine import Engine

import bayeslite.core as core

from bayeslite.exception import BQLError
from bayeslite.metamodel import IBayesDBMetamodel
from bayeslite.sqlite3_util import sqlite3_quote_name
from bayeslite.util import casefold

import cgpm_parse

CGPM_SCHEMA_1 = '''
INSERT INTO bayesdb_metamodel (name, version) VALUES ('cgpm', 1);

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

class CGPM_Metamodel(IBayesDBMetamodel):
    def __init__(self, cgpm_registry):
        self._cgpm_registry = cgpm_registry

    def name(self):
        return 'cgpm'

    def register(self, bdb):
        with bdb.savepoint():
            # Get the current version, if there is one.
            version = bayesdb_metamodel_version(bdb, self.name())
            # Check the version.
            if version is None:
                # No version -- CGPM schema not instantaited.
                # Instantiate it.
                bdb.sql_execute(CGPM_SCHEMA_1)
                version = 1
            if version != 1:
                # Unrecognized version.
                raise BQLError(bdb, 'CGPM already installed'
                    ' with unknown schema version: %d' % (version,))

    def create_generator(self, bdb, generator_id, schema_tokens):
        schema_ast = cgpm_parse.parse(schema_tokens)
        schema = _create_schema(bdb, generator_id, schema_ast)

        # Store the schema.
        bdb.sql_execute('''
            INSERT INTO bayesdb_cgpm_generator (generator_id, schema_json)
                VALUES (?, ?)
        ''', (generator_id, json_dumps(schema)))

        # Get the underlying population and table.
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        table = core.bayesdb_population_table(bdb, population_id)
        qt = sqlite3_quote_name(table)

        # Assign codes to categories and consecutive column numbers to
        # the modelled variables.
        vars_cursor = bdb.sql_execute('''
            SELECT v.colno, c.name, v.stattype
                FROM bayesdb_variable AS v,
                    bayesdb_population AS p,
                    bayesdb_column AS c
                WHERE p.id = ? AND v.population_id = p.id
                    AND c.tabname = p.tabname AND c.colno = v.colno
                    AND 0 <= v.colno
        ''', (population_id,))
        for colno, name, stattype in vars_cursor:
            if casefold(stattype) == 'categorical':
                qn = sqlite3_quote_name(name)
                cursor = bdb.sql_execute('''
                    SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL
                ''' % (qn, qt, qn))
                for code, (value,) in enumerate(cursor):
                    bdb.sql_execute('''
                        INSERT INTO bayesdb_cgpm_category
                            (generator_id, colno, value, code)
                            VALUES (?, ?, ?, ?)
                    ''', (generator_id, colno, value, code))

        # Assign contiguous 0-indexed ids to the individuals in the
        # table.
        #
        # XXX subsampling
        cursor = bdb.sql_execute('SELECT _rowid_ FROM %s' % (qt,))
        for cgpm_rowid, (table_rowid,) in enumerate(cursor):
            bdb.sql_execute('''
                INSERT INTO bayesdb_cgpm_individual
                    (generator_id, table_rowid, cgpm_rowid)
                    VALUES (?, ?, ?)
            ''', (generator_id, table_rowid, cgpm_rowid))

    def drop_generator(self, bdb, generator_id):
        # Flush any cached schema or engines.
        cache = self._cache_nocreate(bdb)
        if cache is not None:
            if generator_id in cache.schema:
                del cache.schema[generator_id]
            if generator_id in cache.engine:
                del cache.engine[generator_id]

        # Delete categories.
        bdb.sql_execute('''
            DELETE FROM bayesdb_cgpm_category WHERE generator_id = ?
        ''', (generator_id,))

        # Delete individual rowid mappings.
        bdb.sql_execute('''
            DELETE FROM bayesdb_cgpm_individual WHERE generator_id = ?
        ''', (generator_id,))

        # Delete generator.
        bdb.sql_execute('''
            DELETE FROM bayesdb_cgpm_generator WHERE generator_id = ?
        ''', (generator_id,))

    def initialize_models(self, bdb, generator_id, modelnos):
        # Caller should guarantee a nondegenerate request.
        n = len(modelnos)
        assert 0 < n
        assert modelnos == range(n)     # XXX incremental model initialization

        # Get the schema.
        schema = self._schema(bdb, generator_id)

        # Initialize an engine.
        variables = schema['variables']
        engine = self._initialize_engine(bdb, generator_id, n, variables)

        # Initialize CGPMs for each state.
        for cgpm_ext in schema['cgpm_composition']:
            cgpm_initializer = self._cgpm_initializer(
                bdb, generator_id, cgpm_ext)
            for i, state in enumerate(engine.get_states(xrange(n))):
                cgpm = cgpm_initializer()
                _token = state.compose_cgpm(cgpm)
                engine.states[i] = state.to_metadata()

        # Store the newly initialized engine.
        engine_json = json_dumps(engine.to_metadata())
        bdb.sql_execute('''
            UPDATE bayesdb_cgpm_generator
                SET engine_json = :engine_json
                WHERE generator_id = :generator_id
        ''', {'generator_id': generator_id, 'engine_json': engine_json})

    def drop_models(self, bdb, generator_id, modelnos=None):
        assert modelnos is None

        # Delete the engine.
        bdb.sql_execute('''
            UPDATE bayesdb_cgpm_generator SET engine_json = NULL
                WHERE generator_id = ?
        ''', (generator_id,))

        # Delete it from the cache too if necessary.
        cache = self._cache(bdb)
        if cache is not None and generator_id in cache.engine:
            del cache.engine[generator_id]

    def analyze_models(self, bdb, generator_id, modelnos=None, iterations=1,
            max_seconds=None, ckpt_iterations=None, ckpt_seconds=None):
        assert modelnos is None

        if ckpt_iterations is not None or ckpt_seconds is not None:
            # XXX
            raise NotImplementedError('cgpm analysis checkpoint')

        # Get the engine.
        engine = self._engine(bdb, generator_id)

        # Do the transition.
        engine.transition(N=iterations, S=max_seconds, multithread=False)

        # Serialize the engine.
        engine_json = json_dumps(engine.to_metadata())

        # Update the engine.
        bdb.sql_execute('''
            UPDATE bayesdb_cgpm_generator
                SET engine_json = :engine_json
                WHERE generator_id = :generator_id
        ''', {'generator_id': generator_id, 'engine_json': engine_json})

    def column_dependence_probability(self, bdb, generator_id, modelno,
            colno0, colno1):
        # Special-case vacuous case of self-dependence.  XXX Caller
        # should avoid this.
        if colno0 == colno1:
            return 1

        # Get the engine.
        engine = self._engine(bdb, generator_id)

        # Go!
        return engine.dependence_probability(colno0, colno1, multithread=False)

    def column_mutual_information(self, bdb, generator_id, modelno,
            colno0, colno1, numsamples=None):
        # XXX Default number of samples drawn from my arse.
        if numsamples is None:
            numsamples = 1000

        # Get the engine.
        engine = self._engine(bdb, generator_id)

        # Go!
        mi_list = engine.mutual_information(colno0, colno1, N=numsamples)

        # Engine gives us a list of samples which it is our
        # responsibility to integrate over.
        #
        # XXX Is this integral correct?  Should it be weighted?
        return arithmetic_mean(mi_list)

    def row_similarity(self, bdb, generator_id, modelno, rowid, target_rowid,
            colnos):
        # Map the variable and individual indexing.
        cgpm_rowid = self._cgpm_rowid(bdb, generator_id, rowid)
        cgpm_target_rowid = self._cgpm_rowid(bdb, generator_id,
            target_rowid)

        # Get the engine.
        engine = self._engine(bdb, generator_id)

        # Go!
        return engine.row_similarity(cgpm_rowid, cgpm_target_rowid, colnos)

    def simulate_joint(self, bdb, generator_id, targets, constraints, modelno,
            num_predictions=None):
        if num_predictions is None:
            num_predictions = 1
        rowid = self._unique_rowid(
            [r for r, _c in targets] + [r for r, _c, _v in constraints])
        cgpm_rowid = self._cgpm_rowid(bdb, generator_id, rowid)
        cgpm_query = [colno for _r, colno in targets]
        cgpm_evidence = {
            colno: self._cgpm_value(bdb, generator_id, colno, value)
            for colno, value in constraints
        }
        engine = self._engine(bdb, generator_id)
        samples = engine.simulate(
            cgpm_rowid, cgpm_query, cgpm_evidence, N=num_predictions,
            multithread=False)
        weighted_samples = engine._process_samples(
            samples, cgpm_rowid, cgpm_evidence, multithread=False)
        return [[row[colno] for colno in cgpm_query]
            for row in weighted_samples]

    def logpdf_joint(self, bdb, generator_id, targets, constraints, modelno):
        rowid = self._unique_rowid(
            [r for r, _c, _v in targets + constraints])
        cgpm_rowid = self._cgpm_rowid(bdb, generator_id, rowid)
        cgpm_query = {
            colno: self._cgpm_value(bdb, generator_id, colno, value)
            for _r, colno, value in targets
        }
        cgpm_evidence = {
            colno: self._cgpm_value(bdb, generator_id, colno, value)
            for _r, colno, value in constraints
        }
        engine = self._engine(bdb, generator_id)
        logpdfs = engine.logpdf(
            cgpm_rowid, cgpm_query, cgpm_evidence, multithread=False)
        # XXX abstraction violation
        return engine._process_logpdfs(
            logpdfs, cgpm_rowid, cgpm_evidence, multithread=False)

    def _unique_rowid(self, rowids):
        if len(set(rowids)) != 1:
            raise ValueError('Multiple-row query: %r' % (list(set(rowids)),))
        return rowids[0]

    def _data(self, bdb, generator_id, vars):
        # Get the column numbers and statistical types.
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        colnos = [core.bayesdb_variable_number(bdb, population_id, var)
            for var in vars]
        stattypes = [core.bayesdb_variable_stattype(bdb, population_id, colno)
            for colno in colnos]

        # Get the table name, quoted for constructing SQL.
        table_name = core.bayesdb_generator_table(bdb, generator_id)
        qt = sqlite3_quote_name(table_name)

        # Create SQL expressions to cast each variable to the correct
        # affinity for its statistical type.
        qexpressions = ','.join('CAST(%s AS %s)' %
                ('NULL' if colno < 0 else sqlite3_quote_name(var),
                    sqlite3_quote_name(core.bayesdb_stattype_affinity(bdb,
                            stattype)))
            for var, (colno, stattype) in zip(vars, zip(colnos, stattypes)))

        # Get a cursor.
        #
        # XXX Subsampling?
        cursor = bdb.sql_execute('SELECT %s FROM %s ORDER BY _rowid_ ASC' %
            (qexpressions, qt))

        # Map values to codes.
        def map_value(colno, value):
            return self._cgpm_value(bdb, generator_id, colno, value)
        return [tuple(map_value(colno, x) for colno, x in zip(colnos, row))
            for row in cursor]

    def _initialize_engine(self, bdb, generator_id, n, variables):
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        def map_var(var):
            return core.bayesdb_variable_number(bdb, population_id, var)
        outputs = [map_var(var) for var, _st, _cct, _da in variables]
        cctypes = [cctype for _n, _st, cctype, _da in variables]
        distargs = [distargs for _n, _st, _cct, distargs in variables]
        vars = [var for var, _stattype, _dist, _params in variables]
        data = self._data(bdb, generator_id, vars)
        return Engine(
            data, num_states=n, rng=bdb.np_prng, multithread=False,
            outputs=outputs, cctypes=cctypes, distargs=distargs)

    def _cgpm_initializer(self, bdb, generator_id, cgpm_ext):
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        def map_var(var):
            return core.bayesdb_variable_number(bdb, population_id, var)
        name = cgpm_ext['name']
        outputs = map(map_var, cgpm_ext['outputs'])
        inputs = map(map_var, cgpm_ext['inputs'])
        args = cgpm_ext.get('args', ())
        kwds = cgpm_ext.get('kwds', {})
        if name not in self._cgpm_registry:
            raise BQLError(bdb, 'Unknown CGPM: %s' % (repr(name),))
        cls = self._cgpm_registry[name]
        vars = cgpm_ext['outputs'] + cgpm_ext['inputs']
        data = self._data(bdb, generator_id, vars)
        def initialize():
            cgpm = cls(outputs, inputs, rng=bdb.np_prng, *args, **kwds)
            for cgpm_rowid, row in enumerate(data):
                # CGPMs do not uniformly handle null values or missing
                # values sensibly yet, so until we have that sorted
                # out we both (a) omit nulls and (b) ignore errors in
                # incorporate.
                query = {
                    colno: row[i]
                    for i, colno in enumerate(outputs)
                    if not math.isnan(row[i])
                }
                n = len(outputs)
                evidence = {
                    colno: row[n + i]
                    for i, colno in enumerate(inputs)
                    if not math.isnan(row[n + i])
                }
                try:
                    cgpm.incorporate(cgpm_rowid, query, evidence)
                except Exception:
                    pass
            return cgpm
        return initialize

    def _cache(self, bdb):
        # If there's no BayesDB-wide cache, there's no CGPM cache.
        if bdb.cache is None:
            return None

        # If there already is a CGPM cache, return it.  Otherwise
        # create a new one and cache it.
        if 'cgpm' in bdb.cache:
            return bdb.cache['cgpm']
        else:
            cache = CGPM_Cache()
            bdb.cache['cgpm'] = cache
            return cache

    def _cache_nocreate(self, bdb):
        # If there's no BayesDB-wide cache, there's no CGPM cache.
        if bdb.cache is None:
            return None

        # If there's no CGPM cache already, tough.
        if 'cgpm' not in bdb.cache:
            return None

        # Otherwise, get it.
        return bdb.cache['cgpm']

    def _schema(self, bdb, generator_id):
        # Probe the cache.
        cache = self._cache(bdb)
        if cache is not None:
            if generator_id in cache.schema:
                return cache.schema[generator_id]

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

        # Cache it, if we can.
        if cache is not None:
            cache.schema[generator_id] = schema
        return schema

    def _engine(self, bdb, generator_id):
        # Probe the cache.
        cache = self._cache(bdb)
        if cache is not None and generator_id in cache.engine:
            return cache.engine[generator_id]

        # Not cached.  Load the engine from the database.
        cursor = bdb.sql_execute('''
            SELECT engine_json FROM bayesdb_cgpm_generator
                WHERE generator_id = ?
        ''', (generator_id,))
        engine_json = cursor_value(cursor)
        if engine_json is None:
            generator = core.bayesdb_generator_name(bdb, generator_id)
            raise BQLError(bdb, 'No models initialized for generator: %r' %
                (generator,))

        # Deserialize the engine.
        engine = Engine.from_metadata(json.loads(engine_json), rng=bdb.np_prng)

        # Cache it, if we can.
        if cache is not None:
            cache.engine[generator_id] = engine
        return engine

    def _cgpm_rowid(self, bdb, generator_id, table_rowid):
        cursor = bdb.sql_execute('''
            SELECT cgpm_rowid FROM bayesdb_cgpm_individual
                WHERE generator_id = ? AND table_rowid = ?
        ''', (generator_id, table_rowid))
        cgpm_rowid = cursor_value(cursor, nullok=True)
        return cgpm_rowid if cgpm_rowid is not None else -1

    def _cgpm_value(self, bdb, generator_id, colno, value):
        if value is None:
            return float('NaN')
        stattype = core.bayesdb_generator_column_stattype(
            bdb, generator_id, colno)
        if casefold(stattype) == 'categorical':
            cursor = bdb.sql_execute('''
                SELECT code FROM bayesdb_cgpm_category
                    WHERE generator_id = ? AND colno = ? AND value = ?
            ''', (generator_id, colno, value))
            code = cursor_value(cursor, nullok=True)
            if code is None:
                raise BQLError('Invalid category: %r' % (value,))
            return code
        return value

class CGPM_Cache(object):
    def __init__(self):
        self.schema = {}
        self.engine = {}

def _create_schema(bdb, generator_id, schema_ast):
    # Get some parameters.
    population_id = core.bayesdb_generator_population(bdb, generator_id)
    table = core.bayesdb_population_table(bdb, population_id)
    qt = sqlite3_quote_name(table)

    # State.
    variables = []
    categoricals = {}
    cgpm_composition = []
    deferred_dist = {}
    modelled = set()

    # Error-reporting state.
    duplicate = set()
    unknown = set()
    needed = set()
    invalid_latent = set()

    # Process each clause one by one.
    for clause in schema_ast:

        if isinstance(clause, cgpm_parse.Basic):
            # Basic Crosscat component model: one variable to be put
            # into Crosscat views.
            var = clause.var
            dist = clause.dist
            params = dict(clause.params) # XXX error checking

            # Reject if the variable does not exist.
            if not core.bayesdb_has_variable(bdb, population_id, var):
                unknown.add(var)
                continue

            # Reject if the variable has already been modelled.
            if var in modelled:
                duplicate.add(var)
                continue

            # Reject if it is a latent variable.
            colno = core.bayesdb_variable_number(bdb, population_id, var)
            if colno < 0:
                invalid_latent.add(var)
                continue

            # Add it to the list and mark it modelled.
            stattype = core.bayesdb_variable_stattype(
                bdb, population_id, colno)
            variables.append([var, stattype, dist, params])
            modelled.add(var)

        elif isinstance(clause, cgpm_parse.Foreign):
            # Foreign model: some set of output variables is to be
            # modelled by foreign logic, possibly conditional on some
            # set of input variables.
            #
            # Gather up the state for a cgpm_composition record, which
            # we may have to do incrementally because it must refer to
            # the distribution types of variables we may not have
            # seen.
            name = clause.name
            outputs = clause.outputs
            inputs = clause.inputs
            cctypes = []
            ccargs = []
            distargs = {'cctypes': cctypes, 'ccargs': ccargs}
            kwds = {'distargs': distargs}

            # First make sure all the output variables exist and have
            # not yet been modelled.
            for var in clause.outputs:
                if not core.bayesdb_has_variable(bdb, population_id, var):
                    unknown.add(var)
                    continue
                if var in modelled:
                    duplicate.add(var)
                    break
                modelled.add(var)
                # XXX check agreement with statistical type
            else:
                # Next make sure all the input variables exist, mark
                # them needed, and record where to put their
                # distribution type and parameters.
                for var in inputs:
                    if not core.bayesdb_has_variable(bdb, population_id, var):
                        unknown.add(var)
                        continue
                    needed.add(var)
                    # XXX check agreement with statistical type
                    n = len(cctypes)
                    assert n == len(ccargs)
                    cctypes.append(None)
                    ccargs.append(None)
                    assert var not in deferred_dist
                    deferred_dist[var] = (cctypes, ccargs, n)
                else:
                    # Finally, add a cgpm_composition record.
                    cgpm_composition.append({
                        'name': name,
                        'inputs': inputs,
                        'outputs': outputs,
                        'kwds': kwds,
                    })
        else:
            assert False

    # Raise an exception if there were duplicates or unknown
    # variables.
    if duplicate:
        raise BQLError(bdb, 'Duplicate model variables: %r' %
            (sorted(duplicate),))
    if unknown:
        raise BQLError(bdb, 'Unknown model variables: %r' %
            (sorted(unknown),))
    if invalid_latent:
        raise BQLError(bdb, 'Invalid latent variables: %r' %
            (sorted(invalid_latent),))

    # Use the default distribution for any variables that remain to be
    # modelled, excluding any that are latent or that have statistical
    # types we don't know about.
    for var in core.bayesdb_variable_names(bdb, population_id):
        if var in modelled:
            continue
        colno = core.bayesdb_variable_number(bdb, population_id, var)
        if colno < 0:
            continue
        stattype = core.bayesdb_variable_stattype(bdb, population_id, colno)
        if stattype not in _DEFAULT_DIST:
            continue
        dist, params = _DEFAULT_DIST[stattype](bdb, generator_id, var)
        variables.append([var, stattype, dist, params])
        modelled.add(var)

    # If there remain any variables that we needed to model, because
    # others are conditional on them, fail.
    needed -= modelled
    if needed:
        raise BQLError(bdb, 'Unmodellable variables: %r' % (needed,))

    # Assign the deferred distribution types and parameters.  All
    # should be accounted for by now -- otherwise one of the previous
    # exceptions should have been raised.
    for var, stattype, dist, params in variables:
        if var in deferred_dist:
            cctypes, ccargs, i = deferred_dist[var]
            cctypes[i] = dist
            ccargs[i] = params
            del deferred_dist[var]
    assert not deferred_dist

    # Finally, create a CGPM schema.
    return {
        'variables': variables,
        'cgpm_composition': cgpm_composition,
    }

def _default_categorical(bdb, generator_id, var):
    table = core.bayesdb_generator_table(bdb, generator_id)
    qt = sqlite3_quote_name(table)
    qv = sqlite3_quote_name(var)
    cursor = bdb.sql_execute('SELECT COUNT(DISTINCT %s) FROM %s' % (qv, qt))
    k = cursor_value(cursor)
    return 'categorical', {'k': k}

def _default_numerical(bdb, generator_id, var):
    return 'normal', {}

_DEFAULT_DIST = {
    'categorical': _default_categorical,
    'numerical': _default_numerical,
}

# XXX Move these utilities elsewhere.

def bayesdb_metamodel_version(bdb, mm_name):
    cursor = bdb.sql_execute('''
        SELECT version FROM bayesdb_metamodel WHERE name = ?
    ''', (mm_name,))
    return cursor_value(cursor, nullok=True)

def cursor_row(cursor, nullok=None):
    if nullok is None:
        nullok = False
    try:
        row = cursor.next()
    except StopIteration:
        if nullok:
            return None
        raise ValueError('Empty cursor')
    else:
        try:
            cursor.next()
        except StopIteration:
            pass
        else:
            raise ValueError('Multiple-result cursor')
        return row

def cursor_value(cursor, nullok=None):
    row = cursor_row(cursor, nullok)
    if row is None:
        assert nullok
        return None
    if len(row) != 1:
        raise ValueError('Non-unit cursor')
    return row[0]

def json_dumps(obj):
    return json.dumps(obj, sort_keys=True)
