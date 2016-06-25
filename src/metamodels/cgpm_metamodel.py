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
# [DONE] Fix necessary XXX: fill bayesdb_cgpm_variable.
# [DONE] Teach analysis to update CGPMs too.
# [DONE] Kludge up Kepler's laws in Python.
# [DONE] Test hand-kludged CGPM registry.
# - Write stupid schema parser.  Adapt axch's?
# - Teach INITIALIZE MODELS to take a model_config argument for real:
#   . [DONE] INITIALIZE 10 MODELS FOR <population> (x POISSON, y ...)
#   . rename model_config -> model schema
#   . Meaning: prior and likelihood model?  Or, `just SP'...
# - Introduce named model schemas:
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

from collections import namedtuple

from cgpm.crosscat.state import State

import bayeslite.core as core

from bayeslite.exception import BQLError
from bayeslite.metamodel import IBayesDBMetamodel
from bayeslite.sqlite3_util import sqlite3_quote_name
from bayeslite.util import casefold

CGPM_SCHEMA_1 = '''
INSERT INTO bayesdb_metamodel (name, version) VALUES ('cgpm', 1);

CREATE TABLE bayesdb_cgpm_category (
    generator_id        INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    colno               INTEGER NOT NULL CHECK (0 <= colno),
    value               TEXT NOT NULL,
    code                INTEGER NOT NULL,
    PRIMARY KEY(generator_id, colno, value),
    UNIQUE(generator_id, colno, code)
);

CREATE TABLE bayesdb_cgpm_variable (
    generator_id        INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    colno               INTEGER NOT NULL CHECK (0 <= colno),
    cgpm_colno          INTEGER NOT NULL CHECK (0 <= cgpm_colno),
    PRIMARY KEY(generator_id, colno),
    FOREIGN KEY(generator_id, colno)
        REFERENCES bayesdb_generator_column(generator_id, colno),
    UNIQUE(generator_id, cgpm_colno)
);

CREATE TABLE bayesdb_cgpm_model (
    generator_id        INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    modelno             INTEGER NOT NULL,
    state_json          BLOB NOT NULL,
    PRIMARY KEY(generator_id, modelno),
    FOREIGN KEY(generator_id, modelno)
        REFERENCES bayesdb_generator_model(generator_id, modelno)
);
'''

@contextlib.contextmanager
def engine_states(engine, states):
    # XXX Whattakludge!
    ostates = engine.states
    engine.states = [state.to_metadata() for state in states]
    try:
        yield
    finally:
        engine.states = ostates

@contextlib.contextmanager
def engine_X(engine, X):
    oX = engine.X
    engine.X = X
    try:
        yield
    finally:
        engine.X = oX

class CGPM_Metamodel(IBayesDBMetamodel):
    def __init__(self, engine, cgpm_registry):
        self._engine = engine
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

    def create_generator(self, bdb, table, schema, instantiate):
        # Instantiate the generator.  No interesting schema parsing
        # here -- we just pass along the statistical types verbatim
        # specified by the user.
        generator_id, column_list = instantiate(schema)

        # Assign codes to categories.
        qt = sqlite3_quote_name(table)
        for colno, name, stattype in column_list:
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

        # Assign consecutive column numbers to the modelled variables.
        for cgpm_colno, (colno, _name, _stattype) in enumerate(column_list):
            bdb.sql_execute('''
                INSERT INTO bayesdb_cgpm_variable
                    (generator_id, colno, cgpm_colno)
                    VALUES (?, ?, ?)
            ''', (generator_id, colno, cgpm_colno))

    def drop_generator(self, bdb, generator_id):
        # Flush any cached schema or models.
        cache = self._cache_nocreate(bdb)
        if cache is not None:
            if generator_id in cache.schema:
                del cache.schema[generator_id]
            if generator_id in cache.model:
                del cache.model[generator_id]

        # Delete models.
        bdb.sql_execute('''
            DELETE FROM bayesdb_cgpm_model WHERE generator_id = ?
        ''', (generator_id,))

        # Delete variables.
        bdb.sql_execute('''
            DELETE FROM bayesdb_cgpm_variable WHERE generator_id = ?
        ''', (generator_id,))

        # Delete categories.
        bdb.sql_execute('''
            DELETE FROM bayesdb_cgpm_category WHERE generator_id = ?
        ''', (generator_id,))

    def initialize_models(self, bdb, generator_id, modelnos, schema_tokens):
        # Caller should guarantee a nondegenerate request.
        assert 0 < len(modelnos)

        # Get the cache.  It had better not have any models yet.
        cache = self._cache(bdb)
        if cache is not None and generator_id in cache.models:
            assert not any(modelno in cache.models[generator_id]
                for modelno in modelnos)

        # If the caller didn't provide a schema for these models, get
        # the generator-wide one.
        if schema_tokens is None:
            raise BQLError(bdb, 'Missing CGPM model schema')
        else:
            # XXX MEGA-KLUDGE!
            def jsonify(x):
                if isinstance(x, int):
                    return str(x)
                elif x == '<':
                    return '{'
                elif x == '>':
                    return '}'
                elif x == '(':
                    return '['
                elif x == ')':
                    return ']'
                elif x == '~':
                    return ':'
                elif x == ',':
                    return ','
                else:
                    return '"' + x + '"'
            schema = json.loads(' '.join(map(jsonify, schema_tokens)))
            # XXX check schema

        # Initialize fresh states and their composed CGPMs.
        X = self._X(bdb, generator_id)
        states = self._initialize(len(modelnos), X, schema)
        for state in states:
            for cgpm_ext in schema['cgpm_composition']:
                cgpm = self._initialize_cgpm(bdb, cgpm_ext)
                _token = state.compose_cgpm(cgpm)

        # Make sure the cache, if available, is ready for models for
        # this generator.
        if cache is not None:
            if generator_id not in cache.models:
                cache.models[generator_id] = {}

        # Store the newly initialized states.
        for modelno, state in zip(modelnos, states):
            # Serialize the state.
            state_json = json_dumps(state.to_metadata())

            # Store it persistently in the database.
            bdb.sql_execute('''
                INSERT INTO bayesdb_cgpm_model
                    (generator_id, modelno, state_json)
                    VALUES (?, ?, ?)
            ''', (generator_id, modelno, state_json))

            # Store it in the cache, if available.
            if cache is not None:
                cache.models[generator_id][modelno] = state

    def drop_models(self, bdb, generator_id, modelnos=None):
        # Get the cache.
        cache = self._cache(bdb)

        # Are we dropping all models or only selected ones?
        if modelnos is None:
            # All models.
            if cache is not None:
                if generator_id in cache.models:
                    del cache.models[generator_id]
            bdb.sql_execute('''
                DELETE FROM bayesdb_cgpm_model WHERE generator_id = ?
            ''', (generator_id,))
        else:
            # Selected models.
            for modelno in modelnos:
                bdb.sql_execute('''
                    DELETE FROM bayesdb_cgpm_model
                        WHERE generator_id = ? AND modelno = ?
                ''', (generator_id, modelno))
                if cache is not None and generator_id in cache.models:
                    del cache.models[generator_id][modelno]
                    if len(cache.models[generator_id]) == 0:
                        del cache.models[generator_id]

    def analyze_models(self, bdb, generator_id, modelnos=None, iterations=1,
            max_seconds=None, ckpt_iterations=None, ckpt_seconds=None):
        if ckpt_iterations is not None or ckpt_seconds is not None:
            # XXX
            raise NotImplementedError('cgpm analysis checkpoint')

        with self._engine_states(bdb, generator_id, modelnos):
            with self._engine_data(bdb, generator_id):
                # Do the transition.
                self._engine.transition(
                    N=iterations, S=max_seconds, multithread=10)

                # Get the cache and make sure if it is available it is
                # ready for models in this generator.
                cache = self._cache(bdb)
                if cache is not None:
                    if generator_id not in cache.models:
                        cache.models[generator_id] = {}

                # Enumerate the states -- consecutively numbered, if the
                # caller didn't specify which ones.
                if modelnos is None:
                    modelnos = xrange(len(self._engine.states))
                for modelno, state in zip(modelnos, self._engine.states):
                    # Get the actual state, until we persuade engines
                    # to store states and not serialized states.
                    state = State.from_metadata(state, rng=bdb.np_prng)

                    # Serialize the state.
                    state_json = json_dumps(state.to_metadata())

                    # Store it persistently in the database.
                    bdb.sql_execute('''
                        UPDATE bayesdb_cgpm_model
                            SET state_json = :state_json
                            WHERE generator_id = :generator_id
                                AND modelno = :modelno
                    ''', {
                        'generator_id': generator_id,
                        'modelno': modelno,
                        'state_json': state_json,
                    })

                    # Store it in the cache, if available.
                    if cache is not None:
                        cache.models[generator_id][modelno] = state

    def column_dependence_probability(self, bdb, generator_id, modelno,
            colno0, colno1):
        # Special-case vacuous case of self-dependence.  XXX Caller
        # should avoid this.
        if colno0 == colno1:
            return 1

        # Prepare the engine with the requested states.
        modelnos = None if modelno is None else [modelno]
        with self._engine_states(bdb, generator_id, modelnos):
            # Map the variable indexing.
            cgpm_colno0 = self._cgpm_colno(bdb, generator_id, colno0)
            cgpm_colno1 = self._cgpm_colno(bdb, generator_id, colno1)

            # Go!
            return self._engine.dependence_probability(
                cgpm_colno0, cgpm_colno1)

    def column_mutual_information(self, bdb, generator_id, modelno,
            colno0, colno1, numsamples=None):
        # XXX Default number of samples drawn from my arse.
        if numsamples is None:
            numsamples = 1000

        # Prepare the engine with the requested states.
        modelnos = None if modelno is None else [modelno]
        with self._engine_states(bdb, generator_id, modelnos):
            # Map the variable indexing.
            cgpm_colno0 = self._cgpm_colno(bdb, generator_id, colno0)
            cgpm_colno1 = self._cgpm_colno(bdb, generator_id, colno1)

            # Go!
            mi_list = self._engine.mutual_information(
                cgpm_colno0, cgpm_colno1, N=numsamples)

            # Engine gives us a list of samples which it is our
            # responsibility to integrate over.
            #
            # XXX Is this integral correct?  Should it be weighted?
            return arithmetic_mean(mi_list)

    def row_similarity(self, bdb, generator_id, modelno, rowid, target_rowid,
            colnos):
        # Prepare the engine with the requested states.
        modelnos = None if modelno is None else [modelno]
        with self._engine_states(bdb, generator_id, modelnos):
            # Map the variable and individual indexing.
            cgpm_rowid = self._cgpm_rowid(bdb, generator_id, rowid)
            cgpm_target_rowid = self._cgpm_rowid(bdb, generator_id,
                target_rowid)
            cgpm_colnos = [self._cgpm_colno(bdb, generator_id, colno)
                for colno in colnos]

            # Go!
            return self._engine.row_similarity(
                cgpm_rowid, cgpm_target_rowid, cgpm_colnos)

    def simulate_joint(self, bdb, generator_id, targets, constraints, modelno,
            numpredictions=None):
        # XXX
        raise NotImplementedError('cgpm simulate')
        if numpredictions is None:
            numpredictions = 1
        rowid = self._unique_rowid(targets, constraints)
        cgpm_rowid = self._cgpm_rowid(bdb, generator_id, rowid)
        cgpm_colnos = [self._cgpm_colno(bdb, generator_id, colno)
            for _, colno, _ in targets]
        modelnos = None if modelno is None else [modelno]
        with self._engine_states(bdb, generator_id, modelnos):
            samples = self._engine.simulate(rowid, colnos, whatever)

    def logpdf_joint(self, bdb, generator_id, targets, constraints, modelno):
        # XXX
        raise NotImplementedError('cgpm logpdf')

    @contextlib.contextmanager
    def _engine_states(self, bdb, generator_id, modelnos):
        # Load the states from the database.
        states = self._models(bdb, generator_id, modelnos)

        # Prepare the engine with these states.
        with engine_states(self._engine, states):
            yield

    @contextlib.contextmanager
    def _engine_data(self, bdb, generator_id):
        # Load the mapped data from the database.
        X = self._X(bdb, generator_id)

        # Prepare the engine with these data.
        with engine_X(self._engine, X):
            yield

    def _X(self, bdb, generator_id):
        return self._data(bdb, generator_id)
        # data = self._data(bdb, generator_id)
        # colnos = core.bayesdb_generator_column_numbers(bdb, generator_id)
        # return [[data[rowid][colno] for rowid in xrange(len(data))]
        #     for colno in xrange(len(colnos))]

    def _data(self, bdb, generator_id):
        # Get the table name, quoted for constructing SQL.
        table_name = core.bayesdb_generator_table(bdb, generator_id)
        qt = sqlite3_quote_name(table_name)

        # Get all the columns of interest and their statistical types.
        columns_sql = '''
            SELECT c.name, c.colno, gc.stattype
                FROM bayesdb_column AS c,
                    bayesdb_generator AS g,
                    bayesdb_generator_column AS gc
                WHERE g.id = ?
                    AND c.tabname = g.tabname
                    AND c.colno = gc.colno
                    AND gc.generator_id = g.id
                ORDER BY c.colno ASC
        '''
        columns = bdb.sql_execute(columns_sql, (generator_id,)).fetchall()

        # Create SQL expressions to cast each variable to the correct
        # affinity for its statistical type.
        qexpressions = ','.join('CAST(t.%s AS %s)' %
                (sqlite3_quote_name(name),
                    sqlite3_quote_name(core.bayesdb_stattype_affinity(bdb,
                            stattype)))
            for name, _colno, stattype in columns)

        # Get the data.
        #
        # XXX Subsampling?
        cursor = bdb.sql_execute('''
            SELECT %s FROM %s AS t
        ''' % (qexpressions, qt))

        # Map values to codes.
        colnos = core.bayesdb_generator_column_numbers(bdb, generator_id)
        def map(colno, value):
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
        return [tuple(map(colno, x) for colno, x in zip(colnos, row))
            for row in cursor]

    def _initialize(self, nstates, X, schema):
        # XXX Parallelize me!  Push me into the engine!
        variables = schema['variables']
        outputs = range(len(variables))
        cctypes = [cctype for _n, _st, cctype, _da in variables]
        distargs = [distargs for _n, _st, _cct, distargs in variables]
        return [State(X, outputs, cctypes=cctypes, distargs=distargs)
            for _ in xrange(nstates)]

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

    def _models(self, bdb, generator_id, modelno):
        # Get the model numbers.
        modelnos = [modelno] if modelno is not None else \
            core.bayesdb_generator_modelnos(bdb, generator_id)

        # Get the model for each model number.
        return [self._model(bdb, generator_id, modelno)
            for modelno in modelnos]

    def _model(self, bdb, generator_id, modelno):
        # Probe the cache.
        cache = self._cache(bdb)
        if cache is not None and \
           generator_id in cache.models and \
           modelno in cache.models[generator_id]:
            return cache.models[generator_id][modelno]

        # Not cached.  Load the model from the database.
        cursor = bdb.sql_execute('''
            SELECT state_json FROM bayesdb_cgpm_model
                WHERE generator_id = ? AND modelno = ?
        ''', (generator_id, modelno))
        state_json = cursor_value(cursor, nullok=True)
        if state_json is None:
            generator = core.bayesdb_generator_name(bdb, generator_id)
            raise BQLError(bdb, 'No such model for generator %s: %d' %
                (repr(generator), modelno))

        # Deserialize the state.
        state = State.from_metadata(json.loads(state_json), rng=bdb.np_prng)

        # Cache it, if available.
        if cache is not None:
            if generator_id not in cache.models:
                cache.models[generator_id] = {}
            cache.models[generator_id][modelno] = state
        return state

    def _initialize_cgpm(self, bdb, cgpm_ext):
        name = cgpm_ext['name']
        inputs = cgpm_ext['inputs']
        outputs = cgpm_ext['outputs']
        args = cgpm_ext.get('args', ())
        kwds = cgpm_ext.get('kwds', {})
        if name not in self._cgpm_registry:
            raise BQLError(bdb, 'Unknown CGPM: %s' % (repr(name),))
        cls = self._cgpm_registry[name]
        return cls(inputs, outputs, rng=bdb.np_prng, *args, **kwds)

    # XXX subsampling
    def _cgpm_rowid(self, bdb, generator_id, rowid):
        return rowid

    def _cgpm_colno(self, bdb, generator_id, colno):
        cursor = bdb.sql_execute('''
            SELECT cgpm_colno FROM bayesdb_cgpm_variable
                WHERE generator_id = ? AND colno = ?
        ''', (generator_id, colno))
        # XXX error message if not modelled
        return cursor_value(cursor)

class CGPM_Cache(object):
    def __init__(self):
        self.schema = {}
        self.models = {}

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
