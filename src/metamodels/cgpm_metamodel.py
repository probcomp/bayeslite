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

from collections import Counter
from collections import defaultdict

from cgpm.crosscat.engine import Engine

import bayeslite.core as core

from bayeslite.exception import BQLError
from bayeslite.metamodel import IBayesDBMetamodel
from bayeslite.metamodel import bayesdb_metamodel_version
from bayeslite.sqlite3_util import sqlite3_quote_name
from bayeslite.stats import arithmetic_mean
from bayeslite.util import casefold
from bayeslite.util import cursor_value
from bayeslite.util import json_dumps

import cgpm_schema.parse
import cgpm_analyze.parse

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
    def __init__(self, cgpm_registry, multiprocess=None):
        self._cgpm_registry = cgpm_registry
        self._ncpu = multiprocess

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
        schema_ast = cgpm_schema.parse.parse(schema_tokens)
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

        # Assign latent variable numbers.
        for var, stattype in sorted(schema['latents'].iteritems()):
            core.bayesdb_add_latent(
                bdb, population_id, generator_id, var, stattype)

        # Assign codes to categories and consecutive column numbers to
        # the modelled variables.
        vars_cursor = bdb.sql_execute('''
            SELECT colno, name, stattype FROM bayesdb_variable
                WHERE population_id = ? AND 0 <= colno
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
            cgpms = [self._initialize_cgpm(bdb, generator_id, cgpm_ext)
                for _ in xrange(n)]
            engine.compose_cgpm(cgpms, multiprocess=self._ncpu)

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
            max_seconds=None, ckpt_iterations=None, ckpt_seconds=None,
            program=None):
        assert modelnos is None

        if ckpt_iterations is not None or ckpt_seconds is not None:
            # XXX
            raise NotImplementedError('CGpm analysis checkpoint not supported.')

        if program is None:
            program = []

        population_id = core.bayesdb_generator_population(bdb, generator_id)

        def retrieve_analyze_variables(ast):
            # Transition all variables by default.
            if len(ast) == 0:
                variables = core.bayesdb_variable_names(bdb, population_id,
                    generator_id)
            # Exactly 1 clause supported.
            elif len(ast) == 1:
                clause = ast[0]
                # Transition user specified variables only.
                if isinstance(clause, cgpm_analyze.parse.Variables):
                    included = set()
                    unknown = set()
                    for var in clause.vars:
                        if not core.bayesdb_has_variable(bdb, population_id,
                                generator_id, var):
                            unknown.add(var)
                        included.add(var)
                    if unknown:
                        raise BQLError(bdb, 'Unknown variables in ANALYZE: %r'
                            % (sorted(unknown),))
                    variables = sorted(included)
                # Transition all variables except user specified skip.
                elif isinstance(clause, cgpm_analyze.parse.Skip):
                    excluded = set()
                    unknown = set()
                    for var in clause.vars:
                        if not core.bayesdb_has_variable(bdb, population_id,
                                generator_id, var):
                            unknown.add(var)
                        excluded.add(var)
                    if unknown:
                        raise BQLError(bdb, 'Unknown variables in ANALYZE: %r'
                            % (sorted(unknown),))
                    all_vars = core.bayesdb_variable_names(
                        bdb, population_id, generator_id)
                    variables = sorted(set(all_vars) - excluded)
                # Unknown/impossible clause.
                else:
                    raise ValueError('Unknown clause in ANALYZE: %s.' % ast)
            # Crash if more than 1 clause.
            else:
                raise ValueError('1 clause permitted in ANALYZE: %s.' % ast)
            return variables

        # Retrieve target variables.
        analyze_ast = cgpm_analyze.parse.parse(program)
        variables = retrieve_analyze_variables(analyze_ast)
        varnos = [core.bayesdb_variable_number(bdb, population_id,
                generator_id, v)
            for v in variables]

        # Run transition.
        engine = self._engine(bdb, generator_id)
        engine.transition(
            N=iterations, S=max_seconds, cols=varnos, multiprocess=self._ncpu)

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
        return engine.dependence_probability(colno0, colno1,
            multiprocess=self._ncpu)

    def column_mutual_information(self, bdb, generator_id, modelno,
            colno0, colno1, numsamples=None):
        # XXX Default number of samples drawn from my arse.
        if numsamples is None:
            numsamples = 1000

        # Get the engine.
        engine = self._engine(bdb, generator_id)

        # Go!
        mi_list = engine.mutual_information(colno0, colno1, N=numsamples,
            multiprocess=self._ncpu)

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
        return engine.row_similarity(cgpm_rowid, cgpm_target_rowid, colnos,
            multiprocess=self._ncpu)

    def predict_confidence(self, bdb, generator_id, modelno, colno, rowid,
            numsamples=None):
        if not numsamples:
            numsamples = 2
        assert numsamples > 0
        def _impute_categorical(sample):
            counts = Counter(s[0] for s in sample)
            mode_count = max(counts[v] for v in counts)
            pred = iter(v for v in counts if counts[v] == mode_count).next()
            conf = float(mode_count) / numsamples
            return pred, conf
        def _impute_numerical(sample):
            pred = sum(s[0] for s in sample) / float(len(sample))
            conf = 0 # XXX Punt confidence for now
            return pred, conf
        sample = self.simulate_joint(
            bdb, generator_id, [(rowid, colno)], [], modelno, numsamples)
        stattype = core.bayesdb_variable_stattype(
            bdb, core.bayesdb_generator_population(bdb, generator_id), colno)
        if stattype == 'categorical': # XXX Right way to find the stattype?
            return _impute_categorical(sample)
        else:
            return _impute_numerical(sample)

    def simulate_joint(self, bdb, generator_id, targets, constraints, modelno,
            num_predictions=None):
        if num_predictions is None:
            num_predictions = 1
        rowid = self._unique_rowid(
            [r for r, _c in targets] + [r for r, _c, _v in constraints])
        cgpm_rowid = self._cgpm_rowid(bdb, generator_id, rowid)
        cgpm_query = [colno for _r, colno in targets]
        cgpm_evidence = {
            colno: self._to_numeric(bdb, generator_id, colno, value)
            for _r, colno, value in constraints
        }
        engine = self._engine(bdb, generator_id)
        samples = engine.simulate(
            cgpm_rowid, cgpm_query, cgpm_evidence, N=num_predictions,
            multiprocess=self._ncpu)
        weighted_samples = engine._process_samples(
            samples, cgpm_rowid, cgpm_evidence, multiprocess=self._ncpu)
        def map_value(colno, value):
            return self._from_numeric(bdb, generator_id, colno, value)
        return [[map_value(colno, row[colno]) for colno in cgpm_query]
            for row in weighted_samples]

    def logpdf_joint(self, bdb, generator_id, targets, constraints, modelno):
        rowid = self._unique_rowid(
            [r for r, _c, _v in targets + constraints])
        cgpm_rowid = self._cgpm_rowid(bdb, generator_id, rowid)
        cgpm_query = {
            colno: self._to_numeric(bdb, generator_id, colno, value)
            for _r, colno, value in targets
        }
        cgpm_evidence = {
            colno: self._to_numeric(bdb, generator_id, colno, value)
            for _r, colno, value in constraints
        }
        engine = self._engine(bdb, generator_id)
        logpdfs = engine.logpdf(
            cgpm_rowid, cgpm_query, cgpm_evidence, multiprocess=self._ncpu)
        # XXX abstraction violation
        return engine._process_logpdfs(
            logpdfs, cgpm_rowid, cgpm_evidence, multiprocess=self._ncpu)

    def _unique_rowid(self, rowids):
        if len(set(rowids)) != 1:
            raise ValueError('Multiple-row query: %r' % (list(set(rowids)),))
        return rowids[0]

    def _data(self, bdb, generator_id, vars):
        # Get the column numbers and statistical types.
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        colnos = [core.bayesdb_variable_number(bdb, population_id,
                generator_id, var)
            for var in vars]
        stattypes = [core.bayesdb_variable_stattype(bdb, population_id, colno)
            for colno in colnos]

        # Get the table name, quoted for constructing SQL.
        table_name = core.bayesdb_generator_table(bdb, generator_id)
        qt = sqlite3_quote_name(table_name)

        # Create SQL expressions to cast each variable to the correct
        # affinity for its statistical type.
        def cast(var, colno, stattype):
            if colno < 0:
                return 'NULL'
            qv = sqlite3_quote_name(var)
            affinity = core.bayesdb_stattype_affinity(bdb, stattype)
            qa = sqlite3_quote_name(affinity)
            return 'CAST(t.%s AS %s)' % (qv, qa)
        qexpressions = ','.join(map(cast, vars, colnos, stattypes))

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
        return [tuple(map_value(colno, x) for colno, x in zip(colnos, row))
            for row in cursor]

    def _initialize_engine(self, bdb, generator_id, n, variables):
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        def map_var(var):
            return core.bayesdb_variable_number(bdb, population_id,
                generator_id, var)
        outputs = [map_var(var) for var, _st, _cct, _da in variables]
        cctypes = [cctype for _n, _st, cctype, _da in variables]
        distargs = [distargs for _n, _st, _cct, distargs in variables]
        gpmcc_vars = [var for var, _stattype, _dist, _params in variables]
        gpmcc_data = self._data(bdb, generator_id, gpmcc_vars)
        return Engine(
            gpmcc_data, num_states=n, rng=bdb.np_prng, multiprocess=self._ncpu,
            outputs=outputs, cctypes=cctypes, distargs=distargs)

    def _initialize_cgpm(self, bdb, generator_id, cgpm_ext):
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        def map_var(var):
            return core.bayesdb_variable_number(bdb, population_id,
                generator_id, var)
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

    def _to_numeric(self, bdb, generator_id, colno, value):
        """Convert value in bayeslite to equivalent cgpm format."""
        if value is None:
            return float('NaN')
        # XXX Latent variables are not associated with an entry in
        # bayesdb_cgpm_category, so just pass through whatever value
        # the user supplied, as a float.
        if colno < 0:
            return float(value)
        stattype = core.bayesdb_generator_column_stattype(
            bdb, generator_id, colno)
        if casefold(stattype) == 'categorical':
            cursor = bdb.sql_execute('''
                SELECT code FROM bayesdb_cgpm_category
                    WHERE generator_id = ? AND colno = ? AND value = ?
            ''', (generator_id, colno, value))
            integer = cursor_value(cursor, nullok=True)
            if integer is None:
                raise BQLError('Invalid category: %r' % (value,))
            return integer
        else:
            return value

    def _from_numeric(self, bdb, generator_id, colno, value):
        """Convert value in cgpm to equivalent bayeslite format."""
        # XXX Latent variables are not associated with an entry in
        # bayesdb_cgpm_category, so just pass through whatever value cgpm
        # returns.
        if colno < 0:
            return value
        if math.isnan(value):
            return None
        stattype = core.bayesdb_generator_column_stattype(
            bdb, generator_id, colno)
        if casefold(stattype) == 'categorical':
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

class CGPM_Cache(object):
    def __init__(self):
        self.schema = {}
        self.engine = {}

def _create_schema(bdb, generator_id, schema_ast):
    # Get some parameters.
    population_id = core.bayesdb_generator_population(bdb, generator_id)
    table = core.bayesdb_population_table(bdb, population_id)

    # State.
    variables = []
    variable_dist = {}
    latents = {}
    cgpm_composition = []
    modelled = set()
    default_modelled = set()
    subsample = None
    deferred = defaultdict(lambda: [])

    # Error-reporting state.
    duplicate = set()
    unknown = set()
    needed = set()
    existing_latent = set()
    must_exist = []
    unknown_stattype = {}

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

            # Reject if the variable has already been modelled.
            if var in modelled:
                duplicate.add(var)
                continue

            # Reject if the variable is latent.
            if core.bayesdb_has_latent(bdb, population_id, var):
                existing_latent.add(var)
                continue

            # Get the column number.
            colno = core.bayesdb_variable_number(bdb, population_id, None, var)
            assert 0 <= colno

            # Add it to the list and mark it modelled by default.
            stattype = core.bayesdb_variable_stattype(
                bdb, population_id, colno)
            variables.append([var, stattype, dist, params])
            assert var not in variable_dist
            variable_dist[var] = (stattype, dist, params)
            modelled.add(var)
            default_modelled.add(var)

        elif isinstance(clause, cgpm_schema.parse.Latent):
            var = clause.name
            stattype = clause.stattype

            # Reject if the variable has already been modelled by the
            # default model.
            if var in default_modelled:
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

            # Add it to the set of latent variables.
            latents[var] = stattype

        elif isinstance(clause, cgpm_schema.parse.Foreign):
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
            kwds.update(clause.params)

            # First make sure all the output variables exist and have
            # not yet been modelled.
            for var in outputs:
                must_exist.append(var)
                if var in modelled:
                    duplicate.add(var)
                    continue
                modelled.add(var)

            # Next make sure all the input variables exist, mark them
            # needed, and record where to put their distribution type
            # and parameters.
            for var in inputs:
                must_exist.append(var)
                needed.add(var)
                i = len(cctypes)
                assert i == len(ccargs)
                cctypes.append(None)
                ccargs.append(None)
                deferred[var].append((cctypes, ccargs, i))

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
        raise BQLError(bdb, 'Duplicate model variables: %r' %
            (sorted(duplicate),))
    if unknown:
        raise BQLError(bdb, 'Unknown model variables: %r' %
            (sorted(unknown),))
    if existing_latent:
        raise BQLError(bdb, 'Latent variables already defined: %r' %
            (sorted(existing_latent),))

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
    # modelled, excluding any that are latent or that have statistical
    # types we don't know about.
    for var in core.bayesdb_variable_names(bdb, population_id, None):
        if var in modelled:
            continue
        colno = core.bayesdb_variable_number(bdb, population_id, None, var)
        assert 0 <= colno
        stattype = core.bayesdb_variable_stattype(bdb, population_id, colno)
        distparams = default_dist(var, stattype)
        if distparams is None:
            continue
        dist, params = distparams
        variables.append([var, stattype, dist, params])
        assert var not in variable_dist
        variable_dist[var] = (stattype, dist, params)
        modelled.add(var)

    # Fill in the deferred statistical type assignments.
    for var in sorted(deferred.iterkeys()):
        # Check whether the variable is modelled.  If not, skip -- we
        # will fail later because this variable is guaranteed to also
        # be in needed.
        if var not in modelled:
            assert var in needed
            continue

        # Determine (possibly fictitious) distribution and parameters.
        if var in default_modelled:
            # Manifest variable modelled by default Crosscat model.
            assert var in variable_dist
            stattype, dist, params = variable_dist[var]
        else:
            # Modelled by a foreign model.  Assign a fictitious
            # default distribution because the 27B/6 of CGPM requires
            # this.
            if var in latents:
                # Latent variable modelled by a foreign model.  Use
                # the statistical type specified for it.
                stattype = latents[var]
            else:
                # Manifest variable modelled by a foreign model.  Use
                # the statistical type in the population.
                assert core.bayesdb_has_variable(bdb, population_id, None, var)
                colno = core.bayesdb_variable_number(
                    bdb, population_id, None, var)
                stattype = core.bayesdb_variable_stattype(
                    bdb, population_id, colno)
            distparams = default_dist(var, stattype)
            if distparams is None:
                continue
            dist, params = distparams

        # Assign the distribution and parameters.
        for cctypes, ccargs, i in deferred[var]:
            assert cctypes[i] is None
            assert ccargs[i] is None
            cctypes[i] = dist
            ccargs[i] = params

    if unknown_stattype:
        raise BQLError(bdb, 'Unknown statistical types for variables: %r' %
            (sorted(unknown_stattype.iteritems(),)))

    # If there remain any variables that we needed to model, because
    # others are conditional on them, fail.
    needed -= modelled
    if needed:
        raise BQLError(bdb, 'Unmodellable variables: %r' % (needed,))

    # Finally, create a CGPM schema.
    return {
        'variables': variables,
        'cgpm_composition': cgpm_composition,
        'subsample': subsample,
        'latents': latents,
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
    'cyclic': _default_numerical, # XXX can't do cyclic yet
    'numerical': _default_numerical,
}
