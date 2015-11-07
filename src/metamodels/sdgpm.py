# -*- coding: utf-8 -*-

#   Copyright (c) 2015, MIT Probabilistic Computing Project
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

import sqlite3

import bayeslite.core as core
import venture.value.dicts as v
import venture.shortcuts as s

from bayeslite.exception import BQLError
from bayeslite.util import casefold
from venture.ripl.ripl import Ripl

from bayeslite.sqlite3_util import sqlite3_quote_name

sdgpm_schema_1 = [
'''
INSERT INTO bayesdb_metamodel (name, version) VALUES ('sdgpm', 1);
''','''
CREATE TABLE bayesdb_sdgpm_program (
	generator_id    INTEGER NOT NULL PRIMARY KEY
        REFERENCES bayesdb_generator(id),
	program    TEXT NOT NULL
);
''','''
CREATE TABLE bayesdb_sdgpm_ripl (
	generator_id	INTEGER NOT NULL
        REFERENCES bayesdb_generator(id),
	modelno    INTEGER NOT NULL,
	ripl_binary	BLOB NOT NULL,
	PRIMARY KEY(generator_id, modelno),
	FOREIGN KEY(generator_id, modelno)
		REFERENCES bayesdb_generator_model(generator_id, modelno)
);
''']

class SdGpm(object):
    def __init__(self):
        self.hack_in_memory_cache = {}

    def name(self):
        return 'sdgpm'

    def register(self, bdb):
        with bdb.savepoint():
            schema_sql = 'SELECT version FROM bayesdb_metamodel WHERE name = ?'
            cursor = bdb.sql_execute(schema_sql, (self.name(),))
            version = None
            try:
                row = cursor.next()
            except StopIteration:
                version = 0
            else:
                version = row[0]
            assert version is not None
            if version == 0:
                # XXX WHATTAKLUDGE!
                for stmt in sdgpm_schema_1:
                    bdb.sql_execute(stmt)
                version = 1
            if version != 1:
                raise BQLError(bdb, 'sdgpm already installed with unknown'
                    ' schema version: %d' % (version,))

    def create_generator(self, bdb, _table, schema, instantiate):
        (program, columns) = self._parse_schema(bdb, schema)
        (generator_id, _column_list) = instantiate(columns)
        with bdb.savepoint():
            insert_program_sql = '''
                INSERT INTO bayesdb_sdgpm_program
                    (generator_id, program)
                    VALUES (?, ?)
            '''
            bdb.sql_execute(insert_program_sql, (generator_id, program))

    def initialize_models(self, bdb, generator_id, modelnos, _model_config):
        program = self._program(bdb, generator_id)
        # data = self._data(bdb, generator_id)
        for modelno in modelnos:
            ripl = s.make_lite_church_prime_ripl()
            ripl.execute_program(program)
            ripl.execute_program('''
                (assume get_cell (lambda (col rowid)
                    ((lookup (list u x) col) rowid)))''')
            # Note: This is not ripl.observe_dataset because I want to
            # give the inference program a chance to do something
            # between each row, if it wants.
            # for row in data:
            #     ripl.infer(v.app(v.sym("datum"), v.quote(row)))
            self._save_ripl(bdb, generator_id, modelno, ripl)

    def analyze_models(self, bdb, generator_id, modelnos=None, iterations=1,
            max_seconds=None, ckpt_iterations=None, ckpt_seconds=None):
        return

    def simulate_joint(self, bdb, generator_id, targets, constraints, modelno,
            num_predictions=1):
        constraints = self._remap_constraints(bdb, generator_id, constraints)
        targets = self._remap_targets(bdb, generator_id, targets)
        n_model = len(core.bayesdb_generator_modelnos(bdb, generator_id))
        results = [[] for _ in xrange(num_predictions)]
        for k in range(num_predictions):
            m = bdb.np_prng.randint(0, high=n_model)
            ripl = self._hack_ripl(bdb, generator_id, m)
            for (rowid, col, val) in constraints:
                ripl.observe('(get_cell %i %i)' % (col, rowid), val)
            for rowid in set([r for (r,c,v) in constraints]):
                ripl.infer('(mh (quote rlat) %i 100)' % rowid)
            for (rowid, col) in targets:
                rc_sample = ripl.predict(
                    '(get_cell %i %i)' % (col, rowid))
                ripl.infer('(mh (quote rlat) %i 100)' % rowid)
                results[k].append(rc_sample)
        return results

    def _parse_schema(self, bdb, schema):
        program = None
        columns = []
        for directive in schema:
            if isinstance(directive, list) and \
                    len(directive) == 2 and \
                    isinstance(directive[0], (str, unicode)) and \
                    casefold(directive[0]) == 'program':
                program = directive[1][0]
                continue
            if isinstance(directive, list) and \
                    len(directive) == 2 and \
                    isinstance(directive[0], (str, unicode)) and \
                    casefold(directive[0]) == 'columns' and \
                    isinstance(directive[1], (list)):
                L = [x for x in directive[1] if x != ',']
                columns.extend(zip(L[::2],L[1::2]))
                continue
            if directive == []:
                continue
            raise BQLError(bdb, 'Invalid sdgpm directive: %s.' %
                (repr(directive),))
        if program is None:
            raise BQLError(bdb, 'Cannot initialize sdgpm metamodel, '
                'no PROGRAM given.')
        return (program, columns)

    def _program(self, bdb, generator_id):
        sql = '''
            SELECT program FROM bayesdb_sdgpm_program
                WHERE generator_id = ?
        '''
        cursor = bdb.sql_execute(sql, (generator_id,))
        try:
            row = cursor.next()
        except StopIteration:
            generator = core.bayesdb_generator_name(bdb, generator_id)
            raise BQLError(bdb, 'No sdgpm program for generator: %s.' %
                (generator,))
        else:
            program = row[0]
            return program

    def _data(self, bdb, generator_id):
        # TODO This is almost identical with
        # crosscat.py:_crosscat_data, except for subsampling and value
        # coding.
        table_name = core.bayesdb_generator_table(bdb, generator_id)
        qt = sqlite3_quote_name(table_name)
        columns_sql = '''
            SELECT c.name, c.colno
                FROM bayesdb_column AS c,
                    bayesdb_generator AS g,
                    bayesdb_generator_column AS gc
                WHERE g.id = ?
                    AND c.tabname = g.tabname
                    AND c.colno = gc.colno
                    AND gc.generator_id = g.id
                ORDER BY c.colno ASC
        '''
        columns = list(bdb.sql_execute(columns_sql, (generator_id,)))
        colnames = [name for name, _colno in columns]
        qcns = map(sqlite3_quote_name, colnames)
        cursor = bdb.sql_execute('''
            SELECT %s FROM %s AS t
        ''' % (','.join('t.%s' % (qcn,) for qcn in qcns), qt))
        return [row for row in cursor]

    def _save_ripl(self, bdb, generator_id, model_no, ripl):
        ripl_binary = ripl.saves()
        insert_ripl_sql = '''
            INSERT INTO bayesdb_sdgpm_ripl
                (generator_id, modelno, ripl_binary)
                VALUES (:generator_id, :modelno, :ripl_binary)
        '''
        bdb.sql_execute(insert_ripl_sql, {
            'generator_id': generator_id,
            'modelno': model_no,
            'ripl_binary': sqlite3.Binary(ripl_binary),
        })
        self._cache_for(bdb, ('ripls', generator_id, model_no)).set(ripl)

    def _ripl(self, bdb, generator_id, model_no):
        box = self._cache_for(bdb, ('ripls', generator_id, model_no))
        return box.get() or box.set(self._load_ripl(bdb, generator_id,
            model_no))

    def _hack_ripl(self, bdb, generator_id, model_no):
        hack_ripl_binary = self._ripl(bdb, generator_id, model_no).saves()
        hack_ripl = s.make_lite_church_prime_ripl()
        hack_ripl.loads(hack_ripl_binary)
        return hack_ripl

    def _cache(self, bdb):
        return self.hack_in_memory_cache
        if bdb.cache is None:
            return None
        if 'sdgpm' in bdb.cache:
            return bdb.cache['sdgpm']
        else:
            _cache = {}
            bdb.cache['sdgpm'] = _cache
            return _cache

    def _cache_for(self, bdb, key):
        cache = self._cache(bdb)
        if cache is None:
            return NullBox()
        else:
            return HashBox(cache, key)

    def _load_ripl(self, bdb, generator_id, model_no):
        sql = '''
            SELECT ripl_binary FROM bayesdb_sdgpm_ripl
                WHERE generator_id = ? AND modelno = ?
        '''
        cursor = bdb.sql_execute(sql, (generator_id, model_no))
        try:
            row = cursor.next()
        except StopIteration:
            generator = core.bayesdb_generator_name(bdb, generator_id)
            raise BQLError(bdb, 'No sdgpm ripl for generator: %s model: %s.' %
                (generator, model_no))
        else:
            ripl = s.make_lite_church_prime_ripl()
            ripl_binary = row[0]
            ripl.loads(ripl_binary)
            return ripl

    def _column_map(self, bdb, generator_id):
        table_cols = core.bayesdb_generator_column_numbers(bdb, generator_id)
        return {c:i for (i,c) in enumerate(table_cols)}

    def _remap_constraints(self, bdb, generator_id, constraints):
        column_map = self._column_map(bdb, generator_id)
        return [(row, column_map[col], val) for (row, col, val) in constraints]

    def _remap_targets(self, bdb, generator_id, targets):
        column_map = self._column_map(bdb, generator_id)
        return [(row, column_map[col]) for (row, col) in targets]

class NullBox(object):
    def get(self):
        return None
    def set(self, value):
        return value

class HashBox(object):
    def __init__(self, cache, key):
        self.cache = cache
        self.key = key

    def get(self):
        if self.key in self.cache:
            return self.cache[self.key]
        else:
            return None

    def set(self, value):
        self.cache[self.key] = value
        return value
