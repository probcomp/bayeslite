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

import bayeslite.core as core
import venture.value.dicts as v
import venture.shortcuts as s

from bayeslite.exception import BQLError
from bayeslite.util import casefold
from venture.ripl.ripl import Ripl

from bayeslite.sqlite3_util import sqlite3_quote_name

vs_schema_1 = '''
INSERT INTO bayesdb_metamodel (name, version) VALUES ('venture_script', 1);

CREATE TABLE bayesdb_venture_script_program (
	generator_id	INTEGER NOT NULL PRIMARY KEY
				REFERENCES bayesdb_generator(id),
	program		TEXT NOT NULL
);

CREATE TABLE bayesdb_venture_script_ripl (
	generator_id	INTEGER NOT NULL REFERENCES bayesdb_generator(id),
	modelno		INTEGER NOT NULL,
	ripl_str	BLOB NOT NULL,
	PRIMARY KEY(generator_id, modelno),
	FOREIGN KEY(generator_id, modelno)
		REFERENCES bayesdb_generator_model(generator_id, modelno)
);
'''

class VSMetamodel(object): # TODO New metamodel
    def name(self):
        return 'venture_script'

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
                for stmt in vs_schema_1.split(';'):
                    bdb.sql_execute(stmt)
                version = 1
            if version != 1:
                raise BQLError(bdb, 'VentureScript already installed'
                    ' with unknown schema version: %d' % (version,))

    def _parse_schema(self, bdb, schema):
        program = None
        columns = []
        for directive in schema:
            if isinstance(directive, list) and \
               len(directive) == 2 and \
               isinstance(directive[0], (str, unicode)) and \
               casefold(directive[0]) == 'program':
                program = directive[1]
                continue
            if isinstance(directive, list) and \
               len(directive) == 2 and \
               isinstance(directive[0], (str, unicode)) and \
               casefold(directive[0]) != 'program' and \
               isinstance(directive[1], (str, unicode)):
                columns.append((directive[0], directive[1]))
                continue
            if directive == []:
                # Skip extra commas so you can write
                #
                #    CREATE GENERATOR t_cc FOR t USING venture_script(
                #        x,
                #        y,
                #        z,
                #    )
                continue
            raise BQLError(bdb, 'Invalid venture_script column model: %s' %
                (repr(directive),))
        if program is None:
            raise BQLError(bdb, 'Cannot initialize VentureScript metamodel, no PROGRAM given')
        return (program, columns)

    def _vs_program(self, bdb, generator_id):
        sql = '''
            SELECT program FROM bayesdb_venture_script_program
                WHERE generator_id = ?
        '''
        cursor = bdb.sql_execute(sql, (generator_id,))
        try:
            row = cursor.next()
        except StopIteration:
            generator = core.bayesdb_generator_name(bdb, generator_id)
            raise BQLError(bdb, 'No venture_script program for generator: %s' %
                (generator,))
        else:
            program = row[0]
            return program

    def _vs_data(self, bdb, generator_id):
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

    def _vs_ripl(self, bdb, generator_id, model_no):
        sql = '''
            SELECT ripl_str FROM bayesdb_venture_script_ripl
                WHERE generator_id = ? AND modelno = ?
        '''
        cursor = bdb.sql_execute(sql, (generator_id, model_no))
        try:
            row = cursor.next()
        except StopIteration:
            generator = core.bayesdb_generator_name(bdb, generator_id)
            raise BQLError(bdb, 'No venture_script ripl for generator: %s model: %s' %
                (generator, model_no))
        else:
            string_ = row[0]
            return Ripl.deserialize(string_)

    def create_generator(self, bdb, _table, schema, instantiate):
        (program, columns) = self._parse_schema(bdb, schema)
        (generator_id, _column_list) = instantiate(columns)
        with bdb.savepoint():
            insert_program_sql = '''
                INSERT INTO bayesdb_venture_script_program
                    (generator_id, program)
                    VALUES (?, ?)
            '''
            bdb.sql_execute(insert_program_sql, (generator_id, program))

    def initialize_models(self, bdb, generator_id, modelnos, _model_config):
        program = self._vs_program(bdb, generator_id)
        data = self._vs_data(bdb, generator_id)
        for modelno in modelnos:
            ripl = s.make_ripl()
            ripl.execute_program(program)
            # TODO Probably want to replace this with passing a
            # callable that will fetch streaming data from the DB,
            # except the question of caching the fetch across model
            # instantiations.
            # Note: This is not ripl.observe_dataset because I want to
            # give the inference program a chance to do something
            # between each row, if it wants.
            for row in data:
                ripl.infer(v.app(v.sym("datum"), v.quote(row)))
            string_ = ripl.serialize()
            insert_ripl_sql = '''
                INSERT INTO bayesdb_venture_script_ripl
                    (generator_id, modelno, ripl_str)
                    VALUES (:generator_id, :modelno, :ripl_str)
            '''
            bdb.sql_execute(insert_ripl_sql, {
                'generator_id': generator_id,
                'modelno': modelno,
                'ripl_str': string_,
            })

    def analyze_models(self, bdb, generator_id, modelnos=None, iterations=1,
            _max_seconds=None, _ckpt_iterations=None, _ckpt_seconds=None):
        update_ripl_sql = '''
            UPDATE bayesdb_venture_script_ripl SET ripl_str = :ripl_str
                WHERE generator_id = :generator_id AND modelno = :modelno
        '''
        # TODO Deal with treating None modelnos as "all of them"
        for model_no in modelnos:
            with bdb.savepoint():
                ripl = self._vs_ripl(bdb, generator_id, model_no)
                for _ in range(iterations):
                    ripl.infer(v.app(v.sym("analyze")))
                total_changes = bdb.sqlite3.total_changes
                bdb.sql_execute(update_ripl_sql, {
                    'generator_id': generator_id,
                    'modelno': model_no,
                    'ripl_str': ripl.serialize(),
                })
                assert bdb.sqlite3.total_changes - total_changes == 1

    def simulate(self, bdb, generator_id, modelno, constraints, colnos,
            numpredictions=1):
        ripl = self._vs_ripl(bdb, generator_id, modelno)
        results = []
        for _ in range(numpredictions):
            result = []
            for (colno, value) in constraints:
                ripl.infer(v.app(v.sym("obs_cell"), colno, v.quote(value)))
            for colno in colnos:
                result.append(ripl.infer(v.app(v.sym("sim_cell"), colno)))
            results.append(result)
        return results
