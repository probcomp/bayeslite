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

import contextlib

import bayeslite.ast as ast
import bayeslite.bqlfn as bqlfn
import bayeslite.compiler as compiler
import bayeslite.core as core
import bayeslite.import_csv as import_csv
import bayeslite.txn as txn

from bayeslite.sqlite3_util import sqlite3_quote_name
from bayeslite.util import casefold

def execute_phrase(bdb, phrase, bindings=()):
    '''Execute the BQL AST phrase PHRASE and return a cursor of results.'''
    if isinstance(phrase, ast.Parametrized):
        n_numpar = phrase.n_numpar
        nampar_map = phrase.nampar_map
        phrase = phrase.phrase
        assert 0 < n_numpar
    else:
        n_numpar = 0
        nampar_map = None
        # Ignore extraneous bindings.  XXX Bad idea?

    if ast.is_query(phrase):
        # Compile the query in the transaction in case we need to
        # execute subqueries to determine column lists.  Compiling is
        # a quick tree descent, so this should be fast.
        out = compiler.Output(n_numpar, nampar_map, bindings)
        with bdb.savepoint():
            compiler.compile_query(bdb, phrase, out)
        return bdb.sql_execute(out.getvalue(), out.getbindings())

    if isinstance(phrase, ast.Begin):
        txn.bayesdb_begin_transaction(bdb)
        return empty_cursor(bdb)

    if isinstance(phrase, ast.Rollback):
        txn.bayesdb_rollback_transaction(bdb)
        return empty_cursor(bdb)

    if isinstance(phrase, ast.Commit):
        txn.bayesdb_commit_transaction(bdb)
        return empty_cursor(bdb)

    if isinstance(phrase, ast.CreateTableAs):
        assert ast.is_query(phrase.query)
        with bdb.savepoint():
            out = compiler.Output(n_numpar, nampar_map, bindings)
            qt = sqlite3_quote_name(phrase.name)
            temp = 'TEMP ' if phrase.temp else ''
            ifnotexists = 'IF NOT EXISTS ' if phrase.ifnotexists else ''
            out.write('CREATE %sTABLE %s%s AS ' % (temp, ifnotexists, qt))
            compiler.compile_query(bdb, phrase.query, out)
            return bdb.sql_execute(out.getvalue(), out.getbindings())

    if isinstance(phrase, ast.CreateTableSim):
        assert isinstance(phrase.simulation, ast.Simulate)
        with bdb.savepoint():
            if bayesdb_has_generator(bdb, phrase.name):
                raise ValueError('Name already defined as generator: %s' %
                    (repr(phrase.name),))
            if bayesdb_has_table(bdb, phrase.name):
                raise ValueError('Name already defined as table: %s' %
                    (repr(phrase.name),))
            generator_id = core.bayesdb_get_generator(bdb, phrase.generator)
            metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
            table = core.bayesdb_generator_table(bdb, generator_id)
            qn = sqlite3_quote_name(phrase.name)
            qt = sqlite3_quote_name(table)
            qgn = sqlite3_quote_name(phrase.generator)
            column_names = phrase.simulation.columns
            qcns = map(sqlite3_quote_name, column_names)
            cursor = bdb.sql_execute('PRAGMA table_info(%s)' % (qt,))
            column_sqltypes = {}
            for _colno, name, sqltype, _nonnull, _default, _primary in cursor:
                assert casefold(name) not in column_sqltypes
                column_sqltypes[casefold(name)] = sqltype
            assert 0 < len(column_sqltypes)
            for column_name in column_names:
                if casefold(column_name) not in column_sqltypes:
                    raise ValueError('No such column in generator %s table %s'
                        ': %s' %
                        (repr(phrase.generator),
                         repr(table),
                         repr(column_name)))
            for column_name, _expression in phrase.simulation.constraints:
                if casefold(column_name) not in column_sqltypes:
                    raise ValueError('No such column in generator %s table %s'
                        ': %s' %
                        (repr(phrase.generator),
                         repr(table),
                         repr(column_name)))
            # XXX Move to compiler.py.
            out = compiler.Output(n_numpar, nampar_map, bindings)
            out.write('SELECT ')
            with compiler.compiling_paren(bdb, out, 'CAST(', ' AS INTEGER)'):
                compiler.compile_nobql_expression(bdb,
                    phrase.simulation.nsamples, out)
            for _column_name, expression in phrase.simulation.constraints:
                out.write(', ')
                compiler.compile_nobql_expression(bdb, expression, out)
            cursor = list(bdb.sql_execute(out.getvalue(), out.getbindings()))
            assert len(cursor) == 1
            nsamples = cursor[0][0]
            assert isinstance(nsamples, int)
            constraints = \
                [(core.bayesdb_generator_column_number(bdb, generator_id, name),
                        value)
                    for (name, _expression), value in
                        zip(phrase.simulation.constraints, cursor[0][1:])]
            colnos = \
                [core.bayesdb_generator_column_number(bdb, generator_id, name)
                    for name in column_names]
            bdb.sql_execute('CREATE %sTABLE %s%s (%s)' %
                ('TEMP ' if phrase.temp else '',
                 'IF NOT EXISTS ' if phrase.ifnotexists else '',
                 qn,
                 ','.join('%s %s' % (qcn, column_types[casefold(column_name)])
                            for qcn, column_name in zip(qcns, column_names))))
            insert_sql = '''
                INSERT INTO %s (%s) VALUES (%s)
            ''' % (qn, ','.join(qcns), ','.join('?' for qcn in qcns))
            for row in bqlfn.bayesdb_simulate(bdb, generator_id, constraints,
                    colnos, numpredictions=nsamples):
                bdb.sql_execute(insert_sql, row)
        return empty_cursor(bdb)

    if isinstance(phrase, ast.DropTable):
        ifexists = 'IF EXISTS ' if phrase.ifexists else ''
        qt = sqlite3_quote_name(phrase.name)
        return bdb.sql_execute('DROP TABLE %s%s' % (ifexists, qt))

    if isinstance(phrase, ast.CreateGen):
        assert isinstance(phrase.schema, ast.GenSchema)
        name = phrase.name
        table = phrase.table
        if phrase.metamodel not in bdb.metamodels:
            raise ValueError('No such metamodel: %s' %
                (repr(phrase.metamodel),))
        metamodel = bdb.metamodels[phrase.metamodel]
        # XXX The metamodel should have responsibility for supplying
        # the actual columns, after munching up an arbitrary schema.
        with bdb.savepoint():
            if core.bayesdb_has_table(bdb, phrase.name):
                raise ValueError('Name already defined as table: %s' %
                    (repr(phrase.name),))

            # Make sure the bayesdb_column table knows all the columns.
            core.bayesdb_table_guarantee_columns(bdb, table)

            # Create the generator record.
            generator_sql = '''
                INSERT%s INTO bayesdb_generator (name, tabname, metamodel)
                    VALUES (?, ?, ?)
            ''' % (' OR IGNORE' if phrase.ifnotexists else '')
            parameters = (name, table, metamodel.name())
            cursor = bdb.sql_execute(generator_sql, parameters)
            generator_id = cursor.lastrowid
            assert generator_id
            assert 0 < generator_id

            # Get a map from column name to (colno, stattype).  Check
            # - for duplicates,
            # - for nonexistent columns,
            # - for invalid statistical types.
            columns = {}
            duplicates = set()
            missing = set()
            invalid = set()
            colno_sql = '''
                SELECT colno FROM bayesdb_column WHERE tabname = ? AND name = ?
            '''
            stattype_sql = '''
                SELECT COUNT(*) FROM bayesdb_stattype WHERE name = ?
            '''
            for column in phrase.schema.columns:
                column_name = casefold(column.name)
                if column_name in columns:
                    duplicates.add(column_name)
                    continue
                cursor = bdb.sql_execute(colno_sql, (table, column_name))
                try:
                    row = cursor.next()
                except StopIteration:
                    missing.add(column_name)
                    continue
                else:
                    colno = row[0]
                    assert isinstance(colno, int)
                    stattype = column.stattype
                    cursor = bdb.sql_execute(stattype_sql, (stattype,))
                    if cursor.next()[0] == 0:
                        invalid.add(stattype)
                        continue
                    columns[column_name] = (colno, stattype)
            # XXX Would be nice to report these simultaneously.
            if missing:
                raise ValueError('No such columns in table %s: %s' %
                    (repr(table), repr(list(missing))))
            if duplicates:
                raise ValueError('Duplicate column names: %s' %
                    (repr(list(duplicates)),))
            if invalid:
                raise ValueError('Invalid statistical types: %s' %
                    (repr(list(invalid)),))

            # Insert column records.
            column_sql = '''
                INSERT INTO bayesdb_generator_column
                    (generator_id, colno, stattype)
                    VALUES (?, ?, ?)
            '''
            for column in phrase.schema.columns:
                colno, stattype = columns[casefold(column.name)]
                stattype = casefold(stattype)
                bdb.sql_execute(column_sql, (generator_id, colno, stattype))

            # Metamodel-specific construction.
            column_list = sorted((colno, name, stattype)
                for name, (colno, stattype) in columns.iteritems())
            metamodel.create_generator(bdb, generator_id, column_list)

        # All done.  Nothing to return.
        return empty_cursor(bdb)

    if isinstance(phrase, ast.DropGen):
        with bdb.savepoint():
            generator_id = core.bayesdb_get_generator(bdb, phrase.name)
            metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)

            # Metamodel-specific destruction.
            metamodel.drop_generator(bdb, generator_id)

            # Drop the columns, models, and, finally, generator.
            drop_columns_sql = '''
                DELETE FROM bayesdb_generator_column WHERE generator_id = ?
            '''
            bdb.sql_execute(drop_columns_sql, (generator_id,))
            drop_model_sql = '''
                DELETE FROM bayesdb_generator_model WHERE generator_id = ?
            '''
            bdb.sql_execute(drop_model_sql, (generator_id,))
            drop_generator_sql = '''
                DELETE FROM bayesdb_generator WHERE generator_id = ?
            '''
            bdb.sql_execute(drop_generator_sql, (generator_id,))
        return empty_cursor(bdb)

    if isinstance(phrase, ast.RenameGen):
        with bdb.savepoint():
            # Ensure the old name exists and the new one doesn't.
            # XXX What about `RENAME X TO X'?
            if not core.bayesdb_has_generator(bdb, phrase.oldname):
                raise ValueError('No such generator: %s' %
                    (repr(phrase.oldname),))
            if core.bayesdb_has_generator(bdb, phrase.newname):
                raise ValueError('Name already defined as generator: %s' %
                    (repr(phrase.newname),))
            if core.bayesdb_has_table(bdb, phrase.newname):
                raise ValueError('Name already defined as generator: %s' %
                    (repr(phrase.newname),))

            # Rename by changing the `name' column of the generator.
            # Everything else refers to generators by id.
            rename_sql = 'UPDATE bayesdb_generator SET name = ? WHERE name = ?'
            bdb.sql_execute(rename_sql, (phrase.oldname, phrase.newname))
        return empty_cursor(bdb)

    if isinstance(phrase, ast.InitModels):
        with bdb.savepoint():
            # Grab the arguments.
            generator_id = core.bayesdb_get_generator(bdb, phrase.generator)
            modelnos = range(phrase.nmodels)
            model_config = None         # XXX For now.

            # Find the model numbers.  Omit existing ones for
            # ifnotexists; reject existing ones otherwise.
            if phrase.ifnotexists:
                modelnos = set(modelno for modelno in modelnos
                    if not core.bayesdb_generator_has_model(bdb, generator_id,
                        modelno))
            else:
                existing = set(modelno for modelno in modelnos
                    if core.bayesdb_generator_has_model(bdb, generator_id,
                        modelno))
                if 0 < len(existing):
                    raise ValueError('Generator %s already has models: %s' %
                        (repr(phrase.generator), sorted(existing)))

            # Stop now if there's nothing to initialize.
            if len(modelnos) == 0:
                return

            # Create the bayesdb_generator_model records.
            modelnos = sorted(modelnos)
            insert_model_sql = '''
                INSERT INTO bayesdb_generator_model
                    (generator_id, modelno, iterations)
                    VALUES (?, ?, 0)
            '''
            for modelno in modelnos:
                bdb.sql_execute(insert_model_sql, (generator_id, modelno))

            # Do metamodel-specific initialization.
            metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
            metamodel.initialize(bdb, generator_id, modelnos, model_config)
        return empty_cursor(bdb)

    if isinstance(phrase, ast.AnalyzeModels):
        if not phrase.wait:
            raise NotImplementedError('No background analysis -- use WAIT.')
        # WARNING: It is the metamodel's responsibility to work in a
        # transaction.
        #
        # WARNING: It is the metamodel's responsibility to update the
        # iteration count in bayesdb_generator_model records.
        #
        # We do this so that the metamodel can save incremental
        # progress in case of ^C in the middle.
        #
        # XXX Put these warning somewhere more appropriate.
        generator_id = core.bayesdb_get_generator(bdb, phrase.generator)
        metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
        metamodel.analyze(bdb, generator_id,
            modelnos=phrase.modelnos,
            iterations=phrase.iterations,
            max_seconds=phrase.seconds)
        return empty_cursor(bdb)

    if isinstance(phrase, ast.DropModels):
        with bdb.savepoint():
            generator_id = core.bayesdb_get_generator(bdb, phrase.generator)
            metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
            metamodel.drop_models(bdb, phrase.generator, modelnos)
            modelnos = None
            if phrase.modelnos is not None:
                lookup_model_sql = '''
                    SELECT COUNT(*) FROM bayesdb_generator_model
                        WHERE generator_id = ? AND modelno = ?
                '''
                modelnos = sorted(list(phrase.modelnos))
                for modelno in modelnos:
                    parameters = (generator_id, modelno)
                    cursor = bdb.sql_execute(lookup_model_sql, parameters)
                    if cursor.next()[0] == 0:
                        raise ValueError('No such model in generator %s: %s' %
                            (repr(phrase.generator), repr(modelno)))
            if modelnos is None:
                drop_models_sql = '''
                    DELETE FROM bayesdb_generator_model WHERE generator_id = ?
                '''
                bdb.sql_execute(drop_models_sql, (generator_id,))
            else:
                drop_model_sql = '''
                    DELETE FROM bayesdb_generator_model
                        WHERE generator_id = ? AND modelno = ?
                '''
                for modelno in modelnos:
                    bdb.sql_execute(drop_model_sql, (generator_id, modelno))
        return empty_cursor(bdb)

    assert False                # XXX

# XXX Temporary kludge until we get BQL cursors proper, with, e.g.,
# declared modelled column types in cursor.description.  We go through
# sqlite3 directly to avoid cluttering the trace.
def empty_cursor(bdb):
    cursor = bdb.sqlite3.cursor()
    cursor.execute('')
    return cursor

@contextlib.contextmanager
def defer_foreign_keys(bdb):
    defer = bdb.sql_execute('PRAGMA defer_foreign_keys').next()[0]
    bdb.sql_execute('PRAGMA defer_foreign_keys = ON')
    yield
    if not defer:
        bdb.sql_execute('PRAGMA defer_foreign_keys = OFF')
        bdb.sql_execute('PRAGMA foreign_key_check')
