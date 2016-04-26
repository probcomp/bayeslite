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

"""BQL execution.

This module implements the main dispatcher for executing different
kinds of BQL phrases.  Queries, as in ``SELECT``, ``ESTIMATE``, and so
on, are compiled into SQL; commands, as in ``CREATE TABLE``,
``INSERT``, and the rest of the DDL/DML (Data Definition/Modelling
language) are executed directly.
"""

import apsw

import bayeslite.ast as ast
import bayeslite.bqlfn as bqlfn
import bayeslite.compiler as compiler
import bayeslite.core as core
import bayeslite.txn as txn

from bayeslite.exception import BQLError
from bayeslite.schema import bayesdb_schema_required
from bayeslite.sqlite3_util import sqlite3_quote_name
from bayeslite.util import casefold
from bayeslite.util import cursor_value

def execute_phrase(bdb, phrase, bindings=()):
    """Execute the BQL AST phrase `phrase` and return a cursor of results."""
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
        winders, unwinders = out.getwindings()
        return execute_wound(bdb, winders, unwinders, out.getvalue(),
            out.getbindings())

    if isinstance(phrase, ast.Begin):
        txn.bayesdb_begin_transaction(bdb)
        return empty_cursor(bdb)

    if isinstance(phrase, ast.Rollback):
        txn.bayesdb_rollback_transaction(bdb)
        return empty_cursor(bdb)

    if isinstance(phrase, ast.Commit):
        txn.bayesdb_commit_transaction(bdb)
        return empty_cursor(bdb)

    if isinstance(phrase, ast.CreateTabAs):
        assert ast.is_query(phrase.query)
        with bdb.savepoint():
            out = compiler.Output(n_numpar, nampar_map, bindings)
            qt = sqlite3_quote_name(phrase.name)
            temp = 'TEMP ' if phrase.temp else ''
            ifnotexists = 'IF NOT EXISTS ' if phrase.ifnotexists else ''
            out.write('CREATE %sTABLE %s%s AS ' % (temp, ifnotexists, qt))
            compiler.compile_query(bdb, phrase.query, out)
            winders, unwinders = out.getwindings()
            with compiler.bayesdb_wind(bdb, winders, unwinders):
                bdb.sql_execute(out.getvalue(), out.getbindings())
        return empty_cursor(bdb)

    if isinstance(phrase, ast.CreateTabSim):
        assert isinstance(phrase.simulation, ast.Simulate)
        with bdb.savepoint():
            if core.bayesdb_has_generator(bdb, phrase.name):
                raise BQLError(bdb, 'Name already defined as generator: %s' %
                    (repr(phrase.name),))
            if core.bayesdb_has_table(bdb, phrase.name):
                raise BQLError(bdb, 'Name already defined as table: %s' %
                    (repr(phrase.name),))
            if not core.bayesdb_has_generator_default(bdb,
                    phrase.simulation.generator):
                raise BQLError(bdb, 'No such generator: %s' %
                    (phrase.simulation.generator,))
            generator_id = core.bayesdb_get_generator_default(bdb,
                phrase.simulation.generator)
            metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
            table = core.bayesdb_generator_table(bdb, generator_id)
            qn = sqlite3_quote_name(phrase.name)
            qt = sqlite3_quote_name(table)
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
                    raise BQLError(bdb, 'No such column'
                        ' in generator %s table %s: %s' %
                        (repr(phrase.simulation.generator),
                         repr(table),
                         repr(column_name)))
            for column_name, _expression in phrase.simulation.constraints:
                if casefold(column_name) not in column_sqltypes:
                    raise BQLError(bdb, 'No such column'
                        ' in generator %s table %s: %s' %
                        (repr(phrase.simulation.generator),
                         repr(table),
                         repr(column_name)))
            # XXX Move to compiler.py.
            # XXX Copypasta of this in compile_simulate!
            out = compiler.Output(n_numpar, nampar_map, bindings)
            out.write('SELECT ')
            with compiler.compiling_paren(bdb, out, 'CAST(', ' AS INTEGER)'):
                compiler.compile_nobql_expression(bdb,
                    phrase.simulation.nsamples, out)
            out.write(', ')
            with compiler.compiling_paren(bdb, out, 'CAST(', ' AS INTEGER)'):
                compiler.compile_nobql_expression(bdb,
                    phrase.simulation.modelno, out)
            for _column_name, expression in phrase.simulation.constraints:
                out.write(', ')
                compiler.compile_nobql_expression(bdb, expression, out)
            winders, unwinders = out.getwindings()
            with compiler.bayesdb_wind(bdb, winders, unwinders):
                cursor = bdb.sql_execute(out.getvalue(),
                    out.getbindings()).fetchall()
            assert len(cursor) == 1
            nsamples = cursor[0][0]
            assert isinstance(nsamples, int)
            modelno = cursor[0][1]
            assert modelno is None or isinstance(modelno, int)
            constraints = \
                [(core.bayesdb_generator_column_number(bdb, generator_id, name),
                        value)
                    for (name, _expression), value in
                        zip(phrase.simulation.constraints, cursor[0][2:])]
            colnos = \
                [core.bayesdb_generator_column_number(bdb, generator_id, name)
                    for name in column_names]
            bdb.sql_execute('CREATE %sTABLE %s%s (%s)' %
                ('TEMP ' if phrase.temp else '',
                 'IF NOT EXISTS ' if phrase.ifnotexists else '',
                 qn,
                 ','.join('%s %s' %
                        (qcn, column_sqltypes[casefold(column_name)])
                    for qcn, column_name in zip(qcns, column_names))))
            insert_sql = '''
                INSERT INTO %s (%s) VALUES (%s)
            ''' % (qn, ','.join(qcns), ','.join('?' for qcn in qcns))
            for row in bqlfn.bayesdb_simulate(bdb, generator_id, constraints,
                    colnos, modelno=modelno, numpredictions=nsamples):
                bdb.sql_execute(insert_sql, row)
        return empty_cursor(bdb)

    if isinstance(phrase, ast.DropTab):
        with bdb.savepoint():
            sql = 'SELECT COUNT(*) FROM bayesdb_generator WHERE tabname = ?'
            cursor = bdb.sql_execute(sql, (phrase.name,))
            if 0 < cursor_value(cursor):
                # XXX Automatically delete the generators?  Generators
                # are more interesting than triggers and indices, so
                # automatic deletion is not obviously right.
                raise BQLError(bdb, 'Table still in use by generators: %s' %
                    (repr(phrase.name),))
            bdb.sql_execute('DELETE FROM bayesdb_column WHERE tabname = ?',
                (phrase.name,))
            ifexists = 'IF EXISTS ' if phrase.ifexists else ''
            qt = sqlite3_quote_name(phrase.name)
            return bdb.sql_execute('DROP TABLE %s%s' % (ifexists, qt))

    if isinstance(phrase, ast.AlterTab):
        with bdb.savepoint():
            table = phrase.table
            if not core.bayesdb_has_table(bdb, table):
                raise BQLError(bdb, 'No such table: %s' % (repr(table),))
            for cmd in phrase.commands:
                if isinstance(cmd, ast.AlterTabRenameTab):
                    # If the names differ only in case, we have to do
                    # some extra work because SQLite will reject the
                    # table rename.  Note that we may even have table
                    # == cmd.name here, but if the stored table name
                    # differs in case from cmd.name, we want to update
                    # it anyway.
                    if casefold(table) == casefold(cmd.name):
                        # Go via a temporary table.
                        temp = table + '_temp'
                        while core.bayesdb_has_table(bdb, temp) or \
                              core.bayesdb_has_generator(bdb, temp):
                            temp += '_temp'
                        rename_table(bdb, table, temp)
                        rename_table(bdb, temp, cmd.name)
                    else:
                        # Make sure nothing else has this name and
                        # rename it.
                        if core.bayesdb_has_table(bdb, cmd.name):
                            raise BQLError(bdb, 'Name already defined as table'
                                ': %s' %
                                (repr(cmd.name),))
                        if core.bayesdb_has_generator(bdb, cmd.name):
                            raise BQLError(bdb, 'Name already defined'
                                ' as generator: %s' %
                                (repr(cmd.name),))
                        rename_table(bdb, table, cmd.name)
                    # Remember the new name for subsequent commands.
                    table = cmd.name
                elif isinstance(cmd, ast.AlterTabRenameCol):
                    # XXX Need to deal with this in the compiler.
                    raise NotImplementedError('Renaming columns'
                        ' not yet implemented.')
                    # Make sure the old name exist and the new name does not.
                    old_folded = casefold(cmd.old)
                    new_folded = casefold(cmd.new)
                    if old_folded != new_folded:
                        if not core.bayesdb_table_has_column(bdb, table,
                                cmd.old):
                            raise BQLError(bdb, 'No such column in table %s'
                                ': %s' %
                                (repr(table), repr(cmd.old)))
                        if core.bayesdb_table_has_column(bdb, table, cmd.new):
                            raise BQLError(bdb, 'Column already exists'
                                ' in table %s: %s' %
                                (repr(table), repr(cmd.new)))
                    # Update bayesdb_column.  Everything else refers
                    # to columns by (tabname, colno) pairs rather than
                    # by names.
                    update_column_sql = '''
                        UPDATE bayesdb_column SET name = :new
                            WHERE tabname = :table AND name = :old
                    '''
                    total_changes = bdb._sqlite3.totalchanges()
                    bdb.sql_execute(update_column_sql, {
                        'table': table,
                        'old': cmd.old,
                        'new': cmd.new,
                    })
                    assert bdb._sqlite3.totalchanges() - total_changes == 1
                    # ...except metamodels may have the (case-folded)
                    # name cached.
                    if old_folded != new_folded:
                        generators_sql = '''
                            SELECT id FROM bayesdb_generator WHERE tabname = ?
                        '''
                        cursor = bdb.sql_execute(generators_sql, (table,))
                        for (generator_id,) in cursor:
                            metamodel = core.bayesdb_generator_metamodel(bdb,
                                generator_id)
                            metamodel.rename_column(bdb, generator_id,
                                old_folded, new_folded)
                elif isinstance(cmd, ast.AlterTabSetDefGen):
                    if not core.bayesdb_has_generator(bdb, cmd.generator):
                        raise BQLError(bdb, 'No such generator: %s' %
                            (repr(cmd.generator),))
                    generator_id = core.bayesdb_get_generator(bdb,
                        cmd.generator)
                    bayesdb_schema_required(bdb, 6, "generator defaults")
                    unset_default_sql = '''
                        UPDATE bayesdb_generator SET defaultp = 0
                            WHERE tabname = ? AND defaultp
                    '''
                    total_changes = bdb._sqlite3.totalchanges()
                    bdb.sql_execute(unset_default_sql, (table,))
                    assert bdb._sqlite3.totalchanges() - total_changes in (0, 1)
                    set_default_sql = '''
                        UPDATE bayesdb_generator SET defaultp = 1 WHERE id = ?
                    '''
                    total_changes = bdb._sqlite3.totalchanges()
                    bdb.sql_execute(set_default_sql, (generator_id,))
                    assert bdb._sqlite3.totalchanges() - total_changes == 1
                elif isinstance(cmd, ast.AlterTabUnsetDefGen):
                    unset_default_sql = '''
                        UPDATE bayesdb_generator SET defaultp = 0
                            WHERE tabname = ? AND defaultp
                    '''
                    total_changes = bdb._sqlite3.totalchanges()
                    bdb.sql_execute(unset_default_sql, (table,))
                    assert bdb._sqlite3.totalchanges() - total_changes in (0, 1)
                else:
                    assert False, 'Invalid alter table command: %s' % \
                        (cmd,)
        return empty_cursor(bdb)

    if isinstance(phrase, ast.CreateGen):
        # Find the metamodel.
        if phrase.metamodel not in bdb.metamodels:
            raise BQLError(bdb, 'No such metamodel: %s' %
                (repr(phrase.metamodel),))
        metamodel = bdb.metamodels[phrase.metamodel]

        # Let the metamodel parse the schema itself and call
        # create_generator with the modelled columns.
        with bdb.savepoint():
            if core.bayesdb_has_generator(bdb, phrase.name):
                if not phrase.ifnotexists:
                    raise BQLError(bdb,
                                   'Name already defined as generator: %s' %
                        (repr(phrase.name),))
            else:
                def instantiate(columns):
                    return instantiate_generator(bdb, phrase.name, phrase.table,
                        metamodel, columns,
                        default=phrase.default)
                metamodel.create_generator(bdb, phrase.table, phrase.schema,
                    instantiate)

        # All done.  Nothing to return.
        return empty_cursor(bdb)

    if isinstance(phrase, ast.DropGen):
        with bdb.savepoint():
            if not core.bayesdb_has_generator(bdb, phrase.name):
                if phrase.ifexists:
                    return empty_cursor(bdb)
                raise BQLError(bdb, 'No such generator: %s' %
                    (repr(phrase.name),))
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
                DELETE FROM bayesdb_generator WHERE id = ?
            '''
            bdb.sql_execute(drop_generator_sql, (generator_id,))
        return empty_cursor(bdb)

    if isinstance(phrase, ast.AlterGen):
        with bdb.savepoint():
            generator = phrase.generator
            if not core.bayesdb_has_generator(bdb, generator):
                raise BQLError(bdb, 'No such generator: %s' %
                    (repr(generator),))
            generator_id = core.bayesdb_get_generator(bdb, generator)
            for cmd in phrase.commands:
                if isinstance(cmd, ast.AlterGenRenameGen):
                    # Make sure nothing else has this name.
                    if casefold(generator) != casefold(cmd.name):
                        if core.bayesdb_has_table(bdb, cmd.name):
                            raise BQLError(bdb, 'Name already defined as table'
                                ': %s' %
                                (repr(cmd.name),))
                        if core.bayesdb_has_generator(bdb, cmd.name):
                            raise BQLError(bdb, 'Name already defined'
                                ' as generator: %s' %
                                (repr(cmd.name),))
                    # Update bayesdb_generator.  Everything else
                    # refers to it by id.
                    update_generator_sql = '''
                        UPDATE bayesdb_generator SET name = ? WHERE id = ?
                    '''
                    total_changes = bdb._sqlite3.totalchanges()
                    bdb.sql_execute(update_generator_sql,
                        (cmd.name, generator_id))
                    assert bdb._sqlite3.totalchanges() - total_changes == 1
                    # Remember the new name for subsequent commands.
                    generator = cmd.name
                else:
                    assert False, 'Invalid ALTER GENERATOR command: %s' % \
                        (repr(cmd),)
        return empty_cursor(bdb)

    if isinstance(phrase, ast.InitModels):
        if not core.bayesdb_has_generator_default(bdb, phrase.generator):
            raise BQLError(bdb, 'No such generator: %s' %
                (phrase.generator,))
        generator_id = core.bayesdb_get_generator_default(bdb,
            phrase.generator)
        modelnos = range(phrase.nmodels)
        model_config = None         # XXX For now.

        with bdb.savepoint():
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
                    raise BQLError(bdb, 'Generator %s already has models: %s' %
                        (repr(phrase.generator), sorted(existing)))

            # Stop now if there's nothing to initialize.
            if len(modelnos) == 0:
                return

            # Create the bayesdb_generator_model records.
            modelnos = sorted(modelnos)
            insert_model_sql = '''
                INSERT INTO bayesdb_generator_model
                    (generator_id, modelno, iterations)
                    VALUES (:generator_id, :modelno, :iterations)
            '''
            for modelno in modelnos:
                bdb.sql_execute(insert_model_sql, {
                    'generator_id': generator_id,
                    'modelno': modelno,
                    'iterations': 0,
                })

            # Do metamodel-specific initialization.
            metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
            metamodel.initialize_models(bdb, generator_id, modelnos,
                model_config)
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
        if not core.bayesdb_has_generator_default(bdb, phrase.generator):
            raise BQLError(bdb, 'No such generator: %s' %
                (phrase.generator,))
        generator_id = core.bayesdb_get_generator_default(bdb,
            phrase.generator)
        metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
        # XXX Should allow parameters for iterations and ckpt/iter.
        metamodel.analyze_models(bdb, generator_id,
            modelnos=phrase.modelnos,
            iterations=phrase.iterations,
            max_seconds=phrase.seconds,
            ckpt_iterations=phrase.ckpt_iterations,
            ckpt_seconds=phrase.ckpt_seconds)
        return empty_cursor(bdb)

    if isinstance(phrase, ast.DropModels):
        with bdb.savepoint():
            generator_id = core.bayesdb_get_generator_default(bdb,
                phrase.generator)
            metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
            modelnos = None
            if phrase.modelnos is not None:
                lookup_model_sql = '''
                    SELECT COUNT(*) FROM bayesdb_generator_model
                        WHERE generator_id = :generator_id
                        AND modelno = :modelno
                '''
                modelnos = sorted(list(phrase.modelnos))
                for modelno in modelnos:
                    cursor = bdb.sql_execute(lookup_model_sql, {
                        'generator_id': generator_id,
                        'modelno': modelno,
                    })
                    if cursor_value(cursor) == 0:
                        raise BQLError(bdb, 'No such model'
                            ' in generator %s: %s' %
                            (repr(phrase.generator), repr(modelno)))
            metamodel.drop_models(bdb, generator_id, modelnos=modelnos)
            if modelnos is None:
                drop_models_sql = '''
                    DELETE FROM bayesdb_generator_model WHERE generator_id = ?
                '''
                bdb.sql_execute(drop_models_sql, (generator_id,))
            else:
                drop_model_sql = '''
                    DELETE FROM bayesdb_generator_model
                        WHERE generator_id = :generator_id
                        AND modelno = :modelno
                '''
                for modelno in modelnos:
                    bdb.sql_execute(drop_model_sql, {
                        'generator_id': generator_id,
                        'modelno': modelno,
                    })
        return empty_cursor(bdb)

    assert False                # XXX

# Metamodel-independent logic to create a generator.  Don't call this
# directly yourself -- it must be called via the metamodel's
# create_generator method.
def instantiate_generator(bdb, gen_name, table, metamodel, columns,
                          default=None):
    if default is None:
        default = False

    # Make sure there is no table by this name.
    if core.bayesdb_has_table(bdb, gen_name):
        raise BQLError(bdb, 'Name already defined as table: %s' %
            (repr(gen_name),))

    # Make sure the bayesdb_column table knows all the columns.
    core.bayesdb_table_guarantee_columns(bdb, table)

    generator_already_existed = False
    if core.bayesdb_has_generator(bdb, gen_name):
        generator_already_existed = True
    else:
        # Create the generator record.
        generator_sql = '''INSERT INTO bayesdb_generator
                           (name, tabname, metamodel, defaultp)
                           VALUES (:name, :table, :metamodel, :defaultp)'''
        cursor = bdb.sql_execute(generator_sql, {
            'name': gen_name,
            'table': table,
            'metamodel': metamodel.name(),
            'defaultp': default,
        })
    generator_id = core.bayesdb_get_generator(bdb, gen_name)

    assert generator_id
    assert 0 < generator_id

    # Get a map from column name to colno.  Check
    # - for duplicates,
    # - for nonexistent columns,
    # - for invalid statistical types.
    column_map = {}
    duplicates = set()
    missing = set()
    invalid = set()
    colno_sql = '''
        SELECT colno FROM bayesdb_column
            WHERE tabname = :table AND name = :column_name
    '''
    stattype_sql = '''
        SELECT COUNT(*) FROM bayesdb_stattype WHERE name = :stattype
    '''
    for name, stattype in columns:
        name_folded = casefold(name)
        if name_folded in column_map:
            duplicates.add(name)
            continue
        cursor = bdb.sql_execute(colno_sql, {
            'table': table,
            'column_name': name,
        })
        try:
            row = cursor.next()
        except StopIteration:
            missing.add(name)
            continue
        else:
            colno = row[0]
            assert isinstance(colno, int)
            cursor = bdb.sql_execute(stattype_sql, {
                'stattype': stattype,
            })
            if cursor_value(cursor) == 0:
                invalid.add(stattype)
                continue
            column_map[casefold(name)] = colno
    # XXX Would be nice to report these simultaneously.
    if missing:
        raise BQLError(bdb, 'No such columns in table %s: %s' %
            (repr(table), repr(list(missing))))
    if duplicates:
        raise BQLError(bdb, 'Duplicate column names: %s' %
            (repr(list(duplicates)),))
    if invalid:
        raise BQLError(bdb, 'Invalid statistical types: %s' %
            (repr(list(invalid)),))

    if not generator_already_existed:
        # Insert column records.
        column_sql = '''
            INSERT INTO bayesdb_generator_column
            (generator_id, colno, stattype)
            VALUES (:generator_id, :colno, :stattype)
        '''
        for name, stattype in columns:
            colno = column_map[casefold(name)]
            stattype = casefold(stattype)
            bdb.sql_execute(column_sql, {
                'generator_id': generator_id,
                'colno': colno,
                'stattype': stattype,
            })

    column_list = sorted((column_map[casefold(name)], name, stattype)
        for name, stattype in columns)
    return generator_id, column_list

def rename_table(bdb, old, new):
    assert core.bayesdb_has_table(bdb, old)
    assert not core.bayesdb_has_generator(bdb, old)
    assert not core.bayesdb_has_table(bdb, new)
    assert not core.bayesdb_has_generator(bdb, new)
    # Rename the SQL table.
    qo = sqlite3_quote_name(old)
    qn = sqlite3_quote_name(new)
    rename_sql = 'ALTER TABLE %s RENAME TO %s' % (qo, qn)
    bdb.sql_execute(rename_sql)
    # Update bayesdb_column to use the new name.
    update_columns_sql = '''
        UPDATE bayesdb_column SET tabname = ? WHERE tabname = ?
    '''
    bdb.sql_execute(update_columns_sql, (new, old))
    # Update bayesdb_column_map to use the new name.
    update_column_maps_sql = '''
        UPDATE bayesdb_column_map SET tabname = ? WHERE tabname = ?
    '''
    bdb.sql_execute(update_column_maps_sql, (new, old))
    # Update bayesdb_generator to use the new name.
    update_generators_sql = '''
        UPDATE bayesdb_generator SET tabname = ? WHERE tabname = ?
    '''
    bdb.sql_execute(update_generators_sql, (new, old))

def empty_cursor(bdb):
    return None

def execute_wound(bdb, winders, unwinders, sql, bindings):
    if len(winders) == 0 and len(unwinders) == 0:
        return bdb.sql_execute(sql, bindings)
    with bdb.savepoint():
        for (wsql, wbindings) in winders:
            bdb.sql_execute(wsql, wbindings)
        try:
            return WoundCursor(bdb, bdb.sql_execute(sql, bindings), unwinders)
        except:
            for (usql, ubindings) in unwinders:
                bdb.sql_execute(usql, ubindings)
            raise

class BayesDBCursor(object):
    """Cursor for a BQL or SQL query from a BayesDB."""
    def __init__(self, bdb, cursor):
        self._bdb = bdb
        self._cursor = cursor
        # XXX Must save the description early because apsw discards it
        # after we have iterated over all rows -- or if there are no
        # rows, discards it immediately!
        try:
            self._description = cursor.description
        except apsw.ExecutionCompleteError:
            self._description = []
        else:
            assert self._description is not None
            if self._description is None:
                self._description = []
    def __iter__(self):
        return self
    def next(self):
        return self._cursor.next()
    def fetchone(self):
        return self._cursor.fetchone()
    def fetchvalue(self):
        return cursor_value(self)
    def fetchmany(self, size=1):
        with txn.bayesdb_caching(self._bdb):
            return self._cursor.fetchmany(size=size)
    def fetchall(self):
        with txn.bayesdb_caching(self._bdb):
            return self._cursor.fetchall()
    @property
    def connection(self):
        return self._bdb
    @property
    def lastrowid(self):
        return self._bdb.last_insert_rowid()
    @property
    def description(self):
        return self._description

class WoundCursor(BayesDBCursor):
    def __init__(self, bdb, cursor, unwinders):
        self._unwinders = unwinders
        super(WoundCursor, self).__init__(bdb, cursor)
    def __del__(self):
        del self._cursor
        # If the database is still open, we need to undo the effects
        # of the cursor when done.  But the effects are (intended to
        # be) in-memory only, so otherwise, if the database is closed,
        # we need not do anything.
        #
        # XXX Name the question of whether it's closed a little less
        # kludgily.  (But that might encourage people outside to
        # depend on that, which is not such a great idea.)
        if self._bdb._sqlite3 is not None:
            for sql, bindings in reversed(self._unwinders):
                self._bdb.sql_execute(sql, bindings)
        # Apparently object doesn't have a __del__ method.
        #super(WoundCursor, self).__del__()
