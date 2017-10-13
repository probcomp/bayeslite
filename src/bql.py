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

import itertools

import apsw

import bayeslite.ast as ast
import bayeslite.bqlfn as bqlfn
import bayeslite.compiler as compiler
import bayeslite.core as core
import bayeslite.guess as guess
import bayeslite.txn as txn

from bayeslite.exception import BQLError
from bayeslite.guess import bayesdb_guess_stattypes
from bayeslite.read_csv import bayesdb_read_csv_file
from bayeslite.schema import bayesdb_schema_required
from bayeslite.simulate import simulate_models_rows
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
            if core.bayesdb_has_table(bdb, phrase.name):
                if phrase.ifnotexists:
                    return empty_cursor(bdb)
                else:
                    raise BQLError(bdb,
                        'Name already defined as table: %s' %
                        (repr(phrase.name),))
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

    if isinstance(phrase, ast.CreateTabCsv):
        with bdb.savepoint():
            table_exists = core.bayesdb_has_table(bdb, phrase.name)
            if table_exists:
                if phrase.ifnotexists:
                    return empty_cursor(bdb)
                else:
                    raise BQLError(bdb, 'Table already exists: %s' %
                        (repr(phrase.name),))
            bayesdb_read_csv_file(
                bdb, phrase.name, phrase.csv, header=True, create=True)
        return empty_cursor(bdb)

    if isinstance(phrase, ast.DropTab):
        with bdb.savepoint():
            sql = 'SELECT COUNT(*) FROM bayesdb_population WHERE tabname = ?'
            cursor = bdb.sql_execute(sql, (phrase.name,))
            if 0 < cursor_value(cursor):
                raise BQLError(bdb, 'Table still in use by populations: %s' %
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
                        while core.bayesdb_has_table(bdb, temp):
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
                else:
                    assert False, 'Invalid alter table command: %s' % \
                        (cmd,)
        return empty_cursor(bdb)

    if isinstance(phrase, ast.GuessSchema):
        if not core.bayesdb_has_table(bdb, phrase.table):
            raise BQLError(bdb, 'No such table : %s' % phrase.table)
        out = compiler.Output(0, {}, {})
        with bdb.savepoint():
            qt = sqlite3_quote_name(phrase.table)
            temptable = bdb.temp_table_name()
            qtt = sqlite3_quote_name(temptable)
            cursor = bdb.sql_execute('SELECT * FROM %s' % (qt,))
            column_names = [d[0] for d in cursor.description]
            rows = cursor.fetchall()
            stattypes = bayesdb_guess_stattypes(column_names, rows)
            distinct_value_counts = [
                len(set([row[i] for row in rows]))
                for i in range(len(column_names))
            ]
            out.winder('''
                CREATE TEMP TABLE %s (column TEXT, stattype TEXT, num_distinct INTEGER, reason TEXT)
            ''' % (qtt), ())
            for cn, st, ct in zip(column_names, stattypes, distinct_value_counts):
                out.winder('''
                    INSERT INTO %s VALUES (?, ?, ?, ?)
                ''' % (qtt), (cn, st[0], ct, st[1]))
            out.write('SELECT * FROM %s' % (qtt,))
            out.unwinder('DROP TABLE %s' % (qtt,), ())
        winders, unwinders = out.getwindings()
        return execute_wound(
            bdb, winders, unwinders, out.getvalue(), out.getbindings())

    if isinstance(phrase, ast.CreatePop):
        with bdb.savepoint():
            _create_population(bdb, phrase)
        return empty_cursor(bdb)

    if isinstance(phrase, ast.DropPop):
        with bdb.savepoint():
            if not core.bayesdb_has_population(bdb, phrase.name):
                if phrase.ifexists:
                    return empty_cursor(bdb)
                raise BQLError(bdb, 'No such population: %r' % (phrase.name,))
            population_id = core.bayesdb_get_population(bdb, phrase.name)
            generator_ids = core.bayesdb_population_generators(
                bdb, population_id)
            if generator_ids:
                generators = [core.bayesdb_generator_name(bdb, gid)
                    for gid in generator_ids]
                raise BQLError(bdb, 'Population %r still has metamodels: %r' %
                    (phrase.name, generators))
            # XXX helpful error checking if generators still exist
            # XXX check change counts
            bdb.sql_execute('''
                DELETE FROM bayesdb_variable WHERE population_id = ?
            ''', (population_id,))
            bdb.sql_execute('''
                DELETE FROM bayesdb_population WHERE id = ?
            ''', (population_id,))
        return empty_cursor(bdb)

    if isinstance(phrase, ast.AlterPop):
        with bdb.savepoint():
            population = phrase.population
            if not core.bayesdb_has_population(bdb, population):
                raise BQLError(bdb, 'No such population: %s' %
                    (repr(population),))
            population_id = core.bayesdb_get_population(bdb, population)
            for cmd in phrase.commands:
                if isinstance(cmd, ast.AlterPopAddVar):
                    # Ensure column exists in base table.
                    table = core.bayesdb_population_table(bdb, population_id)
                    if not core.bayesdb_table_has_column(
                            bdb, table, cmd.name):
                        raise BQLError(bdb,
                            'No such variable in base table: %s'
                            % (cmd.name))
                    # Ensure variable not already in population.
                    if core.bayesdb_has_variable(
                            bdb, population_id, None, cmd.name):
                        raise BQLError(bdb,
                            'Variable already in population: %s'
                            % (cmd.name))
                    # Ensure there is at least observation in the column.
                    qt = sqlite3_quote_name(table)
                    qc = sqlite3_quote_name(cmd.name)
                    cursor = bdb.sql_execute(
                        'SELECT COUNT(*) FROM %s WHERE %s IS NOT NULL' %
                        (qt, qc))
                    if cursor_value(cursor) == 0:
                        raise BQLError(bdb,
                            'Cannot add variable without any values: %s'
                            % (cmd.name))
                    # If stattype is None, guess.
                    if cmd.stattype is None:
                        cursor = bdb.sql_execute(
                            'SELECT %s FROM %s' % (qc, qt))
                        rows = cursor.fetchall()
                        [stattype, reason] = bayesdb_guess_stattypes(
                            [cmd.name], rows)[0]
                        # Fail if trying to model a key.
                        if stattype == 'key':
                            raise BQLError(bdb,
                                'Values in column %s appear to be keys.'
                                % (cmd.name,))
                        # Fail if cannot determine a stattype.
                        elif stattype == 'ignore':
                            raise BQLError(bdb,
                                'Failed to determine a stattype for %s, '
                                'please specify one manually.' % (cmd.name,))
                    # If user specified stattype, ensure it exists.
                    elif not core.bayesdb_has_stattype(bdb, cmd.stattype):
                        raise BQLError(bdb,
                            'Invalid stattype: %s' % (cmd.stattype))
                    else:
                        stattype = cmd.stattype
                    # Check that strings are not being modeled as numerical.
                    if stattype == 'numerical' \
                            and _column_contains_string(bdb, table, cmd.name):
                        raise BQLError(bdb,
                            'Numerical column contains string values: %r '
                            % (qc,))
                    with bdb.savepoint():
                        # Add the variable to the population.
                        core.bayesdb_add_variable(
                            bdb, population_id, cmd.name, stattype)
                        colno = core.bayesdb_variable_number(
                            bdb, population_id, None, cmd.name)
                        # Add the variable to each (initialized) metamodel in
                        # the population.
                        generator_ids = filter(
                            lambda g: core.bayesdb_generator_modelnos(bdb, g),
                            core.bayesdb_population_generators(
                                bdb, population_id),
                        )
                        for generator_id in generator_ids:
                            # XXX Omit needless bayesdb_generator_column table
                            # Github issue #441.
                            bdb.sql_execute('''
                                INSERT INTO bayesdb_generator_column
                                    VALUES (:generator_id, :colno, :stattype)
                            ''', {
                                'generator_id': generator_id,
                                'colno': colno,
                                'stattype': stattype,
                            })
                            metamodel = core.bayesdb_generator_metamodel(
                                bdb, generator_id)
                            metamodel.add_column(bdb, generator_id, colno)
                elif isinstance(cmd, ast.AlterPopStatType):
                    # Check the no metamodels are defined for this population.
                    generators = core.bayesdb_population_generators(
                        bdb, population_id)
                    if generators:
                        raise BQLError(bdb,
                            'Cannot update statistical types for population '
                            '%s, it has metamodels: %s'
                            % (repr(population), repr(generators),))
                    # Check all the variables are in the population.
                    unknown = [
                        c for c in cmd.names if not
                        core.bayesdb_has_variable(bdb, population_id, None, c)
                    ]
                    if unknown:
                        raise BQLError(bdb,
                            'No such variables in population: %s'
                            % (repr(unknown)))
                    # Check the statistical type is valid.
                    if not core.bayesdb_has_stattype(bdb, cmd.stattype):
                        raise BQLError(bdb,
                            'Invalid statistical type: %r'
                            % (repr(cmd.stattype),))
                    # Check that strings are not being modeled as numerical.
                    if cmd.stattype == 'numerical':
                        table = core.bayesdb_population_table(
                            bdb, population_id)
                        numerical_string_vars = [
                            col for col in cmd.names
                            if _column_contains_string(bdb, table, col)
                        ]
                        if numerical_string_vars:
                            raise BQLError(bdb,
                                'Columns with string values modeled as '
                                'numerical: %r' % (numerical_string_vars,))
                    # Perform the stattype update.
                    colnos = [
                        core.bayesdb_variable_number(
                            bdb, population_id, None, c) for c in cmd.names
                    ]
                    qcolnos = ','.join('%d' % (colno,) for colno in colnos)
                    update_stattype_sql = '''
                        UPDATE bayesdb_variable SET stattype = ?
                            WHERE population_id = ? AND colno IN (%s)
                    ''' % (qcolnos,)
                    bdb.sql_execute(
                        update_stattype_sql,
                        (casefold(cmd.stattype), population_id,))
                else:
                    assert False, 'Invalid ALTER POPULATION command: %s' % \
                        (repr(cmd),)
        return empty_cursor(bdb)

    if isinstance(phrase, ast.CreateGen):
        # Find the population.
        if not core.bayesdb_has_population(bdb, phrase.population):
            raise BQLError(bdb, 'No such population: %r' %
                (phrase.population,))
        population_id = core.bayesdb_get_population(bdb, phrase.population)
        table = core.bayesdb_population_table(bdb, population_id)

        # Find the metamodel, or use the default.
        metamodel_name = phrase.metamodel
        if phrase.metamodel is None:
            metamodel_name = 'cgpm'
        if metamodel_name not in bdb.metamodels:
            raise BQLError(bdb, 'No such metamodel: %s' %
                (repr(metamodel_name),))
        metamodel = bdb.metamodels[metamodel_name]

        with bdb.savepoint():
            if core.bayesdb_has_generator(bdb, population_id, phrase.name):
                if not phrase.ifnotexists:
                    raise BQLError(
                        bdb, 'Name already defined as generator: %s' %
                        (repr(phrase.name),))
            else:
                # Insert a record into bayesdb_generator and get the
                # assigned id.
                bdb.sql_execute('''
                    INSERT INTO bayesdb_generator
                        (name, tabname, population_id, metamodel)
                        VALUES (?, ?, ?, ?)
                ''', (phrase.name, table, population_id, metamodel.name()))
                generator_id = core.bayesdb_get_generator(
                    bdb, population_id, phrase.name)

                # Populate bayesdb_generator_column.
                #
                # XXX Omit needless bayesdb_generator_column table --
                # Github issue #441.
                bdb.sql_execute('''
                    INSERT INTO bayesdb_generator_column
                        (generator_id, colno, stattype)
                        SELECT :generator_id, colno, stattype
                            FROM bayesdb_variable
                            WHERE population_id = :population_id
                                AND generator_id IS NULL
                ''', {
                    'generator_id': generator_id,
                    'population_id': population_id,
                })

                # Do any metamodel-specific initialization.
                metamodel.create_generator(
                    bdb, generator_id, phrase.schema, baseline=phrase.baseline)

                # Populate bayesdb_generator_column with any latent
                # variables that metamodel.create_generator has added
                # with bayesdb_add_latent.
                bdb.sql_execute('''
                    INSERT INTO bayesdb_generator_column
                        (generator_id, colno, stattype)
                        SELECT :generator_id, colno, stattype
                            FROM bayesdb_variable
                            WHERE population_id = :population_id
                                AND generator_id = :generator_id
                ''', {
                    'generator_id': generator_id,
                    'population_id': population_id,
                })

        # All done.  Nothing to return.
        return empty_cursor(bdb)

    if isinstance(phrase, ast.DropGen):
        with bdb.savepoint():
            if not core.bayesdb_has_generator(bdb, None, phrase.name):
                if phrase.ifexists:
                    return empty_cursor(bdb)
                raise BQLError(bdb, 'No such generator: %s' %
                    (repr(phrase.name),))
            generator_id = core.bayesdb_get_generator(bdb, None, phrase.name)
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
            if not core.bayesdb_has_generator(bdb, None, generator):
                raise BQLError(bdb, 'No such generator: %s' %
                    (repr(generator),))
            generator_id = core.bayesdb_get_generator(bdb, None, generator)
            for cmd in phrase.commands:
                if isinstance(cmd, ast.AlterGenRenameGen):
                    # Make sure nothing else has this name.
                    if casefold(generator) != casefold(cmd.name):
                        if core.bayesdb_has_table(bdb, cmd.name):
                            raise BQLError(bdb, 'Name already defined as table'
                                ': %s' %
                                (repr(cmd.name),))
                        if core.bayesdb_has_generator(bdb, None, cmd.name):
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

    if isinstance(phrase, ast.ConvertGen):
        with bdb.savepoint():
            # Get the population and table from the current metamodel
            current_name = phrase.currentname
            if not core.bayesdb_has_generator(bdb, None, current_name):
                raise BQLError(bdb, 'No such generator: %s' %
                    (repr(current_name),))
            current_generator_id = core.bayesdb_get_generator(
                    bdb, None, current_name)
            current_metamodel = core.bayesdb_generator_metamodel(
                    bdb, current_generator_id)
            population_id = core.bayesdb_generator_population(
                    bdb, current_generator_id)
            population_name = core.bayesdb_population_name(
                    bdb, population_id)
            table = core.bayesdb_population_table(bdb, population_id)

            # Initialize the new metamodel
            new_metamodel_type_name = phrase.newmetamodel
            if phrase.newmetamodel is None:
                new_metamodel_type_name = 'cgpm'
            if new_metamodel_type_name not in bdb.metamodels:
                raise BQLError(bdb, 'No such metamodel: %s' %
                    (repr(new_metamodel_type_name),))
            new_metamodel_type = bdb.metamodels[new_metamodel_type_name]

            new_metamodel_name = phrase.newname
            if core.bayesdb_has_generator(bdb, population_id, new_metamodel_name):
                raise BQLError(
                    bdb, 'Name already defined as generator: %s' %
                    (repr(new_metamodel_name),))

            bdb.execute('''CREATE GENERATOR %s
            FOR %s
            USING %s
            ''' % (new_metamodel_name,
                population_name,
                new_metamodel_type_name))

            new_generator_id =core.bayesdb_get_generator(
                bdb, population_id, new_metamodel_name)
            new_metamodel = core.bayesdb_generator_metamodel(
                bdb, new_generator_id)

            # Now that we have a created metamodel,
            # Convert loom's views to those of cgpm
            new_metamodel.initialize_models(
                    bdb, new_generator_id,
                    range(
                        current_metamodel._get_num_models(
                            bdb, current_generator_id)))
            current_metamodel.populate_cgpm_engine(
                    bdb, current_generator_id,
                    new_metamodel._engine(bdb, new_generator_id))

        return empty_cursor(bdb)

    if isinstance(phrase, ast.InitModels):
        if not core.bayesdb_has_generator(bdb, None, phrase.generator):
            raise BQLError(bdb, 'No such generator: %s' %
                (phrase.generator,))
        generator_id = core.bayesdb_get_generator(bdb, None, phrase.generator)
        modelnos = range(phrase.nmodels)

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
            metamodel.initialize_models(bdb, generator_id, modelnos)
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
        if not core.bayesdb_has_generator(bdb, None, phrase.generator):
            raise BQLError(bdb, 'No such generator: %s' %
                (phrase.generator,))
        generator_id = core.bayesdb_get_generator(bdb, None, phrase.generator)
        metamodel = core.bayesdb_generator_metamodel(bdb, generator_id)
        # XXX Should allow parameters for iterations and ckpt/iter.
        metamodel.analyze_models(bdb, generator_id,
            modelnos=phrase.modelnos,
            iterations=phrase.iterations,
            max_seconds=phrase.seconds,
            ckpt_iterations=phrase.ckpt_iterations,
            ckpt_seconds=phrase.ckpt_seconds,
            program=phrase.program)
        return empty_cursor(bdb)

    if isinstance(phrase, ast.DropModels):
        with bdb.savepoint():
            generator_id = core.bayesdb_get_generator(
                bdb, None, phrase.generator)
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

    if isinstance(phrase, ast.Regress):
        # Retrieve the population.
        if not core.bayesdb_has_population(bdb, phrase.population):
            raise BQLError(bdb, 'No such population: %r' % (phrase.population,))
        population_id = core.bayesdb_get_population(bdb, phrase.population)
        # Retrieve the metamodel.
        generator_id = None
        if phrase.metamodel:
            if not core.bayesdb_has_generator(bdb, population_id,
                    phrase.metamodel):
                raise BQLError(bdb,
                    'No such metamodel: %r' % (phrase.population,))
            generator_id = core.bayesdb_get_generator(
                bdb, population_id, phrase.metamodel)
        # Retrieve the target variable.
        if not core.bayesdb_has_variable(
                bdb, population_id, None, phrase.target):
            raise BQLError(bdb, 'No such variable: %r' % (phrase.target,))
        colno_target = core.bayesdb_variable_number(
            bdb, population_id, None, phrase.target)
        if core.bayesdb_variable_stattype(bdb, population_id, colno_target) != \
                'numerical':
            raise BQLError(bdb,
                'Target variable is not numerical: %r' % (phrase.target,))
        # Build the given variables.
        if any(isinstance(col, ast.SelColAll) for col in phrase.givens):
            # Using * is not allowed to be mixed with other variables.
            if len(phrase.givens) > 1:
                raise BQLError(bdb, 'Cannot use (*) with other givens.')
            colno_givens = core.bayesdb_variable_numbers(
                bdb, population_id, None)
        else:
            if any(isinstance(col, ast.SelColSub) for col in phrase.givens):
                # Subexpression needs special compiling.
                out = compiler.Output(n_numpar, nampar_map, bindings)
                bql_compiler = compiler.BQLCompiler_None()
                givens = compiler.expand_select_columns(
                    bdb, phrase.givens, True, bql_compiler, out)
            else:
                givens = phrase.givens
            colno_givens = [
                core.bayesdb_variable_number(
                    bdb, population_id, None, given.expression.column)
                for given in givens
            ]
        # Build the arguments to bqlfn.bayesdb_simulate.
        colno_givens_unique = set(
            colno for colno in colno_givens if colno!= colno_target
        )
        if len(colno_givens_unique) == 0:
            raise BQLError(bdb, 'No matching given columns.')
        constraints = []
        colnos = [colno_target] + list(colno_givens_unique)
        nsamp = 100 if phrase.nsamp is None else phrase.nsamp.value.value
        modelnos = None if phrase.modelnos is None else str(phrase.modelnos)
        rows = bqlfn.bayesdb_simulate(
            bdb, population_id, generator_id, modelnos, constraints,
            colnos, numpredictions=nsamp)
        # Retrieve the stattypes.
        stattypes = [
            core.bayesdb_variable_stattype(bdb, population_id, colno_given)
            for colno_given in colno_givens_unique
        ]
        # Separate the target values from the given values.
        target_values = [row[0] for row in rows]
        given_values = [row[1:] for row in rows]
        given_names = [
            core.bayesdb_variable_name(bdb, population_id, given)
            for given in colno_givens_unique
        ]
        # Compute the coefficients. The import to regress_ols is here since the
        # feature depends on pandas + sklearn, so avoid module-wide import.
        from bayeslite.regress import regress_ols
        coefficients = regress_ols(
            target_values, given_values, given_names, stattypes)
        # Store the results in a winder.
        temptable = bdb.temp_table_name()
        qtt = sqlite3_quote_name(temptable)
        out = compiler.Output(0, {}, {})
        out.winder('''
            CREATE TEMP TABLE %s (variable TEXT, coefficient REAL);
        ''' % (qtt,), ())
        for variable, coef in coefficients:
            out.winder('''
                INSERT INTO %s VALUES (?, ?)
            ''' % (qtt), (variable, coef,))
        out.write('SELECT * FROM %s ORDER BY variable' % (qtt,))
        out.unwinder('DROP TABLE %s' % (qtt,), ())
        winders, unwinders = out.getwindings()
        return execute_wound(
            bdb, winders, unwinders, out.getvalue(), out.getbindings())

    assert False                # XXX

def _create_population(bdb, phrase):
    if core.bayesdb_has_population(bdb, phrase.name):
        if phrase.ifnotexists:
            return
        else:
            raise BQLError(bdb, 'Name already defined as population: %r' %
                (phrase.name,))

    # Make sure the bayesdb_column table knows all the columns of the
    # underlying table.
    core.bayesdb_table_guarantee_columns(bdb, phrase.table)

    # Retrieve all columns from the base table. The user is required to provide
    # a strategy for each single variable, either MODEL, IGNORE, or GUESS.
    base_table_columns = core.bayesdb_table_column_names(bdb, phrase.table)
    seen_columns = []

    # Create the population record and get the assigned id.
    bdb.sql_execute('''
        INSERT INTO bayesdb_population (name, tabname) VALUES (?, ?)
    ''', (phrase.name, phrase.table))
    population_id = core.bayesdb_get_population(bdb, phrase.name)

    # Extract the population column names and stattypes as pairs.
    pop_model_vars = list(itertools.chain.from_iterable(
        [[(name, s.stattype) for name in s.names]
        for s in phrase.schema if isinstance(s, ast.PopModelVars)]))

    # Extract the ignored columns.
    pop_ignore_vars = list(itertools.chain.from_iterable(
        [[(name, 'ignore') for name in s.names]
        for s in phrase.schema if isinstance(s, ast.PopIgnoreVars)]))

    # Extract the columns to guess.
    pop_guess = list(itertools.chain.from_iterable(
        [s.names for s in phrase.schema if isinstance(s, ast.PopGuessVars)]))
    if '*' in pop_guess:
        # Do not allow * to coincide with other variables.
        if len(pop_guess) > 1:
            raise BQLError(
                bdb, 'Cannot use wildcard GUESS with variables names: %r'
                % (pop_guess, ))
        # Retrieve all variables in the base table.
        avoid = set(casefold(t[0]) for t in pop_model_vars + pop_ignore_vars)
        pop_guess = [t for t in base_table_columns if casefold(t) not in avoid]
    # Perform the guessing.
    if pop_guess:
        qt = sqlite3_quote_name(phrase.table)
        qcns = ','.join(map(sqlite3_quote_name, pop_guess))
        cursor = bdb.sql_execute('SELECT %s FROM %s' % (qcns, qt))
        rows = cursor.fetchall()
        # XXX This function returns a stattype called `key`, which we will add
        # to the pop_ignore_vars.
        pop_guess_stattypes = bayesdb_guess_stattypes(pop_guess, rows)
        pop_guess_vars = zip(pop_guess, [st[0] for st in pop_guess_stattypes])
        migrate = [(col, st) for col, st in pop_guess_vars if st=='key']
        for col, st in migrate:
            pop_guess_vars.remove((col, st))
            pop_ignore_vars.append((col, 'ignore'))
    else:
        pop_guess_vars = []

    # Ensure no string-valued variables are being modeled as numerical.
    numerical_string_vars = [
        var for var, stattype in pop_model_vars
        if stattype == 'numerical'
            and _column_contains_string(bdb, phrase.table, var)
    ]
    if numerical_string_vars:
        raise BQLError(bdb,
            'Column(s) with string values modeled as numerical: %r'
            % (numerical_string_vars, ))

    # Pool all the variables and statistical types together.
    pop_all_vars = pop_model_vars + pop_ignore_vars + pop_guess_vars

    # Check that everyone in the population is modeled.
    # `known` contains all the variables for which a policy is known.
    known = [casefold(t[0]) for t in pop_all_vars]
    not_found = [t for t in base_table_columns if casefold(t) not in known]
    if not_found:
        raise BQLError(
            bdb, 'Cannot determine a modeling policy for variables: %r'
            % (not_found, ))

    # Check
    # - for duplicates,
    # - for nonexistent columns,
    # - for invalid statistical types.
    seen_variables = set()
    duplicates = set()
    missing = set()
    invalid = set()
    stattype_sql = '''
        SELECT COUNT(*) FROM bayesdb_stattype WHERE name = :stattype
    '''
    for nm, st in pop_all_vars:
        name = casefold(nm)
        stattype = casefold(st)
        if name in seen_variables:
            duplicates.add(name)
            continue
        if not core.bayesdb_table_has_column(bdb, phrase.table, nm):
            missing.add(name)
            continue
        cursor = bdb.sql_execute(stattype_sql, {'stattype': stattype})
        if cursor_value(cursor) == 0 and stattype != 'ignore':
            invalid.add(stattype)
            continue
        seen_variables.add(nm)
    # XXX Would be nice to report these simultaneously.
    if missing:
        raise BQLError(bdb, 'No such columns in table %r: %r' %
            (phrase.table, list(missing)))
    if duplicates:
        raise BQLError(bdb, 'Duplicate column names: %r' % (list(duplicates),))
    if invalid:
        raise BQLError(bdb, 'Invalid statistical types: %r' % (list(invalid),))

    # Insert variable records.
    for nm, st in pop_all_vars:
        name = casefold(nm)
        stattype = casefold(st)
        if stattype == 'ignore':
            continue
        core.bayesdb_add_variable(bdb, population_id, name, stattype)

def _column_contains_string(bdb, table, column):
    qt = sqlite3_quote_name(table)
    qc = sqlite3_quote_name(column)
    rows = bdb.sql_execute('SELECT %s FROM %s' % (qc, qt))
    return any(isinstance(r[0], unicode) for r in rows)

def rename_table(bdb, old, new):
    assert core.bayesdb_has_table(bdb, old)
    assert not core.bayesdb_has_table(bdb, new)
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
    # Update bayesdb_population to use the new name.
    update_populations_sql = '''
        UPDATE bayesdb_population SET tabname = ? WHERE tabname = ?
    '''
    bdb.sql_execute(update_populations_sql, (new, old))

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
