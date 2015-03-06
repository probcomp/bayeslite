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
    if isinstance(phrase, ast.DropTable):
        ifexists = 'IF EXISTS ' if phrase.ifexists else ''
        qt = sqlite3_quote_name(phrase.name)
        return bdb.sql_execute('DROP TABLE %s%s' % (ifexists, qt))
    if isinstance(phrase, ast.DropBtable):
        with bdb.savepoint():
            if core.bayesdb_table_exists(bdb, phrase.name):
                table_id = core.bayesdb_table_id(bdb, phrase.name)
                bdb.sql_execute('''
                    DELETE FROM bayesdb_table_column WHERE table_id = ?
                ''', (table_id,))
                bdb.sql_execute('''
                    DELETE FROM bayesdb_value_map WHERE table_id = ?
                ''', (table_id,))
                bdb.sql_execute('DELETE FROM bayesdb_model WHERE table_id = ?',
                    (table_id,))
                bdb.sql_execute('DELETE FROM bayesdb_table WHERE id = ?',
                    (table_id,))
                qt = sqlite3_quote_name(phrase.name)
                bdb.sql_execute('DROP TABLE %s' % (qt,))
            elif not phrase.ifexists:
                # XXX More specific exception.
                raise ValueError('No such btable: %s' % (phrase.name,))
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
            # XXX Move this somewhere appropriate.
            qn = sqlite3_quote_name(phrase.name)
            btable_name = phrase.simulation.btable_name
            qbtn = sqlite3_quote_name(btable_name)
            column_names = phrase.simulation.columns
            qcns = map(sqlite3_quote_name, column_names)
            columns = list(bdb.sql_execute('PRAGMA table_info(%s)' % (qbtn,)))
            if len(columns) < 1 or \
               not core.bayesdb_table_exists(bdb, btable_name):
                raise ValueError('No such btable: %s' % (btable_name,))
            table_id = core.bayesdb_table_id(bdb, btable_name)
            column_types = dict((casefold(n), t)
                for _i, n, t, _nn, _dv, _pk in columns)
            for column_name in column_names:
                if casefold(column_name) not in column_types:
                    raise ValueError('No such column in btable %s: %s' %
                        (btable_name, column_name))
            for column_name, exp in phrase.simulation.constraints:
                if casefold(column_name) not in column_types:
                    raise ValueError('No such column in btable %s: %s' %
                        (btable_name, column_name))
            # XXX Move to compiler.py.
            out = compiler.Output(n_numpar, nampar_map, bindings)
            nobql = compiler.BQLCompiler_None()
            out.write('SELECT ')
            with compiler.compiling_paren(bdb, out, 'CAST(', ' AS INTEGER)'):
                compiler.compile_expression(bdb, phrase.simulation.nsamples,
                    nobql, out)
            for _column_name, exp in phrase.simulation.constraints:
                out.write(', ')
                compiler.compile_expression(bdb, exp, nobql, out)
            cursor = list(bdb.sql_execute(out.getvalue(), out.getbindings()))
            assert len(cursor) == 1
            nsamples = cursor[0][0]
            assert isinstance(nsamples, int)
            constraints = \
                [(core.bayesdb_column_number(bdb, table_id, name), value)
                    for (name, exp), value in
                        zip(phrase.simulation.constraints, cursor[0][1:])]
            colnos = [core.bayesdb_column_number(bdb, table_id, name)
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
            for row in bqlfn.bayesdb_simulate(bdb, table_id, constraints,
                    colnos, numpredictions=nsamples):
                bdb.sql_execute(insert_sql, row)
        return empty_cursor(bdb)
    if isinstance(phrase, ast.CreateBtableCSV):
        # XXX Codebook?
        import_csv.bayesdb_import_csv_file(bdb, phrase.name, phrase.file,
            ifnotexists=phrase.ifnotexists)
        return empty_cursor(bdb)
    if isinstance(phrase, ast.InitModels):
        if not core.bayesdb_table_exists(bdb, phrase.btable):
            raise ValueError('No such btable: %s' % (phrase.btable,))
        table_id = core.bayesdb_table_id(bdb, phrase.btable)
        nmodels = phrase.nmodels
        config = phrase.config
        bqlfn.bayesdb_models_initialize(bdb, table_id, range(nmodels), config,
            ifnotexists=phrase.ifnotexists)
        return empty_cursor(bdb)
    if isinstance(phrase, ast.AnalyzeModels):
        if not core.bayesdb_table_exists(bdb, phrase.btable):
            raise ValueError('No such btable: %s' % (phrase.btable,))
        table_id = core.bayesdb_table_id(bdb, phrase.btable)
        modelnos = phrase.modelnos
        iterations = phrase.iterations
        seconds = phrase.seconds
        wait = phrase.wait
        if not wait: # XXX
            raise NotImplementedError("Background ANALYZE not yet supported."
                " Please use 'WAIT' keyword.")
        bqlfn.bayesdb_models_analyze(bdb, table_id, modelnos=modelnos,
            iterations=iterations, max_seconds=seconds)
        return empty_cursor(bdb)
    if isinstance(phrase, ast.DropModels):
        with bdb.savepoint():
            if not core.bayesdb_table_exists(bdb, phrase.btable):
                raise ValueError('No such btable: %s' % (phrase.btable,))
            table_id = core.bayesdb_table_id(bdb, phrase.btable)
            core.bayesdb_drop_models(bdb, table_id, phrase.modelnos)
            return empty_cursor(bdb)
    if isinstance(phrase, ast.RenameBtable):
        # XXX Move this to core.py?
        with bdb.savepoint():
            if not core.bayesdb_table_exists(bdb, phrase.oldname):
                # XXX More specific exception.
                raise ValueError('No such table: %s' % (phrase.oldname,))
            qto = sqlite3_quote_name(phrase.oldname)
            qtn = sqlite3_quote_name(phrase.newname)
            bdb.sql_execute('ALTER TABLE %s RENAME TO %s' % (qto, qtn))
            bdb.sql_execute('UPDATE bayesdb_table SET name = ? WHERE name = ?',
                (phrase.newname, phrase.oldname))
            return empty_cursor(bdb)
    assert False                # XXX

# XXX Temporary kludge until we get BQL cursors proper, with, e.g.,
# declared modelled column types in cursor.description.  We go through
# sqlite3 directly to avoid cluttering the trace.
def empty_cursor(bdb):
    cursor = bdb.sqlite3.cursor()
    cursor.execute('')
    return cursor
