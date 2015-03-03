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

import StringIO
import contextlib

import bayeslite.ast as ast
import bayeslite.core as core
import bayeslite.import_csv as import_csv

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
        #
        # XXX OOPS!  If we return a lazy iterable from this, iteration
        # will happen outside the transaction.  Hmm.  Maybe we'll just
        # require the user to enact another transaction in that case.
        with bdb.savepoint():
            out = Output(n_numpar, nampar_map, bindings)
            compile_query(bdb, phrase, out)
            return bdb.sql_execute(out.getvalue(), out.getbindings())
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
                bdb.sql_execute('DELETE FROM bayesdb_table WHERE id = ?',
                    (table_id,))
                qt = sqlite3_quote_name(phrase.name)
                bdb.sql_execute('DROP TABLE %s' % (qt,))
            elif not phrase.ifexists:
                # XXX More specific exception.
                raise ValueError('No such btable: %s' % (phrase.name,))
        return []
    if isinstance(phrase, ast.CreateTableAs):
        assert ast.is_query(phrase.query)
        with bdb.savepoint():
            out = Output(n_numpar, nampar_map, bindings)
            qt = sqlite3_quote_name(phrase.name)
            temp = 'TEMP ' if phrase.temp else ''
            ifnotexists = 'IF NOT EXISTS ' if phrase.ifnotexists else ''
            out.write('CREATE %sTABLE %s%s AS ' % (temp, ifnotexists, qt))
            compile_query(bdb, phrase.query, out)
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
            out = Output(n_numpar, nampar_map, bindings)
            nobql = BQLCompiler_None()
            out.write('SELECT ')
            with compiling_paren(bdb, out, 'CAST(', ' AS INTEGER)'):
                compile_expression(bdb, phrase.simulation.nsamples, nobql, out)
            for _column_name, exp in phrase.simulation.constraints:
                out.write(', ')
                compile_expression(bdb, exp, nobql, out)
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
                 ','.join('%s %s' % (qcn, column_types[column_name])
                            for qcn, column_name in zip(qcns, column_names))))
            insert_sql = '''
                INSERT INTO %s (%s) VALUES (%s)
            ''' % (qn, ','.join(qcns), ','.join('?' for qcn in qcns))
            for row in core.bayesdb_simulate(bdb, table_id, constraints,
                    colnos, numpredictions=nsamples):
                bdb.sql_execute(insert_sql, row)
        return []
    if isinstance(phrase, ast.CreateBtableCSV):
        # XXX Codebook?
        import_csv.bayesdb_import_csv_file(bdb, phrase.name, phrase.file,
            ifnotexists=phrase.ifnotexists)
        return []
    if isinstance(phrase, ast.InitModels):
        if not core.bayesdb_table_exists(bdb, phrase.btable):
            raise ValueError('No such btable: %s' % (phrase.btable,))
        table_id = core.bayesdb_table_id(bdb, phrase.btable)
        nmodels = phrase.nmodels
        config = phrase.config
        core.bayesdb_models_initialize(bdb, table_id, range(nmodels), config,
            ifnotexists=phrase.ifnotexists)
        return []
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
        core.bayesdb_models_analyze(bdb, table_id, modelnos=modelnos,
            iterations=iterations, max_seconds=seconds)
        return []
    if isinstance(phrase, ast.DropModels):
        with bdb.savepoint():
            if not core.bayesdb_table_exists(bdb, phrase.btable):
                raise ValueError('No such btable: %s' % (phrase.btable,))
            table_id = core.bayesdb_table_id(bdb, phrase.btable)
            core.bayesdb_models_drop(bdb, table_id, phrase.modelnos)
            return []
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
            return []
    assert False                # XXX

# Output: Compiled SQL output accumulator.  Like StringIO.StringIO()
# for write with getvalue, but also does bookkeeping for parameters
# and subqueries.
class Output(object):
    def __init__(self, n_numpar, nampar_map, bindings):
        self.stringio = StringIO.StringIO()
        # Below, `number' means 1-based, and `index' means 0-based.  n
        # is a source language number; m, an output sqlite3 number; i,
        # an input index passed by the caller, and j, an output index
        # of the tuple we pass to sqlite3.
        self.n_numpar = n_numpar        # number of numbered parameters
        self.nampar_map = nampar_map    # map of param name -> param number
        self.bindings = bindings        # map of input index -> value
        self.renumber = {}              # map of input number -> output number
        self.select = []                # map of output index -> input index

    def subquery(self):
        '''Return an output accumulator for a subquery.'''
        return Output(self.n_numpar, self.nampar_map, self.bindings)

    def getvalue(self):
        '''Return the accumulated output.'''
        return self.stringio.getvalue()

    def getbindings(self):
        '''Return a selection of bindings fit for the accumulated output.

        If there were subqueries, or if this is accumulating output
        for a subquery, this may not use all bindings.
        '''
        if isinstance(self.bindings, dict):
            # User supplied named bindings.
            # - Grow a set of parameters we don't expect (unknown).
            # - Shrink a set of parameters we do expect (missing).
            # - Fill a list for the n_numpar parameters positionally.
            unknown = set([])
            missing = set(self.nampar_map)
            bindings_list = [None] * self.n_numpar

            # For each binding, either add it to the unknown set or
            # (a) remove it from the missing set, (b) use nampar_map
            # to find its user-supplied input position, and (c) use
            # renumber to find its output position for passage to
            # sqlite3.
            for name in self.bindings:
                name_folded = casefold(name)
                if name_folded not in self.nampar_map:
                    unknown.add(name)
                    continue
                missing.remove(name_folded)
                n = self.nampar_map[name_folded]
                m = self.renumber[n]
                j = m - 1
                assert bindings_list[j] is None
                bindings_list[j] = self.bindings[name]

            # Make sure we saw all parameters we expected and none we
            # didn't expect.
            if 0 < len(missing):
                raise ValueError('Missing parameter bindings: %s' % (missing,))
            if 0 < len(unknown):
                raise ValueError('Unknown parameter bindings: %s' % (unknown,))

            # If the query contained any numbered parameters, which
            # will manifest as higher values of n_numpar without more
            # entries in nampar_map, we can't execute the query.
            if len(self.bindings) < self.n_numpar:
                missing_numbers = set(range(1, self.n_numpar + 1))
                for name in self.bindings:
                    missing_numbers.remove(self.nampar_map[casefold(name)])
                raise ValueError('Missing parameter numbers: %s' %
                    (missing_numbers,))

            # All set.
            return bindings_list

        elif isinstance(self.bindings, tuple) or \
             isinstance(self.bindings, list):
            # User supplied numbered bindings.  Make sure there aren't
            # too few or too many, and then select a list of the ones
            # we want.
            if len(self.bindings) < self.n_numpar:
                raise ValueError('Too few parameter bindings: %d < %d' %
                    (len(self.bindings), self.n_numpar))
            if len(self.bindings) > self.n_numpar:
                raise ValueError('Too many parameter bindings: %d > %d' %
                    (len(self.bindings), self.n_numpar))
            assert len(self.select) <= self.n_numpar
            return [self.bindings[j] for j in self.select]

        else:
            # User supplied bindings we didn't understand.
            raise TypeError('Invalid query bindings: %s' % (self.bindings,))

    def write(self, text):
        '''Accumulate TEXT in the output of getvalue().'''
        self.stringio.write(text)

    def write_numpar(self, n):
        '''Accumulate a reference to the parameter numbered N.'''
        assert 0 < n
        assert n <= self.n_numpar
        # The input index i is the input number n minus one.
        i = n - 1
        # Has this parameter already been used?
        m = None
        if n in self.renumber:
            # Yes: its output number is renumber[n].
            m = self.renumber[n]
        else:
            # No: its output index is the number of bindings we've
            # selected so far; append its input index to the list of
            # selected indices and remember its output number in
            # renumber.
            j = len(self.select)
            m = j + 1
            self.select.append(i)
            self.renumber[n] = m
        self.write('?%d' % (m,))

    def write_nampar(self, name, n):
        '''Accumulate a reference to the parameter named NAME, numbered N.'''
        assert 0 < n
        assert n <= self.n_numpar
        # Just treat it as if it were a numbered parameter; it is the
        # parser's job to map between numbered and named parameters.
        assert self.nampar_map[name] == n
        self.write_numpar(n)

def compile_query(bdb, query, out):
    if isinstance(query, ast.Select):
        compile_select(bdb, query, out)
    elif isinstance(query, ast.EstCols):
        compile_estcols(bdb, query, out)
    elif isinstance(query, ast.EstPairCols):
        compile_estpaircols(bdb, query, out)
    elif isinstance(query, ast.EstPairRow):
        compile_estpairrow(bdb, query, out)
    else:
        assert False        # XXX

def compile_subquery(bdb, query, _bql_compiler, out):
    # XXX Do something with the BQL compiler so we can refer to
    # BQL-related quantities in the subquery?
    with compiling_paren(bdb, out, '(', ')'):
        compile_query(bdb, query, out)

def compile_select(bdb, select, out):
    assert isinstance(select, ast.Select)
    out.write('SELECT')
    if select.quantifier == ast.SELQUANT_DISTINCT:
        out.write(' DISTINCT')
    else:
        assert select.quantifier == ast.SELQUANT_ALL
    compile_select_columns(bdb, select, out)
    if select.tables is not None:
        assert 0 < len(select.tables)
        compile_select_tables(bdb, select, out)
    if select.condition is not None:
        out.write(' WHERE ')
        compile_1row_expression(bdb, select.condition, select, out)
    if select.group is not None:
        assert 0 < len(select.group)
        first = True
        for key in select.group:
            if first:
                out.write(' GROUP BY ')
                first = False
            else:
                out.write(', ')
            compile_1row_expression(bdb, key, select, out)
    if select.order is not None:
        assert 0 < len(select.order)
        first = True
        for order in select.order:
            if first:
                out.write(' ORDER BY ')
                first = False
            else:
                out.write(', ')
            compile_1row_expression(bdb, order.expression, select, out)
            if order.sense == ast.ORD_ASC:
                pass
            elif order.sense == ast.ORD_DESC:
                out.write(' DESC')
            else:
                assert False    # XXX
    if select.limit is not None:
        out.write(' LIMIT ')
        compile_1row_expression(bdb, select.limit.limit, select, out)
        if select.limit.offset is not None:
            out.write(' OFFSET ')
            compile_1row_expression(bdb, select.limit.offset, select, out)

def compile_select_columns(bdb, select, out):
    first = True
    for selcol in select.columns:
        if first:
            out.write(' ')
            first = False
        else:
            out.write(', ')
        compile_select_column(bdb, selcol, select, out)

def compile_select_column(bdb, selcol, select, out):
    if isinstance(selcol, ast.SelColAll):
        if selcol.table is not None:
            compile_table_name(bdb, selcol.table, out)
            out.write('.')
        out.write('*')
    elif isinstance(selcol, ast.SelColExp):
        bql_compiler = BQLCompiler_1Row(select)
        compile_expression(bdb, selcol.expression, bql_compiler, out)
        if selcol.name is not None:
            out.write(' AS ')
            compile_name(bdb, selcol.name, out)
    else:
        assert False            # XXX

def compile_select_tables(bdb, select, out):
    first = True
    for seltab in select.tables:
        if first:
            out.write(' FROM ')
            first = False
        else:
            out.write(', ')
        compile_select_table(bdb, seltab.table, out)
        if seltab.name is not None:
            out.write(' AS ')
            compile_name(bdb, seltab.name, out)

def compile_select_table(bdb, table, out):
    if ast.is_query(table):
        bql_compiler = None     # XXX
        compile_subquery(bdb, table, bql_compiler, out)
    elif isinstance(table, str): # XXX name
        compile_table_name(bdb, table, out)
    else:
        assert False            # XXX

# XXX Use context to determine whether to yield column names or
# numbers, so that top-level queries yield names, but, e.g.,
# subqueries in SIMILARITY TO 0 WITH RESPECT TO (...) yield numbers
# since that's what bql_row_similarity wants.
#
# XXX Use query parameters, not quotation.
def compile_estcols(bdb, estcols, out):
    assert isinstance(estcols, ast.EstCols)
    # XXX UH OH!  This will have the effect of shadowing names.  We
    # need an alpha-renaming pass.
    if not core.bayesdb_table_exists(bdb, estcols.btable):
        raise ValueError('No such btable: %s' % (estcols.btable,))
    out.write('SELECT name FROM bayesdb_table_column WHERE table_id = %d' %
        (core.bayesdb_table_id(bdb, estcols.btable),))
    colno_exp = 'colno'         # XXX
    if estcols.condition is not None:
        out.write(' AND ')
        compile_1col_expression(bdb, estcols.condition, estcols, colno_exp,
            out)
    if estcols.order is not None:
        assert 0 < len(estcols.order)
        first = True
        for order in estcols.order:
            if first:
                out.write(' ORDER BY ')
                first = False
            else:
                out.write(', ')
            compile_1col_expression(bdb, order.expression, estcols, colno_exp,
                out)
            if order.sense == ast.ORD_ASC:
                pass
            elif order.sense == ast.ORD_DESC:
                out.write(' DESC')
            else:
                assert False    # XXX
    if estcols.limit is not None:
        out.write(' LIMIT ')
        compile_1col_expression(bdb, estcols.limit.limit, estcols, colno_exp,
            out)
        if estcols.limit.offset is not None:
            out.write(' OFFSET ')
            compile_1col_expression(bdb, estcols.limit.offset, estcols,
                colno_exp, out)

def compile_estpaircols(bdb, estpaircols, out):
    assert isinstance(estpaircols, ast.EstPairCols)
    colno0_exp = 'c0.colno'     # XXX
    colno1_exp = 'c1.colno'     # XXX
    if not core.bayesdb_table_exists(bdb, estpaircols.btable):
        raise ValueError('No such btable: %s' % (estpaircols.btable,))
    table_id = core.bayesdb_table_id(bdb, estpaircols.btable)
    out.write('SELECT %d AS table_id, c0.name AS name0, c1.name AS name1, ' %
        (table_id,))
    compile_2col_expression(bdb, estpaircols.expression, estpaircols,
        colno0_exp, colno1_exp, out)
    out.write(' AS value')
    out.write(' FROM bayesdb_table_column AS c0, bayesdb_table_column AS c1')
    out.write(' WHERE c0.table_id = %d AND c1.table_id = %d' %
        (table_id, table_id))
    if estpaircols.condition is not None:
        out.write(' AND ')
        compile_2col_expression(bdb, estpaircols.condition, estpaircols,
            colno0_exp, colno1_exp, out)
    if estpaircols.order is not None:
        assert 0 < len(estpaircols.order)
        first = True
        for order in estpaircols.order:
            if first:
                out.write(' ORDER BY ')
                first = False
            else:
                out.write(', ')
            compile_2col_expression(bdb, order.expression, estpaircols,
                colno0_exp, colno1_exp, out)
            if order.sense == ast.ORD_ASC:
                pass
            elif order.sense == ast.ORD_DESC:
                out.write(' DESC')
            else:
                assert False    # XXX
    if estpaircols.limit is not None:
        out.write(' LIMIT ')
        compile_2col_expression(bdb, estpaircols.limit.limit, estpaircols,
            colno0_exp, colno1_exp, out)
        if estpaircols.limit.offset is not None:
            out.write(' OFFSET ')
            compile_2col_expression(bdb, estpaircols.limit.offset, estpaircols,
                colno0_exp, colno1_exp, out)

def compile_estpairrow(bdb, estpairrow, out):
    assert isinstance(estpairrow, ast.EstPairRow)
    table_name = estpairrow.btable
    rowid0_exp = 'r0._rowid_'
    rowid1_exp = 'r1._rowid_'
    out.write('SELECT %s AS rowid0, %s AS rowid1, ' % (rowid0_exp, rowid1_exp))
    compile_2row_expression(bdb, estpairrow.expression, estpairrow,
        rowid0_exp, rowid1_exp, out)
    out.write(' AS value')
    out.write(' FROM %s AS r0, %s AS r1' % (table_name, table_name))
    if estpairrow.condition is not None:
        out.write(' WHERE ')
        compile_2row_expression(bdb, estpairrow.condition, estpairrow,
            rowid0_exp, rowid1_exp, out)
    if estpairrow.order is not None:
        assert 0 < len(estpairrow.order)
        first = True
        for order in estpairrow.order:
            if first:
                out.write(' ORDER BY ')
                first = False
            else:
                out.write(', ')
            compile_2row_expression(bdb, order.expression, estpairrow,
                rowid0_exp, rowid1_exp, out)
            if order.sense == ast.ORD_ASC:
                pass
            elif order.sense == ast.ORD_DESC:
                out.write(' DESC')
            else:
                assert False    # XXX
    if estpairrow.limit is not None:
        out.write(' LIMIT ')
        compile_2row_expression(bdb, estpairrow.limit.limit, estpairrow,
            rowid0_exp, rowid1_exp, out)
        if estpairrow.limit.offset is not None:
            out.write(' OFFSET ')
            compile_2row_expression(bdb, estpairrow.limit.offset, estpairrow,
                rowid0_exp, rowid1_exp, out)

class BQLCompiler_None(object):
    def compile_bql(self, bdb, bql, out):
        # XXX Report source location.
        raise ValueError('Invalid context for BQL!')

class BQLCompiler_1Row(object):
    def __init__(self, ctx):
        assert isinstance(ctx, ast.Select)
        self.ctx = ctx

    def compile_bql(self, bdb, bql, out):
        assert ast.is_bql(bql)
        # XXX Can some other context determine the table, so that we can
        # do selects on multiple tables at once?
        if self.ctx.tables is None:
            raise ValueError('BQL row query without table: %s' % (self.ctx,))
        if len(self.ctx.tables) != 1:
            assert 1 < len(self.ctx.tables)
            raise ValueError('BQL row query with >1 table: %s' % (self.ctx,))
        if not isinstance(self.ctx.tables[0].table, str): # XXX name
            raise ValueError('Subquery in BQL row query: %s' % (self.ctx,))
        if not core.bayesdb_table_exists(bdb, self.ctx.tables[0].table):
            raise ValueError('No such btable: %s' %
                (self.ctx.tables[0].table,))
        table_id = core.bayesdb_table_id(bdb, self.ctx.tables[0].table)
        rowid_col = '_rowid_'   # XXX Don't hard-code this.
        if isinstance(bql, ast.ExpBQLPredProb):
            if bql.column is None:
                raise ValueError('Predictive probability at row needs column.')
            colno = core.bayesdb_column_number(bdb, table_id, bql.column)
            out.write('bql_row_column_predictive_probability(%s, %s, %s)' %
                (table_id, rowid_col, colno))
        elif isinstance(bql, ast.ExpBQLProb):
            # XXX Why is this independent of the row?  Can't we
            # condition on the values of the row?  Maybe need another
            # notation; PROBABILITY OF X = V GIVEN ...?
            if bql.column is None:
                raise ValueError('Probability of value at row needs column.')
            colno = core.bayesdb_column_number(bdb, table_id, bql.column)
            out.write('bql_column_value_probability(%s, %s, ' %
                (table_id, colno))
            compile_expression(bdb, bql.value, self, out)
            out.write(')')
        elif isinstance(bql, ast.ExpBQLTyp):
            if bql.column is None:
                out.write('bql_row_typicality(%s, _rowid_)' % (table_id,))
            else:
                colno = core.bayesdb_column_number(bdb, table_id, bql.column)
                out.write('bql_column_typicality(%s, %s)' % (table_id, colno))
        elif isinstance(bql, ast.ExpBQLSim):
            if bql.rowid is None:
                raise ValueError('Similarity as 1-row function needs row.')
            out.write('bql_row_similarity(%s, _rowid_, ' % (table_id,))
            compile_expression(bdb, bql.rowid, self, out)
            if len(bql.column_lists) == 1 and \
               isinstance(bql.column_lists[0], ast.ColListAll):
                # We'll likely run up against SQLite's limit on the
                # number of arguments in this case.  Instead, let
                # bql_row_similarity find the columns.
                pass
            else:
                out.write(', ')
                compile_column_lists(bdb, table_id, bql.column_lists, self,
                    out)
            out.write(')')
        elif isinstance(bql, ast.ExpBQLDepProb):
            compile_bql_2col_2(bdb, table_id,
                'bql_column_dependence_probability',
                'Dependence probability', None, bql, self, out)
        elif isinstance(bql, ast.ExpBQLMutInf):
            compile_bql_2col_2(bdb, table_id,
                'bql_column_mutual_information',
                'Mutual information', compile_mutinf_extra, bql, self, out)
        elif isinstance(bql, ast.ExpBQLCorrel):
            compile_bql_2col_2(bdb, table_id,
                'bql_column_correlation',
                'Column correlation', None, bql, self, out)
        elif isinstance(bql, ast.ExpBQLInfer):
            assert bql.column is not None
            colno = core.bayesdb_column_number(bdb, table_id, bql.column)
            out.write('bql_infer(%d, %d, _rowid_, ' % (table_id, colno))
            compile_column_name(bdb, self.ctx.tables[0].table, bql.column, out)
            out.write(', ')
            compile_expression(bdb, bql.confidence, self, out)
            out.write(')')
        else:
            assert False        # XXX

class BQLCompiler_2Row(object):
    def __init__(self, ctx, rowid0_exp, rowid1_exp):
        assert isinstance(ctx, ast.EstPairRow)
        assert isinstance(rowid0_exp, str)
        assert isinstance(rowid1_exp, str)
        self.ctx = ctx
        self.rowid0_exp = rowid0_exp
        self.rowid1_exp = rowid1_exp

    def compile_bql(self, bdb, bql, out):
        assert ast.is_bql(bql)
        assert self.ctx.btable is not None
        if not core.bayesdb_table_exists(bdb, self.ctx.btable):
            raise ValueError('No such btable: %s' % (self.ctx.btable,))
        table_id = core.bayesdb_table_id(bdb, self.ctx.btable)
        if isinstance(bql, ast.ExpBQLProb):
            raise ValueError('Probability of value is 1-row function.')
        elif isinstance(bql, ast.ExpBQLPredProb):
            raise ValueError('Predictive probability is 1-row function.')
        elif isinstance(bql, ast.ExpBQLTyp):
            raise ValueError('Typicality is 1-row function.')
        elif isinstance(bql, ast.ExpBQLSim):
            if bql.rowid is not None:
                raise ValueError('Similarity neds no row id in 2-row context.')
            out.write('bql_row_similarity(%s, %s, %s' %
                (table_id, self.rowid0_exp, self.rowid1_exp))
            if len(bql.column_lists) == 1 and \
               isinstance(bql.column_lists[0], ast.ColListAll):
                # We'll likely run up against SQLite's limit on the
                # number of arguments in this case.  Instead, let
                # bql_row_similarity find the columns.
                pass
            else:
                out.write(', ')
                compile_column_lists(bdb, table_id, bql.column_lists, self,
                    out)
            out.write(')')
        elif isinstance(bql, ast.ExpBQLDepProb):
            raise ValueError('Dependence probability is 0-row function.')
        elif isinstance(bql, ast.ExpBQLMutInf):
            raise ValueError('Mutual information is 0-row function.')
        elif isinstance(bql, ast.ExpBQLCorrel):
            raise ValueError('Column correlation is 0-row function.')
        elif isinstance(bql, ast.ExpBQLInfer):
            raise ValueError('Infer is a 1-row function.')
        else:
            assert False        # XXX

class BQLCompiler_1Col(object):
    def __init__(self, ctx, colno_exp):
        assert isinstance(ctx, ast.EstCols)
        assert isinstance(colno_exp, str)
        self.ctx = ctx
        self.colno_exp = colno_exp

    def compile_bql(self, bdb, bql, out):
        assert ast.is_bql(bql)
        assert self.ctx.btable is not None
        if not core.bayesdb_table_exists(bdb, self.ctx.btable):
            raise ValueError('No such btable: %s' % (self.ctx.btable,))
        table_id = core.bayesdb_table_id(bdb, self.ctx.btable)
        if isinstance(bql, ast.ExpBQLProb):
            if bql.column is not None:
                raise ValueError('Probability of value needs no column.')
            out.write('bql_column_value_probability(%s, %s, ' %
                (table_id, self.colno_exp))
            compile_expression(bdb, bql.value, self, out)
            out.write(')')
        elif isinstance(bql, ast.ExpBQLPredProb):
            # XXX Is this true?
            raise ValueError('Predictive probability makes sense only at row.')
        elif isinstance(bql, ast.ExpBQLTyp):
            if bql.column is not None:
                raise ValueError('Typicality of column needs no column.')
            out.write('bql_column_typicality(%s, %s)' %
                (table_id, self.colno_exp))
        elif isinstance(bql, ast.ExpBQLSim):
            raise ValueError('Similarity to row makes sense only at row.')
        elif isinstance(bql, ast.ExpBQLDepProb):
            compile_bql_2col_1(bdb, table_id,
                'bql_column_dependence_probability',
                'Dependence probability', None, bql, self.colno_exp, self, out)
        elif isinstance(bql, ast.ExpBQLMutInf):
            compile_bql_2col_1(bdb, table_id,
                'bql_column_mutual_information',
                'Mutual information',
                compile_mutinf_extra, bql, self.colno_exp, self, out)
        elif isinstance(bql, ast.ExpBQLCorrel):
            compile_bql_2col_1(bdb, table_id,
                'bql_column_correlation',
                'Column correlation', None, bql, self.colno_exp, self, out)
        elif isinstance(bql, ast.ExpBQLInfer):
            raise ValueError('Infer is a 1-row function.')
        else:
            assert False        # XXX

class BQLCompiler_2Col(object):
    def __init__(self, ctx, colno0_exp, colno1_exp):
        assert isinstance(ctx, ast.EstPairCols)
        assert isinstance(colno0_exp, str)
        assert isinstance(colno1_exp, str)
        self.ctx = ctx
        self.colno0_exp = colno0_exp
        self.colno1_exp = colno1_exp

    def compile_bql(self, bdb, bql, out):
        assert ast.is_bql(bql)
        assert self.ctx.btable is not None
        if not core.bayesdb_table_exists(bdb, self.ctx.btable):
            raise ValueError('No such btable: %s' % (self.ctx.btable,))
        table_id = core.bayesdb_table_id(bdb, self.ctx.btable)
        if isinstance(bql, ast.ExpBQLProb):
            raise ValueError('Probability of value is one-column function.')
        elif isinstance(bql, ast.ExpBQLPredProb):
            raise ValueError('Predictive probability is one-column function.')
        elif isinstance(bql, ast.ExpBQLTyp):
            raise ValueError('Typicality is one-column function.')
        elif isinstance(bql, ast.ExpBQLSim):
            raise ValueError('Similarity to row makes sense only at row.')
        elif isinstance(bql, ast.ExpBQLDepProb):
            compile_bql_2col_0(bdb, table_id,
                'bql_column_dependence_probability',
                'Dependence probability',
                None,
                bql, self.colno0_exp, self.colno1_exp, self, out)
        elif isinstance(bql, ast.ExpBQLMutInf):
            compile_bql_2col_0(bdb, table_id,
                'bql_column_mutual_information',
                'Mutual Information',
                compile_mutinf_extra,
                bql, self.colno0_exp, self.colno1_exp, self, out)
        elif isinstance(bql, ast.ExpBQLCorrel):
            compile_bql_2col_0(bdb, table_id,
                'bql_column_correlation',
                'Correlation',
                None,
                bql, self.colno0_exp, self.colno1_exp, self, out)
        elif isinstance(bql, ast.ExpBQLInfer):
            raise ValueError('Infer is a 1-row function.')
        else:
            assert False        # XXX

def compile_column_lists(bdb, table_id, column_lists, _bql_compiler, out):
    first = True
    for collist in column_lists:
        if first:
            first = False
        else:
            out.write(', ')
        if isinstance(collist, ast.ColListAll):
            colnos = core.bayesdb_column_numbers(bdb, table_id)
            out.write(', '.join(str(colno) for colno in colnos))
        elif isinstance(collist, ast.ColListLit):
            colnos = (core.bayesdb_column_number(bdb, table_id, column)
                for column in collist.columns)
            out.write(', '.join(str(colno) for colno in colnos))
        elif isinstance(collist, ast.ColListSub):
            # XXX We need some kind of type checking to guarantee that
            # what we get out of this will be a list of columns in the
            # table implied by the surrounding context.
            subout = out.subquery()
            compile_query(bdb, collist.query, subout)
            subquery = subout.getvalue()
            subbindings = subout.getbindings()
            columns = bdb.sql_execute(subquery, subbindings).fetchall()
            subfirst = True
            for column in columns:
                if subfirst:
                    subfirst = False
                else:
                    out.write(', ')
                if len(column) != 1:
                    raise ValueError('ESTIMATE COLUMNS subquery returned' +
                        ' multi-cell rows.')
                if not isinstance(column[0], unicode):
                    raise TypeError('ESTIMATE COLUMNS subquery returned' +
                        ' non-string.')
                colno = core.bayesdb_column_number(bdb, table_id, column[0])
                out.write('%d' % (colno,))
        elif isinstance(collist, ast.ColListSav):
            raise NotImplementedError('saved column lists')
        else:
            assert False        # XXX

def compile_bql_2col_2(bdb, table_id, bqlfn, desc, extra, bql, bql_compiler,
        out):
    if bql.column0 is None:
        raise ValueError(desc + ' needs exactly two columns.')
    if bql.column1 is None:
        raise ValueError(desc + ' needs exactly two columns.')
    colno0 = core.bayesdb_column_number(bdb, table_id, bql.column0)
    colno1 = core.bayesdb_column_number(bdb, table_id, bql.column1)
    out.write('%s(%s, %s, %s' % (bqlfn, table_id, colno0, colno1))
    if extra:
        extra(bdb, table_id, bql, bql_compiler, out)
    out.write(')')

def compile_bql_2col_1(bdb, table_id, bqlfn, desc, extra, bql, colno1_exp,
        bql_compiler, out):
    if bql.column0 is None:
        raise ValueError(desc + ' needs at least one column.')
    if bql.column1 is not None:
        raise ValueError(desc + ' needs at most one column.')
    colno0 = core.bayesdb_column_number(bdb, table_id, bql.column0)
    out.write('%s(%s, %s, %s' % (bqlfn, table_id, colno0, colno1_exp))
    if extra:
        extra(bdb, table_id, bql, bql_compiler, out)
    out.write(')')

def compile_bql_2col_0(bdb, table_id, bqlfn, desc, extra, bql,
        colno0_exp, colno1_exp, bql_compiler, out):
    if bql.column0 is not None:
        raise ValueError(desc + ' needs no columns.')
    if bql.column1 is not None:
        raise ValueError(desc + ' needs no columns.')
    out.write('%s(%s, %s, %s' % (bqlfn, table_id, colno0_exp, colno1_exp))
    if extra:
        extra(bdb, table_id, bql, bql_compiler, out)
    out.write(')')

def compile_mutinf_extra(bdb, table_id, bql, bql_compiler, out):
    out.write(', ')
    if bql.nsamples:
        compile_expression(bdb, bql.nsamples, bql_compiler, out)
    else:
        out.write('NULL')

def compile_1row_expression(bdb, exp, query, out):
    bql_compiler = BQLCompiler_1Row(query)
    compile_expression(bdb, exp, bql_compiler, out)

def compile_2row_expression(bdb, exp, query, rowid0_exp, rowid1_exp, out):
    bql_compiler = BQLCompiler_2Row(query, rowid0_exp, rowid1_exp)
    compile_expression(bdb, exp, bql_compiler, out)

def compile_1col_expression(bdb, exp, query, colno_exp, out):
    bql_compiler = BQLCompiler_1Col(query, colno_exp)
    compile_expression(bdb, exp, bql_compiler, out)

def compile_2col_expression(bdb, exp, query, colno0_exp, colno1_exp, out):
    bql_compiler = BQLCompiler_2Col(query, colno0_exp, colno1_exp)
    compile_expression(bdb, exp, bql_compiler, out)

def compile_expression(bdb, exp, bql_compiler, out):
    if isinstance(exp, ast.ExpLit):
        compile_literal(bdb, exp.value, out)
    elif isinstance(exp, ast.ExpNumpar):
        out.write_numpar(exp.number)
    elif isinstance(exp, ast.ExpNampar):
        out.write_nampar(exp.name, exp.number)
    elif isinstance(exp, ast.ExpCol):
        compile_table_column(bdb, exp.table, exp.column, out)
    elif isinstance(exp, ast.ExpSub):
        # XXX Provide context for row list vs column list wanted.
        compile_subquery(bdb, exp.query, bql_compiler, out)
    elif isinstance(exp, ast.ExpApp):
        compile_name(bdb, exp.operator, out)
        with compiling_paren(bdb, out, '(', ')'):
            if exp.distinct:
                out.write('DISTINCT ')
            first = True
            for operand in exp.operands:
                if first:
                    first = False
                else:
                    out.write(', ')
                compile_expression(bdb, operand, bql_compiler, out)
    elif isinstance(exp, ast.ExpAppStar):
        compile_name(bdb, exp.operator, out)
        out.write('(*)')
    elif isinstance(exp, ast.ExpOp):
        with compiling_paren(bdb, out, '(', ')'):
            compile_op(bdb, exp, bql_compiler, out)
    elif isinstance(exp, ast.ExpCollate):
        with compiling_paren(bdb, out, '(', ')'):
            compile_expression(bdb, exp.expression, bql_compiler, out)
            out.write(' COLLATE ')
            compile_name(bdb, exp.collation, out)
    elif isinstance(exp, ast.ExpIn):
        with compiling_paren(bdb, out, '(', ')'):
            compile_expression(bdb, exp.expression, bql_compiler, out)
            if not exp.positive:
                out.write(' NOT')
            out.write(' IN ')
            compile_subquery(bdb, exp.query, bql_compiler, out)
    elif isinstance(exp, ast.ExpCast):
        with compiling_paren(bdb, out, 'CAST(', ')'):
            compile_expression(bdb, exp.expression, bql_compiler, out)
            out.write(' AS ')
            compile_type(bdb, exp.type, out)
    elif isinstance(exp, ast.ExpExists):
        out.write('EXISTS ')
        compile_subquery(bdb, exp.query, bql_compiler, out)
    elif isinstance(exp, ast.ExpCase):
        with compiling_paren(bdb, out, 'CASE', 'END'):
            if exp.key is not None:
                out.write(' ')
                compile_expression(bdb, exp.key, bql_compiler, out)
            for cond, then in exp.whens:
                out.write(' WHEN ')
                compile_expression(bdb, cond, bql_compiler, out)
                out.write(' THEN ')
                compile_expression(bdb, then, bql_compiler, out)
            if exp.otherwise is not None:
                out.write(' ELSE ')
                compile_expression(bdb, exp.otherwise, bql_compiler, out)
            out.write(' ')
    else:
        assert ast.is_bql(exp)
        bql_compiler.compile_bql(bdb, exp, out)

def compile_op(bdb, op, bql_compiler, out):
    fmt = operator_fmts[op.operator]
    i = 0
    r = 0
    while i < len(fmt):
        j = fmt.find('%', i)
        if j == -1:             # Silly indexing convention.
            j = len(fmt)
        if i < j:
            out.write(fmt[i : j])
        if j == len(fmt):
            break
        j += 1                  # Skip %.
        assert j < len(fmt)
        d = fmt[j]
        j += 1                  # Skip directive.
        if d == '%':
            out.write('%')
        elif d == 's':
            assert r < len(op.operands)
            compile_expression(bdb, op.operands[r], bql_compiler, out)
            r += 1
        else:
            assert False        # XXX
        i = j
    assert r == len(op.operands)

operator_fmts = {
    ast.OP_BOOLOR:      '%s OR %s',
    ast.OP_BOOLAND:     '%s AND %s',
    ast.OP_BOOLNOT:     'NOT %s',
    ast.OP_IS:          '%s IS %s',
    ast.OP_ISNOT:       '%s IS NOT %s',
    ast.OP_LIKE:        '%s LIKE %s',
    ast.OP_NOTLIKE:     '%s NOT LIKE %s',
    ast.OP_LIKE_ESC:    '%s LIKE %s ESCAPE %s',
    ast.OP_NOTLIKE_ESC: '%s NOT LIKE %s ESCAPE %s',
    ast.OP_GLOB:        '%s GLOB %s',
    ast.OP_NOTGLOB:     '%s NOT GLOB %s',
    ast.OP_GLOB_ESC:    '%s GLOB %s ESCAPE %s',
    ast.OP_NOTGLOB_ESC: '%s NOT GLOB %s ESCAPE %s',
    ast.OP_REGEXP:      '%s REGEXP %s',
    ast.OP_NOTREGEXP:   '%s NOT REGEXP %s',
    ast.OP_REGEXP_ESC:  '%s REGEXP %s ESCAPE %s',
    ast.OP_NOTREGEXP_ESC: '%s NOT REGEXP %s ESCAPE %s',
    ast.OP_MATCH:       '%s MATCH %s',
    ast.OP_NOTMATCH:    '%s NOT MATCH %s',
    ast.OP_MATCH_ESC:   '%s MATCH %s ESCAPE %s',
    ast.OP_NOTMATCH_ESC: '%s NOT MATCH %s ESCAPE %s',
    ast.OP_BETWEEN:     '%s BETWEEN %s AND %s',
    ast.OP_NOTBETWEEN:  '%s NOT BETWEEN %s AND %s',
    ast.OP_ISNULL:      '%s ISNULL',
    ast.OP_NOTNULL:     '%s NOTNULL',
    ast.OP_NEQ:         '%s != %s',
    ast.OP_EQ:          '%s = %s',
    ast.OP_LT:          '%s < %s',
    ast.OP_LEQ:         '%s <= %s',
    ast.OP_GEQ:         '%s >= %s',
    ast.OP_GT:          '%s > %s',
    ast.OP_BITAND:      '%s & %s',
    ast.OP_BITIOR:      '%s | %s',
    ast.OP_LSHIFT:      '%s << %s',
    ast.OP_RSHIFT:      '%s >> %s',
    ast.OP_ADD:         '%s + %s',
    ast.OP_SUB:         '%s - %s',
    ast.OP_MUL:         '%s * %s',
    ast.OP_DIV:         '%s / %s',
    ast.OP_REM:         '%s %% %s',
    ast.OP_CONCAT:      '%s || %s',
    ast.OP_BITNOT:      '~ %s',
}

def compile_literal(bdb, lit, out):
    if isinstance(lit, ast.LitNull):
        out.write('NULL')
    elif isinstance(lit, ast.LitInt):
        out.write(str(lit.value))
    elif isinstance(lit, ast.LitFloat):
        out.write(str(lit.value)) # XXX Make sure floats unparse as such.
    elif isinstance(lit, ast.LitString):
        compile_string(bdb, lit.value, out)
    else:
        assert False            # XXX

def compile_string(bdb, string, out):
    with compiling_paren(bdb, out, "'", "'"):
        out.write(string.replace("'", "''"))

def compile_name(bdb, name, out):
    out.write(sqlite3_quote_name(name))

def compile_table_name(bdb, table_name, out):
    # XXX Qualified table names.
    compile_name(bdb, table_name, out)

def compile_table_column(bdb, table_name, column_name, out):
    if table_name is not None:
        compile_table_name(bdb, table_name, out)
        out.write('.')
    compile_column_name(bdb, table_name, column_name, out)

def compile_column_name(bdb, _table_name, column_name, out):
    compile_name(bdb, column_name, out)

def compile_type(bdb, type, out):
    first = True
    for n in type.names:
        if first:
            first = False
        else:
            out.write(' ')
        compile_name(bdb, n, out)
    if 0 < len(type.args):
        with compiling_paren(bdb, out, '(', ')'):
            first = True
            for a in type.args:
                if first:
                    first = False
                else:
                    out.write(', ')
                assert isinstance(a, int)
                out.write(str(a))

@contextlib.contextmanager
def compiling_paren(bdb, out, start, end):
    out.write(start)
    yield
    out.write(end)
