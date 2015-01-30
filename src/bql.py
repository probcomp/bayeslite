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

def execute_phrase(bdb, phrase, bindings=()):
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
            return bdb.sqlite.execute(out.getvalue(), out.getbindings())
    if isinstance(phrase, ast.CreateBtableCSV):
        # XXX Codebook?
        import_csv.bayesdb_import_csv_file(bdb, phrase.name, phrase.file,
            ifnotexists=phrase.ifnotexists)
        return []
    if isinstance(phrase, ast.InitModels):
        table_id = core.bayesdb_table_id(bdb, phrase.btable)
        nmodels = phrase.nmodels
        config = phrase.config
        core.bayesdb_models_initialize(bdb, table_id, nmodels, config,
            ifnotexists=phrase.ifnotexists)
        return []
    if isinstance(phrase, ast.AnalyzeModels):
        table_id = core.bayesdb_table_id(bdb, phrase.btable)
        modelnos = phrase.modelnos
        iterations = phrase.iterations
        minutes = phrase.minutes
        wait = phrase.wait
        assert wait             # XXX
        assert minutes is None  # XXX
        if modelnos is None:
            core.bayesdb_models_analyze(bdb, table_id, iterations=iterations)
        else:
            for modelno in modelnos:
                core.bayesdb_models_analyze1(bdb, table_id, modelno,
                    iterations=iterations)
        return []
    assert False                # XXX

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
        return Output(self.n_numpar, self.nampar_map, self.bindings)

    def getvalue(self):
        return self.stringio.getvalue()

    def getbindings(self):
        if isinstance(self.bindings, dict):
            unknown = set([])
            missing = set(self.nampar_map)
            bindings_list = [None] * self.n_numpar
            for name in self.bindings:
                lname = name.lower()
                if lname not in self.nampar_map:
                    unknown.add(name)
                    continue
                missing.remove(lname)
                n = self.nampar_map[lname]
                m = self.renumber[n]
                j = m - 1
                assert bindings_list[j] is None
                bindings_list[j] = self.bindings[name]
            if 0 < len(missing):
                raise ValueError('Missing parameter bindings: %s' % (missing,))
            if 0 < len(unknown):
                raise ValueError('Unknown parameter bindings: %s' % (unknown,))
            if len(self.bindings) < self.n_numpar:
                missing_numbers = set(range(1, self.n_numpar + 1))
                for name in self.bindings:
                    missing_numbers.remove(self.nampar_map[name.lower()])
                raise ValueError('Missing parameter numbers: %s' %
                    (missing_numbers,))
            return bindings_list
        elif isinstance(self.bindings, tuple) or \
             isinstance(self.bindings, list):
            if len(self.bindings) < self.n_numpar:
                raise ValueError('Too few parameter bindings: %d < %d' %
                    (len(self.bindings), self.n_numpar))
            if len(self.bindings) > self.n_numpar:
                raise ValueError('Too many parameter bindings: %d > %d' %
                    (len(self.bindings), self.n_numpar))
            assert len(self.select) <= self.n_numpar
            return [self.bindings[j] for j in self.select]
        else:
            raise TypeError('Invalid query bindings: %s' % (self.bindings,))

    def write(self, text):
        self.stringio.write(text)

    def write_numpar(self, n):
        assert 0 < n
        assert n <= self.n_numpar
        i = n - 1
        m = None
        if n in self.renumber:
            m = self.renumber[n]
        else:
            j = len(self.select)
            m = j + 1
            self.select.append(i)
            self.renumber[n] = m
        self.write('?%d' % (m,))

    def write_nampar(self, name, n):
        assert 0 < n
        assert n <= self.n_numpar
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
    out.write('SELECT c0.name, c1.name')
    out.write(' FROM bayesdb_table_column AS c0, bayesdb_table_column AS c1')
    table_id = core.bayesdb_table_id(bdb, estpaircols.btable)
    out.write(' WHERE c0.table_id = %d AND c1.table_id = %d' %
        (table_id, table_id))
    colno0_exp = 'c0.colno'     # XXX
    colno1_exp = 'c1.colno'     # XXX
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
    rowid0_exp = 'r0.rowid'
    rowid1_exp = 'r1.rowid'
    out.write('SELECT %s, %s, ' % (rowid0_exp, rowid1_exp))
    compile_2row_expression(bdb, estpairrow.expression, estpairrow,
        rowid0_exp, rowid1_exp, out)
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
        table_id = core.bayesdb_table_id(bdb, self.ctx.tables[0].table)
        rowid_col = 'rowid'     # XXX Don't hard-code this.
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
                out.write('bql_row_typicality(%s, rowid)' % (table_id,))
            else:
                colno = core.bayesdb_column_number(bdb, table_id, bql.column)
                out.write('bql_column_typicality(%s, %s)' % (table_id, colno))
        elif isinstance(bql, ast.ExpBQLSim):
            if bql.rowid is None:
                raise ValueError('Similarity as 1-row function needs row.')
            out.write('bql_row_similarity(%s, rowid, ' % (table_id,))
            compile_expression(bdb, bql.rowid, self, out)
            out.write(', ')
            compile_column_lists(bdb, table_id, bql.column_lists, self, out)
            out.write(')')
        elif isinstance(bql, ast.ExpBQLDepProb):
            compile_bql_2col_2(bdb, table_id,
                'bql_column_dependence_probability',
                'Dependence probability', bql, out)
        elif isinstance(bql, ast.ExpBQLMutInf):
            compile_bql_2col_2(bdb, table_id,
                'bql_column_mutual_information',
                'Mutual information', bql, out)
        elif isinstance(bql, ast.ExpBQLCorrel):
            compile_bql_2col_2(bdb, table_id,
                'bql_column_correlation',
                'Column correlation', bql, out)
        elif isinstance(bql, ast.ExpBQLInfer):
            assert bql.column is not None
            colno = core.bayesdb_column_number(bdb, table_id, bql.column)
            out.write('bql_infer(%d, %d, rowid, ' % (table_id, colno))
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
            out.write('bql_row_similarity(%s, %s, %s, ' %
                (table_id, self.rowid0_exp, self.rowid1_exp))
            compile_column_lists(bdb, table_id, bql.column_lists, self, out)
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
                'Dependence probability', bql, self.colno_exp, out)
        elif isinstance(bql, ast.ExpBQLMutInf):
            compile_bql_2col_1(bdb, table_id,
                'bql_column_mutual_information',
                'Mutual information', bql, self.colno_exp, out)
        elif isinstance(bql, ast.ExpBQLCorrel):
            compile_bql_2col_1(bdb, table_id,
                'bql_column_correlation',
                'Column correlation', bql, self.colno_exp, out)
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
                'Dependence probability', bql,
                self.colno0_exp, self.colno1_exp, out)
        elif isinstance(bql, ast.ExpBQLMutInf):
            compile_bql_2col_0(bdb, table_id,
                'bql_column_mutual_information',
                'Mutual Information', bql,
                self.colno0_exp, self.colno1_exp, out)
        elif isinstance(bql, ast.ExpBQLCorrel):
            compile_bql_2col_0(bdb, table_id,
                'bql_column_correlation',
                'Correlation', bql,
                self.colno0_exp, self.colno1_exp, out)
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
            columns = bdb.sqlite.execute(subquery, subbindings).fetchall()
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

def compile_bql_2col_2(bdb, table_id, bqlfn, desc, bql, out):
    if bql.column0 is None:
        raise ValueError(desc + ' needs exactly two columns.')
    if bql.column1 is None:
        raise ValueError(desc + ' needs exactly two columns.')
    colno0 = core.bayesdb_column_number(bdb, table_id, bql.column0)
    colno1 = core.bayesdb_column_number(bdb, table_id, bql.column1)
    out.write('%s(%s, %s, %s)' % (bqlfn, table_id, colno0, colno1))

def compile_bql_2col_1(bdb, table_id, bqlfn, desc, bql, colno1_exp, out):
    if bql.column0 is None:
        raise ValueError(desc + ' needs at least one column.')
    if bql.column1 is not None:
        raise ValueError(desc + ' needs at most one column.')
    colno0 = core.bayesdb_column_number(bdb, table_id, bql.column0)
    out.write('%s(%s, %s, %s)' % (bqlfn, table_id, colno0, colno1_exp))

def compile_bql_2col_0(bdb, table_id, bqlfn, desc, bql, colno0_exp, colno1_exp,
        out):
    if bql.column0 is not None:
        raise ValueError(desc + ' needs no columns.')
    if bql.column1 is not None:
        raise ValueError(desc + ' needs no columns.')
    out.write('%s(%s, %s, %s)' % (bqlfn, table_id, colno0_exp, colno1_exp))

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
            first = True
            for operand in exp.operands:
                if first:
                    first = False
                else:
                    out.write(', ')
                compile_expression(bdb, operand, bql_compiler, out)
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
    out.write(core.sqlite3_quote_name(name))

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
