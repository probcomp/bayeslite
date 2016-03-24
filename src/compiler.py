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

"""BQL->SQL compiler.

To compile a parsed BQL query:

1. Determine the number, names, and values of the parameters.
2. Create an output accumulator, `Output`.
3. Pass the query and accumulator to `compile_query`.
4. Use :meth:`Output.getvalue` to get the compiled SQL text.
5. Use :meth:`Output.getbindings` to get bindings for parameters that
   were actually used in the query.
6. Use :func:`bayesdb_wind` or similar to bracket the execution of the
   SQL query with wind/unwind commands.
"""

import StringIO
import contextlib

import bayeslite.ast as ast
import bayeslite.bqlfn as bqlfn
import bayeslite.core as core

from bayeslite.exception import BQLError
from bayeslite.sqlite3_util import sqlite3_quote_name
from bayeslite.util import casefold

class Output(object):
    """Compiled SQL output accumulator.

    Like a write-only StringIO.StringIO(), but also does bookkeeping
    for parameters and subqueries.
    """

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
        self.winders = []               # list of pre-query (sql, bindings)
        self.unwinders = []             # list of post-query (sql, bindings)

    def subquery(self):
        """Return an output accumulator for a subquery."""
        return Output(self.n_numpar, self.nampar_map, self.bindings)

    def getvalue(self):
        """Return the accumulated output."""
        return self.stringio.getvalue()

    def getbindings(self):
        """Return a selection of bindings fit for the accumulated output.

        If there were subqueries, or if this is accumulating output
        for a subquery, this may not use all bindings.
        """
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

    def getwindings(self):
        return self.winders, self.unwinders

    def write(self, text):
        """Accumulate `text` in the output of :meth:`getvalue`."""
        self.stringio.write(text)

    def write_numpar(self, n):
        """Accumulate a reference to the parameter numbered `n`."""
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
        """Accumulate a reference to the parameter `name` numbered `n`."""
        assert 0 < n
        assert n <= self.n_numpar
        # Just treat it as if it were a numbered parameter; it is the
        # parser's job to map between numbered and named parameters.
        assert self.nampar_map[name] == n
        self.write_numpar(n)

    def winder(self, sql, bindings):
        self.winders.append((sql, bindings))
    def unwinder(self, sql, bindings):
        self.unwinders.append((sql, bindings))

@contextlib.contextmanager
def bayesdb_wind(bdb, winders, unwinders):
    """Perform queries `winders` before and `unwinders` after.

    Each of `winders` and `unwinders` is a list of ``(<sql>,
    <bindings>)`` tuples.
    """
    if 0 < len(winders) or 0 < len(unwinders):
        with bdb.savepoint():
            for (sql, bindings) in winders:
                bdb.sql_execute(sql, bindings)
            try:
                yield
            finally:
                for (sql, bindings) in reversed(unwinders):
                    bdb.sql_execute(sql, bindings)
    else:
        yield

def compile_query(bdb, query, out):
    """Compile `query`, writing output to `output`.

    :param bdb: database in which to interpret `query`
    :param query: abstract syntax tree of a query
    :param Output out: output accumulator
    """
    if isinstance(query, ast.Select):
        compile_select(bdb, query, out)
    elif isinstance(query, ast.Estimate):
        compile_estimate(bdb, query, out)
    elif isinstance(query, ast.EstBy):
        compile_estimate_by(bdb, query, out)
    elif isinstance(query, ast.InferExplicit):
        if any(isinstance(c, ast.PredCol) for c in query.columns):
            compile_infer_explicit_predict(bdb, query, out)
        else:
            named = True
            compile_infer_explicit(bdb, query, named, out)
    elif isinstance(query, ast.InferAuto):
        compile_infer_auto(bdb, query, out)
    elif isinstance(query, ast.Simulate):
        compile_simulate(bdb, query, out)
    elif isinstance(query, ast.EstCols):
        compile_estcols(bdb, query, out)
    elif isinstance(query, ast.EstPairCols):
        compile_estpaircols(bdb, query, out)
    elif isinstance(query, ast.EstPairRow):
        compile_estpairrow(bdb, query, out)
    else:
        assert False, 'Invalid query: %s' % (repr(query),)

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
    named = True
    compile_select_columns(bdb, select.columns, named, BQLCompiler_None(), out)
    if select.tables is not None:
        assert 0 < len(select.tables)
        compile_select_tables(bdb, select, out)
    if select.condition is not None:
        out.write(' WHERE ')
        compile_nobql_expression(bdb, select.condition, out)
    if select.grouping is not None:
        assert 0 < len(select.grouping.keys)
        first = True
        for key in select.grouping.keys:
            if first:
                out.write(' GROUP BY ')
                first = False
            else:
                out.write(', ')
            compile_nobql_expression(bdb, key, out)
        if select.grouping.condition:
            out.write(' HAVING ')
            compile_nobql_expression(bdb, select.grouping.condition, out)
    if select.order is not None:
        assert 0 < len(select.order)
        first = True
        for order in select.order:
            if first:
                out.write(' ORDER BY ')
                first = False
            else:
                out.write(', ')
            compile_nobql_expression(bdb, order.expression, out)
            if order.sense == ast.ORD_ASC:
                pass
            elif order.sense == ast.ORD_DESC:
                out.write(' DESC')
            else:
                assert False, 'Invalid order sense: %s' % (repr(order.sense),)
    if select.limit is not None:
        out.write(' LIMIT ')
        compile_nobql_expression(bdb, select.limit.limit, out)
        if select.limit.offset is not None:
            out.write(' OFFSET ')
            compile_nobql_expression(bdb, select.limit.offset, out)

def compile_infer_explicit_predict(bdb, infer, out):
    out.write('SELECT')
    first = True
    for i, col in enumerate(infer.columns):
        if first:
            first = False
        else:
            out.write(',')
        out.write(' ')
        if isinstance(col, ast.PredCol):
            vcn = col.column if col.name is None else col.name
            qvcn = sqlite3_quote_name(vcn)
            out.write("bql_json_get(c%u, 'value') AS %s" % (i, qvcn))
            out.write(', ')
            qccn = sqlite3_quote_name(col.confname)
            out.write("bql_json_get(c%u, 'confidence') AS %s" % (i, qccn))
        elif isinstance(col, ast.SelColExp):
            out.write('c%u' % (i,))
            if col.name is not None:
                qcn = sqlite3_quote_name(col.name)
                out.write(' AS %s' % (qcn,))
            elif isinstance(col.expression, ast.ExpCol):
                qcn = sqlite3_quote_name(col.expression.column)
                out.write(' AS %s' % (qcn,))
            else:
                # XXX Preserve the expression as a column name...?
                pass
        elif isinstance(col, ast.SelColAll):
            raise NotImplementedError('You have no business'
                ' mixing * with PREDICT!')
        elif isinstance(col, ast.SelColSub):
            raise NotImplementedError('You have no business'
                ' mixing subquery-chosen columns with PREDICT!')
        else:
            assert False, 'Invalid INFER column: %s' % (repr(col),)
    out.write(' FROM ')
    with compiling_paren(bdb, out, '(', ')'):
        named = False
        compile_infer_explicit(bdb, infer, named, out)

def compile_infer_explicit(bdb, infer, named, out):
    assert isinstance(infer, ast.InferExplicit)
    out.write('SELECT')
    if not core.bayesdb_has_generator_default(bdb, infer.generator):
        raise BQLError(bdb, 'No such generator: %s' % (infer.generator,))
    generator_id = core.bayesdb_get_generator_default(bdb, infer.generator)
    bql_compiler = BQLCompiler_1Row_Infer(generator_id, infer.modelno)
    compile_select_columns(bdb, infer.columns, named, bql_compiler, out)
    table_name = core.bayesdb_generator_table(bdb, generator_id)
    qt = sqlite3_quote_name(table_name)
    out.write(' FROM %s' % (qt,))
    if infer.condition is not None:
        out.write(' WHERE ')
        compile_expression(bdb, infer.condition, bql_compiler, out)
    if infer.grouping is not None:
        assert 0 < len(infer.grouping.keys)
        first = True
        for key in infer.grouping.keys:
            if first:
                out.write(' GROUP BY ')
                first = False
            else:
                out.write(', ')
            compile_expression(bdb, key, bql_compiler, out)
        if infer.grouping.condition:
            out.write(' HAVING ')
            compile_expression(bdb, infer.grouping.condition, bql_compiler,
                out)
    if infer.order is not None:
        assert 0 < len(infer.order)
        first = True
        for order in infer.order:
            if first:
                out.write(' ORDER BY ')
                first = False
            else:
                out.write(', ')
            compile_expression(bdb, order.expression, bql_compiler, out)
            if order.sense == ast.ORD_ASC:
                pass
            elif order.sense == ast.ORD_DESC:
                out.write(' DESC')
            else:
                assert False, 'Invalid order sense: %s' % (repr(order.sense),)
    if infer.limit is not None:
        out.write(' LIMIT ')
        compile_expression(bdb, infer.limit.limit, bql_compiler, out)
        if infer.limit.offset is not None:
            out.write(' OFFSET ')
            compile_expression(bdb, infer.limit.offset, bql_compiler, out)

def compile_infer_auto(bdb, infer, out):
    assert isinstance(infer, ast.InferAuto)
    if not core.bayesdb_has_generator_default(bdb, infer.generator):
        raise BQLError(bdb, 'No such generator: %s' % (infer.generator,))
    generator_id = core.bayesdb_get_generator_default(bdb, infer.generator)
    table = core.bayesdb_generator_table(bdb, generator_id)
    confidence = infer.confidence
    def map_column(col, name):
        exp = ast.ExpCol(None, col)
        if core.bayesdb_generator_has_column(bdb, generator_id, col):
            pred = ast.ExpBQLPredict(col, confidence)
            exp = ast.ExpApp(False, 'IFNULL', [exp, pred])
        if name is None:
            name = col
        return ast.SelColExp(exp, name)
    def map_columns(col):
        if isinstance(col, ast.InfColAll):
            column_names = core.bayesdb_table_column_names(bdb, table)
            return [map_column(colname, None) for colname in column_names]
        elif isinstance(col, ast.InfColOne):
            return [map_column(col.column, col.name)]
        else:
            assert False, 'Invalid INFER column: %s' % (repr(col),)
    columns = [mcol for col in infer.columns for mcol in map_columns(col)]
    infer_exp = ast.InferExplicit(columns, infer.generator, infer.modelno,
        infer.condition, infer.grouping, infer.order, infer.limit)
    named = True
    return compile_infer_explicit(bdb, infer_exp, named, out)

def compile_estimate(bdb, estimate, out):
    assert isinstance(estimate, ast.Estimate)
    out.write('SELECT')
    if estimate.quantifier == ast.SELQUANT_DISTINCT:
        out.write(' DISTINCT')
    else:
        assert estimate.quantifier == ast.SELQUANT_ALL
    if not core.bayesdb_has_generator_default(bdb, estimate.generator):
        raise BQLError(bdb, 'No such generator: %s' % (estimate.generator,))
    generator_id = core.bayesdb_get_generator_default(bdb, estimate.generator)
    bql_compiler = BQLCompiler_1Row(generator_id, estimate.modelno)
    named = True
    compile_select_columns(bdb, estimate.columns, named, bql_compiler, out)
    table_name = core.bayesdb_generator_table(bdb, generator_id)
    qt = sqlite3_quote_name(table_name)
    out.write(' FROM %s' % (qt,))
    if estimate.condition is not None:
        out.write(' WHERE ')
        compile_expression(bdb, estimate.condition, bql_compiler, out)
    if estimate.grouping is not None:
        assert 0 < len(estimate.grouping.keys)
        first = True
        for key in estimate.grouping.keys:
            if first:
                out.write(' GROUP BY ')
                first = False
            else:
                out.write(', ')
            compile_expression(bdb, key, bql_compiler, out)
        if estimate.grouping.condition:
            out.write(' HAVING ')
            compile_expression(bdb, estimate.grouping.condition, bql_compiler,
                out)
    if estimate.order is not None:
        assert 0 < len(estimate.order)
        first = True
        for order in estimate.order:
            if first:
                out.write(' ORDER BY ')
                first = False
            else:
                out.write(', ')
            compile_expression(bdb, order.expression, bql_compiler, out)
            if order.sense == ast.ORD_ASC:
                pass
            elif order.sense == ast.ORD_DESC:
                out.write(' DESC')
            else:
                assert False, 'Invalid order sense: %s' % (repr(order.sense),)
    if estimate.limit is not None:
        out.write(' LIMIT ')
        compile_expression(bdb, estimate.limit.limit, bql_compiler, out)
        if estimate.limit.offset is not None:
            out.write(' OFFSET ')
            compile_expression(bdb, estimate.limit.offset, bql_compiler, out)

def compile_estimate_by(bdb, estby, out):
    assert isinstance(estby, ast.EstBy)
    out.write('SELECT ')
    if estby.quantifier == ast.SELQUANT_DISTINCT:
        out.write(' DISTINCT')
    else:
        assert estby.quantifier == ast.SELQUANT_ALL
    if not core.bayesdb_has_generator_default(bdb, estby.generator):
        raise BQLError(bdb, 'No such generator: %s' % (estby.generator,))
    generator_id = core.bayesdb_get_generator_default(bdb, estby.generator)
    bql_compiler = BQLCompiler_Const(generator_id, estby.modelno)
    named = True
    compile_select_columns(bdb, estby.columns, named, bql_compiler, out)

def compile_select_columns(bdb, columns, named, bql_compiler, out):
    first = True
    for i, selcol in enumerate(columns):
        if first:
            out.write(' ')
            first = False
        else:
            out.write(', ')
        compile_select_column(bdb, selcol, i, named, bql_compiler, out)

def compile_select_column(bdb, selcol, i, named, bql_compiler, out):
    if isinstance(selcol, ast.SelColAll):
        if not named:
            raise NotImplementedError('Don\'t mix * with PREDICT!')
        if selcol.table is not None:
            compile_table_name(bdb, selcol.table, out)
            out.write('.')
        out.write('*')
    elif isinstance(selcol, ast.SelColSub):
        if not named:
            raise NotImplementedError('Don\'t mix <tab>.(<query>)'
                ' with PREDICT!')
        # XXX We need some kind of type checking to guarantee that
        # what we get out of this will be a list of columns in the
        # named table.
        subout = out.subquery()
        with compiling_paren(bdb, subout,
                'SELECT CAST(name AS TEXT) FROM (', ')'):
            compile_query(bdb, selcol.query, subout)
        subquery = subout.getvalue()
        subbindings = subout.getbindings()
        subwinders, subunwinders = subout.getwindings()
        with bayesdb_wind(bdb, subwinders, subunwinders):
            qt = sqlite3_quote_name(selcol.table)
            subfirst = True
            for row in bdb.sql_execute(subquery, subbindings):
                assert len(row) == 1
                assert isinstance(row[0], unicode)
                assert str(row[0])
                if subfirst:
                    subfirst = False
                else:
                    out.write(', ')
                qc = sqlite3_quote_name(str(row[0]))
                out.write('%s.%s' % (qt, qc))
    elif isinstance(selcol, ast.SelColExp):
        compile_expression(bdb, selcol.expression, bql_compiler, out)
        if not named:
            out.write(' AS c%u' % (i,))
        elif selcol.name is not None:
            out.write(' AS ')
            compile_name(bdb, selcol.name, out)
    elif isinstance(selcol, ast.PredCol):
        bql = ast.ExpBQLPredictConf(selcol.column)
        bql_compiler.compile_bql(bdb, bql, out)
        out.write(' AS c%u' % (i,))
    else:
        assert False, 'Invalid select column: %s' % (repr(selcol),)

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
        assert False, 'Invalid select table: %s' % (repr(table),)

def compile_simulate(bdb, simulate, out):
    # XXX Reduce copypasta with case for CreateTabSim in bql.py.
    with bdb.savepoint():
        temptable = bdb.temp_table_name()
        assert not core.bayesdb_has_table(bdb, temptable)
        generator_id = core.bayesdb_get_generator_default(bdb,
            simulate.generator)
        table = core.bayesdb_generator_table(bdb, generator_id)
        qtt = sqlite3_quote_name(temptable)
        qt = sqlite3_quote_name(table)
        column_names = simulate.columns
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
                    (repr(simulate.generator), repr(table), repr(column_name)))
        for column_name, _expression in simulate.constraints:
            if casefold(column_name) not in column_sqltypes:
                raise BQLError(bdb, 'No such column'
                    ' in generator %s table %s: %s' %
                    (repr(simulate.generator), repr(table), repr(column_name)))
        # XXX Move to compiler.py.
        subout = out.subquery()
        subout.write('SELECT ')
        with compiling_paren(bdb, subout, 'CAST(', ' AS INTEGER)'):
            compile_nobql_expression(bdb, simulate.nsamples, subout)
        subout.write(', ')
        with compiling_paren(bdb, subout, 'CAST(', ' AS INTEGER)'):
            compile_nobql_expression(bdb, simulate.modelno, subout)
        for _column_name, expression in simulate.constraints:
            subout.write(', ')
            compile_nobql_expression(bdb, expression, subout)
        winders, unwinders = subout.getwindings()
        with bayesdb_wind(bdb, winders, unwinders):
            cursor = bdb.sql_execute(subout.getvalue(),
                subout.getbindings()).fetchall()
        assert len(cursor) == 1
        nsamples = cursor[0][0]
        assert isinstance(nsamples, int)
        modelno = cursor[0][1]
        assert modelno is None or isinstance(modelno, int)
        constraints = \
            [(core.bayesdb_generator_column_number(bdb, generator_id, name),
                    value)
                for (name, _expression), value in
                    zip(simulate.constraints, cursor[0][2:])]
        colnos = \
            [core.bayesdb_generator_column_number(bdb, generator_id, name)
                for name in column_names]
        out.winder('CREATE TEMP TABLE %s (%s)' %
            (qtt,
             ','.join('%s %s' %
                    (qcn, column_sqltypes[casefold(column_name)])
                for qcn, column_name in zip(qcns, column_names))),
            ())
        insert_sql = '''
            INSERT INTO %s (%s) VALUES (%s)
        ''' % (qtt, ','.join(qcns), ','.join('?' for qcn in qcns))
        for row in bqlfn.bayesdb_simulate(bdb, generator_id, constraints,
                colnos, modelno=modelno, numpredictions=nsamples):
            out.winder(insert_sql, row)
        out.unwinder('DROP TABLE %s' % (qtt,), ())
        out.write('SELECT * FROM %s' % (qtt,))

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
    if not core.bayesdb_has_generator_default(bdb, estcols.generator):
        raise BQLError(bdb, 'No such generator: %s' % (estcols.generator,))
    generator_id = core.bayesdb_get_generator_default(bdb, estcols.generator)
    colno_exp = 'c.colno'       # XXX
    bql_compiler = BQLCompiler_1Col(generator_id, estcols.modelno, colno_exp)
    out.write('SELECT')
    first = True
    for col in estcols.columns:
        if first:
            first = False
        else:
            out.write(',')
        out.write(' ')
        if isinstance(col, ast.SelColAll):
            # XXX Whattakludge!  Should we not also include colno,
            # generator id?  Main reason for using * at all for this
            # is we don't have a mechanism for automatically
            # qualifying all input-column references with `c.', and
            # unqualified `name' is ambiguous in the FROM context
            # given below.
            out.write('c.name AS name') # XXX Colno?  Generator id?  ...?
        elif isinstance(col, ast.SelColSub):
            raise NotImplementedError('No subqueries for COLUMNS OF columns!')
        elif isinstance(col, ast.SelColExp):
            compile_expression(bdb, col.expression, bql_compiler, out)
            if col.name is not None:
                out.write(' AS ')
                compile_name(bdb, col.name, out)
        elif isinstance(col, ast.PredCol):
            raise NotImplementedError('No PREDICT on column queries!')
        else:
            assert False, 'Invalid ESTIMATE column: %s' % (repr(col),)
    out.write(' FROM bayesdb_generator AS g,'
        ' bayesdb_generator_column AS gc,'
        ' bayesdb_column AS c'
        ' WHERE g.id = %(generator_id)d'
        ' AND gc.generator_id = g.id'
        ' AND c.tabname = g.tabname AND c.colno = gc.colno' %
            {'generator_id': generator_id})
    if estcols.condition is not None:
        out.write(' AND ')
        compile_expression(bdb, estcols.condition, bql_compiler, out)
    if estcols.order is not None:
        assert 0 < len(estcols.order)
        first = True
        for order in estcols.order:
            if first:
                out.write(' ORDER BY ')
                first = False
            else:
                out.write(', ')
            compile_expression(bdb, order.expression, bql_compiler, out)
            if order.sense == ast.ORD_ASC:
                pass
            elif order.sense == ast.ORD_DESC:
                out.write(' DESC')
            else:
                assert False, 'Invalid order sense: %s' % (repr(order.sense),)
    if estcols.limit is not None:
        out.write(' LIMIT ')
        compile_expression(bdb, estcols.limit.limit, bql_compiler, out)
        if estcols.limit.offset is not None:
            out.write(' OFFSET ')
            compile_expression(bdb, estcols.limit.offset, bql_compiler, out)

def compile_estpaircols(bdb, estpaircols, out):
    assert isinstance(estpaircols, ast.EstPairCols)
    colno0_exp = 'c0.colno'     # XXX
    colno1_exp = 'c1.colno'     # XXX
    if not core.bayesdb_has_generator_default(bdb, estpaircols.generator):
        raise BQLError(bdb, 'No such generator: %s' % (estpaircols.generator,))
    generator_id = core.bayesdb_get_generator_default(bdb,
        estpaircols.generator)
    bql_compiler = BQLCompiler_2Col(generator_id, estpaircols.modelno,
        colno0_exp, colno1_exp)
    out.write('SELECT'
        ' %d AS generator_id, c0.name AS name0, c1.name AS name1' %
        (generator_id,))
    if len(estpaircols.columns) == 1 and estpaircols.columns[0][1] is None:
        # XXX Compatibility with existing queries.
        expression = estpaircols.columns[0][0]
        out.write(', ')
        compile_expression(bdb, expression, bql_compiler, out)
        out.write(' AS value')
    else:
        for exp, name in estpaircols.columns:
            out.write(', ')
            compile_expression(bdb, exp, bql_compiler, out)
            if name is not None:
                out.write(' AS %s' % (sqlite3_quote_name(name),))
    out.write(' FROM'
        ' bayesdb_generator AS g,'
        ' bayesdb_generator_column AS gc0, bayesdb_column AS c0,'
        ' bayesdb_generator_column AS gc1, bayesdb_column AS c1'
        ' WHERE g.id = %(generator_id)d'
        ' AND gc0.generator_id = g.id AND gc1.generator_id = g.id'
        ' AND c0.tabname = g.tabname AND c0.colno = gc0.colno'
        ' AND c1.tabname = g.tabname AND c1.colno = gc1.colno' %
              {'generator_id': generator_id})
    if estpaircols.subcolumns is not None:
        # XXX Would be nice not to duplicate these column lists.
        out.write(' AND c0.colno IN ')
        with compiling_paren(bdb, out, '(', ')'):
            compile_column_lists(bdb, generator_id, estpaircols.subcolumns,
                bql_compiler, out)
        out.write(' AND c1.colno IN ')
        with compiling_paren(bdb, out, '(', ')'):
            compile_column_lists(bdb, generator_id, estpaircols.subcolumns,
                bql_compiler, out)
    if estpaircols.condition is not None:
        out.write(' AND ')
        compile_expression(bdb, estpaircols.condition, bql_compiler, out)
    if estpaircols.order is not None:
        assert 0 < len(estpaircols.order)
        first = True
        for order in estpaircols.order:
            if first:
                out.write(' ORDER BY ')
                first = False
            else:
                out.write(', ')
            compile_expression(bdb, order.expression, bql_compiler, out)
            if order.sense == ast.ORD_ASC:
                pass
            elif order.sense == ast.ORD_DESC:
                out.write(' DESC')
            else:
                assert False, 'Invalid order sense: %s' % (repr(order.sense),)
    if estpaircols.limit is not None:
        out.write(' LIMIT ')
        compile_expression(bdb, estpaircols.limit.limit, bql_compiler, out)
        if estpaircols.limit.offset is not None:
            out.write(' OFFSET ')
            compile_expression(bdb, estpaircols.limit.offset, bql_compiler,
                out)

def compile_estpairrow(bdb, estpairrow, out):
    assert isinstance(estpairrow, ast.EstPairRow)
    if not core.bayesdb_has_generator_default(bdb, estpairrow.generator):
        raise BQLError(bdb, 'No such generator: %s' % (estpairrow.generator,))
    generator_id = core.bayesdb_get_generator_default(bdb,
        estpairrow.generator)
    rowid0_exp = 'r0._rowid_'
    rowid1_exp = 'r1._rowid_'
    bql_compiler = BQLCompiler_2Row(generator_id, estpairrow.modelno,
        rowid0_exp, rowid1_exp)
    out.write('SELECT %s AS rowid0, %s AS rowid1,' % (rowid0_exp, rowid1_exp))
    named = True
    compile_select_columns(bdb, estpairrow.columns, named, bql_compiler, out)
    out.write(' AS value')
    table_name = core.bayesdb_generator_table(bdb, generator_id)
    qt = sqlite3_quote_name(table_name)
    out.write(' FROM %s AS r0, %s AS r1' % (qt, qt))
    if estpairrow.condition is not None:
        out.write(' WHERE ')
        compile_expression(bdb, estpairrow.condition, bql_compiler, out)
    if estpairrow.order is not None:
        assert 0 < len(estpairrow.order)
        first = True
        for order in estpairrow.order:
            if first:
                out.write(' ORDER BY ')
                first = False
            else:
                out.write(', ')
            compile_expression(bdb, order.expression, bql_compiler, out)
            if order.sense == ast.ORD_ASC:
                pass
            elif order.sense == ast.ORD_DESC:
                out.write(' DESC')
            else:
                assert False, 'Invalid order sense: %s' % (repr(order.sense),)
    if estpairrow.limit is not None:
        out.write(' LIMIT ')
        compile_expression(bdb, estpairrow.limit.limit, bql_compiler, out)
        if estpairrow.limit.offset is not None:
            out.write(' OFFSET ')
            compile_expression(bdb, estpairrow.limit.offset, bql_compiler, out)

class BQLCompiler_None(object):
    def compile_bql(self, bdb, bql, out):
        # XXX Report source location.
        raise BQLError(bdb, 'Invalid context for BQL!')

class BQLCompiler_Const(object):
    def __init__(self, generator_id, modelno):
        assert isinstance(generator_id, int)
        self.generator_id = generator_id
        self.modelno = modelno

    def compile_bql(self, bdb, bql, out):
        assert ast.is_bql(bql)
        generator_id = self.generator_id
        if isinstance(bql, ast.ExpBQLPredProb):
            raise BQLError(bdb, 'Predictive probability is 1-row function,'
                ' not a constant.')
        elif isinstance(bql, ast.ExpBQLProb):
            compile_pdf_joint(bdb, generator_id, self.modelno, bql.targets,
                bql.constraints, self, out)
        elif isinstance(bql, ast.ExpBQLProbFn):
            raise BQLError(bdb, 'Probability of value at row is 1-column'
                ' function, not a constant.')
        elif isinstance(bql, ast.ExpBQLSim):
            raise BQLError(bdb, 'Row similarity is 1- or 2-row function,'
                ' not a constant.')
        elif isinstance(bql, ast.ExpBQLDepProb):
            compile_bql_2col_2(bdb, generator_id, self.modelno,
                'bql_column_dependence_probability',
                'Dependence probability', None, bql, self, out)
        elif isinstance(bql, ast.ExpBQLMutInf):
            compile_bql_2col_2(bdb, generator_id, self.modelno,
                'bql_column_mutual_information',
                'Mutual information', compile_mutinf_extra, bql, self, out)
        elif isinstance(bql, ast.ExpBQLCorrel):
            compile_bql_2col_2(bdb, generator_id, None,
                'bql_column_correlation',
                'Column correlation', None, bql, self, out)
        elif isinstance(bql, ast.ExpBQLCorrelPval):
            compile_bql_2col_2(bdb, generator_id, None,
                'bql_column_correlation_pvalue',
                'Column correlation pvalue', None, bql, self, out)
        elif isinstance(bql, (ast.ExpBQLPredict, ast.ExpBQLPredictConf)):
            raise BQLError(bdb, 'PREDICT is not allowed outside INFER.')
        else:
            assert False, 'Invalid BQL function: %s' % (repr(bql),)

class BQLCompiler_1Row(BQLCompiler_Const):
    def compile_bql(self, bdb, bql, out):
        assert ast.is_bql(bql)
        generator_id = self.generator_id
        rowid_col = '_rowid_'   # XXX Don't hard-code this.
        if isinstance(bql, ast.ExpBQLPredProb):
            if bql.column is None:
                raise BQLError(bdb, 'Predictive probability at row'
                    ' needs column.')
            colno = core.bayesdb_generator_column_number(bdb, generator_id,
                bql.column)
            out.write('bql_row_column_predictive_probability(%s, ' %
                (generator_id,))
            compile_expression(bdb, self.modelno, self, out)
            out.write(', %s, %s)' % (rowid_col, colno))
        elif isinstance(bql, ast.ExpBQLSim):
            if bql.condition is None:
                raise BQLError(bdb, 'Similarity as 1-row function needs row.')
            out.write('bql_row_similarity(%s, ' % (generator_id,))
            compile_expression(bdb, self.modelno, self, out)
            out.write(', _rowid_, ')
            with compiling_paren(bdb, out, '(', ')'):
                table_name = core.bayesdb_generator_table(bdb, generator_id)
                qt = sqlite3_quote_name(table_name)
                out.write('SELECT _rowid_ FROM %s WHERE ' % (qt,))
                compile_expression(bdb, bql.condition, self, out)
            if len(bql.column_lists) == 1 and \
               isinstance(bql.column_lists[0], ast.ColListAll):
                # We'll likely run up against SQLite's limit on the
                # number of arguments in this case.  Instead, let
                # bql_row_similarity find the columns.
                pass
            else:
                out.write(', ')
                compile_column_lists(bdb, generator_id, bql.column_lists, self,
                    out)
            out.write(')')
        else:
            super(BQLCompiler_1Row, self).compile_bql(bdb, bql, out)

class BQLCompiler_1Row_Infer(BQLCompiler_1Row):
    def compile_bql(self, bdb, bql, out):
        assert ast.is_bql(bql)
        generator_id = self.generator_id
        rowid_col = '_rowid_' # XXX Don't hard-code this.
        if isinstance(bql, ast.ExpBQLPredict):
            assert bql.column is not None
            if not core.bayesdb_generator_has_column(bdb, generator_id,
                    bql.column):
                generator = core.bayesdb_generator_name(bdb, generator_id)
                raise BQLError(bdb, 'No such column in generator %s: %s' %
                    (generator, bql.column))
            colno = core.bayesdb_generator_column_number(bdb, generator_id,
                bql.column)
            out.write('bql_predict(%d, ' % (generator_id,))
            compile_expression(bdb, self.modelno, self, out)
            out.write(', %d, %s, ' % (colno, rowid_col))
            compile_expression(bdb, bql.confidence, self, out)
            out.write(')')
        elif isinstance(bql, ast.ExpBQLPredictConf):
            assert bql.column is not None
            colno = core.bayesdb_generator_column_number(bdb, generator_id,
                bql.column)
            out.write('bql_predict_confidence(%d, ' % (generator_id,))
            compile_expression(bdb, self.modelno, self, out)
            out.write(', %d, %s)' % (colno, rowid_col))
        else:
            super(BQLCompiler_1Row_Infer, self).compile_bql(bdb, bql, out)

class BQLCompiler_2Row(object):
    def __init__(self, generator_id, modelno, rowid0_exp, rowid1_exp):
        assert isinstance(generator_id, int)
        assert isinstance(rowid0_exp, str)
        assert isinstance(rowid1_exp, str)
        self.generator_id = generator_id
        self.modelno = modelno
        self.rowid0_exp = rowid0_exp
        self.rowid1_exp = rowid1_exp

    def compile_bql(self, bdb, bql, out):
        assert ast.is_bql(bql)
        generator_id = self.generator_id
        if isinstance(bql, ast.ExpBQLProb):
            compile_pdf_joint(bdb, generator_id, self.modelno, bql.targets,
                bql.constraints, self, out)
        elif isinstance(bql, ast.ExpBQLProbFn):
            raise BQLError(bdb, 'Probability of value is 1-column function,'
                ' not 2-row function.')
        elif isinstance(bql, ast.ExpBQLPredProb):
            raise BQLError(bdb, 'Predictive probability is 1-row function,'
                ' not 2-row function.')
        elif isinstance(bql, ast.ExpBQLSim):
            if bql.condition is not None:
                raise BQLError(bdb, 'Similarity needs no row'
                    ' in 2-row context.')
            out.write('bql_row_similarity(%d, ' % (generator_id,))
            compile_expression(bdb, self.modelno, self, out)
            out.write(', %s, %s' % (self.rowid0_exp, self.rowid1_exp))
            if len(bql.column_lists) == 1 and \
               isinstance(bql.column_lists[0], ast.ColListAll):
                # We'll likely run up against SQLite's limit on the
                # number of arguments in this case.  Instead, let
                # bql_row_similarity find the columns.
                pass
            else:
                out.write(', ')
                compile_column_lists(bdb, generator_id, bql.column_lists, self,
                    out)
            out.write(')')
        elif isinstance(bql, ast.ExpBQLDepProb):
            raise BQLError(bdb, 'Dependence probability is 0-row function.')
        elif isinstance(bql, ast.ExpBQLMutInf):
            raise BQLError(bdb, 'Mutual information is 0-row function.')
        elif isinstance(bql, ast.ExpBQLCorrel):
            raise BQLError(bdb, 'Column correlation is 0-row function.')
        elif isinstance(bql, ast.ExpBQLCorrelPval):
            raise BQLError(bdb, 'Column correlation pvalue is 0-row function.')
        elif isinstance(bql, ast.ExpBQLPredict):
            raise BQLError(bdb, 'Predict is a 1-row function.')
        elif isinstance(bql, ast.ExpBQLPredictConf):
            raise BQLError(bdb, 'Predict is a 1-row function.')
        else:
            assert False, 'Invalid BQL function: %s' % (repr(bql),)

class BQLCompiler_1Col(object):
    def __init__(self, generator_id, modelno, colno_exp):
        assert isinstance(generator_id, int)
        assert isinstance(colno_exp, str)
        self.generator_id = generator_id
        self.modelno = modelno
        self.colno_exp = colno_exp

    def compile_bql(self, bdb, bql, out):
        assert ast.is_bql(bql)
        generator_id = self.generator_id
        if isinstance(bql, ast.ExpBQLProb):
            compile_pdf_joint(bdb, generator_id, self.modelno, bql.targets,
                bql.constraints, self, out)
        elif isinstance(bql, ast.ExpBQLProbFn):
            out.write('bql_column_value_probability(%d, ' % (generator_id,))
            compile_expression(bdb, self.modelno, self, out)
            out.write(', %s, ' % (self.colno_exp,))
            compile_expression(bdb, bql.value, self, out)
            compile_constraints(bdb, generator_id, bql.constraints, self, out)
            out.write(')')
        elif isinstance(bql, ast.ExpBQLPredProb):
            raise BQLError(bdb, 'Predictive probability makes sense'
                ' only at row.')
        elif isinstance(bql, ast.ExpBQLSim):
            raise BQLError(bdb, 'Similarity to row makes sense only at row.')
        elif isinstance(bql, ast.ExpBQLDepProb):
            compile_bql_2col_1(bdb, generator_id, self.modelno,
                'bql_column_dependence_probability',
                'Dependence probability', None, bql, self.colno_exp, self, out)
        elif isinstance(bql, ast.ExpBQLMutInf):
            compile_bql_2col_1(bdb, generator_id, self.modelno,
                'bql_column_mutual_information',
                'Mutual information',
                compile_mutinf_extra, bql, self.colno_exp, self, out)
        elif isinstance(bql, ast.ExpBQLCorrel):
            compile_bql_2col_1(bdb, generator_id, None,
                'bql_column_correlation',
                'Column correlation', None, bql, self.colno_exp, self, out)
        elif isinstance(bql, ast.ExpBQLCorrelPval):
            compile_bql_2col_1(bdb, generator_id, None,
                'bql_column_correlation_pvalue',
                'Column correlation pvalue', None, bql, self.colno_exp, self, out)
        elif isinstance(bql, ast.ExpBQLPredict):
            raise BQLError(bdb, 'Predict is a 1-row function.')
        elif isinstance(bql, ast.ExpBQLPredictConf):
            raise BQLError(bdb, 'Predict is a 1-row function.')
        else:
            assert False, 'Invalid BQL function: %s' % (repr(bql),)

class BQLCompiler_2Col(object):
    def __init__(self, generator_id, modelno, colno0_exp, colno1_exp):
        assert isinstance(generator_id, int)
        assert isinstance(colno0_exp, str)
        assert isinstance(colno1_exp, str)
        self.generator_id = generator_id
        self.modelno = modelno
        self.colno0_exp = colno0_exp
        self.colno1_exp = colno1_exp

    def compile_bql(self, bdb, bql, out):
        assert ast.is_bql(bql)
        generator_id = self.generator_id
        if isinstance(bql, ast.ExpBQLProb):
            compile_pdf_joint(bdb, generator_id, self.modelno, bql.targets,
                bql.constraints, self, out)
        elif isinstance(bql, ast.ExpBQLProbFn):
            raise BQLError(bdb, 'Probability of value is 1-column function.')
        elif isinstance(bql, ast.ExpBQLPredProb):
            raise BQLError(bdb, 'Predictive probability'
                ' is one-column function.')
        elif isinstance(bql, ast.ExpBQLSim):
            raise BQLError(bdb, 'Similarity to row makes sense only at row.')
        elif isinstance(bql, ast.ExpBQLDepProb):
            compile_bql_2col_0(bdb, generator_id, self.modelno,
                'bql_column_dependence_probability',
                'Dependence probability',
                None,
                bql, self.colno0_exp, self.colno1_exp, self, out)
        elif isinstance(bql, ast.ExpBQLMutInf):
            compile_bql_2col_0(bdb, generator_id, self.modelno,
                'bql_column_mutual_information',
                'Mutual Information',
                compile_mutinf_extra,
                bql, self.colno0_exp, self.colno1_exp, self, out)
        elif isinstance(bql, ast.ExpBQLCorrel):
            compile_bql_2col_0(bdb, generator_id, None,
                'bql_column_correlation',
                'Correlation',
                None,
                bql, self.colno0_exp, self.colno1_exp, self, out)
        elif isinstance(bql, ast.ExpBQLCorrelPval):
            compile_bql_2col_0(bdb, generator_id, None,
                'bql_column_correlation_pvalue',
                'Correlation pvalue',
                None,
                bql, self.colno0_exp, self.colno1_exp, self, out)
        elif isinstance(bql, ast.ExpBQLPredict):
            raise BQLError(bdb, 'Predict is a 1-row function.')
        elif isinstance(bql, ast.ExpBQLPredictConf):
            raise BQLError(bdb, 'Predict is a 1-row function.')
        else:
            assert False, 'Invalid BQL function: %s' % (repr(bql),)

def compile_pdf_joint(bdb, generator_id, modelno, targets, constraints,
        bql_compiler, out):
    out.write('bql_pdf_joint(%d, ' % (generator_id,))
    compile_expression(bdb, modelno, bql_compiler, out)
    for t_col, t_exp in targets:
        t_colno = core.bayesdb_generator_column_number(bdb, generator_id,
            t_col)
        assert t_colno != -1
        out.write(', %d, ' % (t_colno,))
        compile_expression(bdb, t_exp, bql_compiler, out)
    if 0 < len(constraints):
        out.write(', -1')
        compile_constraints(bdb, generator_id, constraints, bql_compiler, out)
    out.write(')')

def compile_constraints(bdb, generator_id, constraints, bql_compiler, out):
    for c_col, c_exp in constraints:
        if not core.bayesdb_generator_has_column(bdb, generator_id, c_col):
            raise BQLError(bdb, 'No such column in generator %s: %s' %
                (generator, c_col))
        c_colno = core.bayesdb_generator_column_number(bdb,
            generator_id, c_col)
        out.write(', %d, ' % (c_colno,))
        compile_expression(bdb, c_exp, bql_compiler, out)

def compile_column_lists(bdb, generator_id, column_lists, _bql_compiler, out):
    first = True
    for collist in column_lists:
        if first:
            first = False
        else:
            out.write(', ')
        if isinstance(collist, ast.ColListAll):
            colnos = core.bayesdb_generator_column_numbers(bdb, generator_id)
            out.write(', '.join(str(colno) for colno in colnos))
        elif isinstance(collist, ast.ColListLit):
            unknown = set()
            for column in collist.columns:
                if not core.bayesdb_generator_has_column(bdb, generator_id,
                        column):
                    unknown.add(column)
            if 0 < len(unknown):
                raise BQLError(bdb, 'No such columns in generator: %s' %
                    (repr(list(unknown)),))
            colnos = (core.bayesdb_generator_column_number(bdb, generator_id,
                    column)
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
            subwinders, subunwinders = subout.getwindings()
            with bayesdb_wind(bdb, subwinders, subunwinders):
                columns = bdb.sql_execute(subquery, subbindings).fetchall()
            subfirst = True
            for column in columns:
                if subfirst:
                    subfirst = False
                else:
                    out.write(', ')
                if len(column) != 1:
                    raise BQLError(bdb, 'ESTIMATE * FROM COLUMNS OF subquery'
                        ' returned multi-cell rows.')
                if not isinstance(column[0], unicode):
                    raise BQLError(bdb, 'ESTIMATE * FROM COLUMNS OF subquery'
                        ' returned non-string.')
                colno = core.bayesdb_generator_column_number(bdb, generator_id,
                    column[0])
                out.write('%d' % (colno,))
        else:
            assert False, 'Invalid column list: %s' % (repr(collist),)

def compile_bql_2col_2(bdb, generator_id, modelno, bqlfn, desc, extra, bql,
        bql_compiler, out):
    if bql.column0 is None:
        raise BQLError(bdb, desc + ' needs exactly two columns.')
    if bql.column1 is None:
        raise BQLError(bdb, desc + ' needs exactly two columns.')
    colno0 = core.bayesdb_generator_column_number(bdb, generator_id,
        bql.column0)
    colno1 = core.bayesdb_generator_column_number(bdb, generator_id,
        bql.column1)
    out.write('%s(%d, ' % (bqlfn, generator_id))
    if modelno is not None:
        compile_expression(bdb, modelno, bql_compiler, out)
        out.write(', ')
    out.write('%s, %s' % (colno0, colno1))
    if extra:
        extra(bdb, generator_id, bql, bql_compiler, out)
    out.write(')')

def compile_bql_2col_1(bdb, generator_id, modelno, bqlfn, desc, extra, bql,
        colno1_exp, bql_compiler, out):
    if bql.column0 is None:
        raise BQLError(bdb, desc + ' needs at least one column.')
    if bql.column1 is not None:
        raise BQLError(bdb, desc + ' needs at most one column.')
    colno0 = core.bayesdb_generator_column_number(bdb, generator_id,
        bql.column0)
    out.write('%s(%d, ' % (bqlfn, generator_id))
    if modelno is not None:
        compile_expression(bdb, modelno, bql_compiler, out)
        out.write(', ')
    out.write('%s, %s' % (colno0, colno1_exp))
    if extra:
        extra(bdb, generator_id, bql, bql_compiler, out)
    out.write(')')

def compile_bql_2col_0(bdb, generator_id, modelno, bqlfn, desc, extra, bql,
        colno0_exp, colno1_exp, bql_compiler, out):
    if bql.column0 is not None:
        raise BQLError(bdb, desc + ' needs no columns.')
    if bql.column1 is not None:
        raise BQLError(bdb, desc + ' needs no columns.')
    out.write('%s(%d, ' % (bqlfn, generator_id))
    if modelno is not None:
        compile_expression(bdb, modelno, bql_compiler, out)
        out.write(', ')
    out.write('%s, %s' % (colno0_exp, colno1_exp))
    if extra:
        extra(bdb, generator_id, bql, bql_compiler, out)
    out.write(')')

def compile_mutinf_extra(bdb, generator_id, bql, bql_compiler, out):
    out.write(', ')
    if bql.nsamples:
        compile_expression(bdb, bql.nsamples, bql_compiler, out)
    else:
        out.write('NULL')

def compile_nobql_expression(bdb, exp, out):
    bql_compiler = BQLCompiler_None()
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
            assert False, 'Invalid directive: %s' % (repr(d),)
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
    ast.OP_NEGATE:      '- %s',
    ast.OP_PLUSID:      '+ %s',
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
        assert False, 'Invalid literal: %s' % (repr(lit),)

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
