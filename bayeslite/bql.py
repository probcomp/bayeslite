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

def execute_phrase(bdb, phrase):
    if ast.is_query(phrase):
        out = StringIO.StringIO()
        compile_query(bdb, phrase, out)
        return bdb.sqlite.execute(out.getvalue())
    if isinstance(phrase, ast.CreateBtableCSV):
        # XXX Codebook?
        core.bayesdb_import_csv_file(bdb, phrase.name, phrase.file)
        return []
    elif isinstance(phrase, ast.InitModels):
        table_id = core.bayesdb_table_id(bdb, phrase.btable)
        nmodels = phrase.nmodels
        config = phrase.config
        core.bayesdb_models_initialize(bdb, table_id, nmodels, config)
        return []
    elif isinstance(phrase, ast.AnalyzeModels):
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
    else:
        assert False            # XXX

def compile_query(bdb, query, out):
    if isinstance(query, ast.Select):
        compile_select(bdb, query, out)
    else:
        assert False        # XXX

def compile_subquery(bdb, query, _bql_compiler, out):
    # XXX Do something with the BQL compiler so we can refer to
    # BQL-related quantities in the subquery?
    compile_query(bdb, query, out)

def compile_select(bdb, select, out):
    assert isinstance(select, ast.Select)
    out.write('select')
    if select.quantifier == ast.SELQUANT_DISTINCT:
        out.write(' distinct')
    else:
        assert select.quantifier == ast.SELQUANT_ALL
    compile_select_columns(bdb, select, out)
    if select.tables is not None:
        assert 0 < len(select.tables)
        first = True
        for seltab in select.tables:
            if first:
                out.write(' from ')
                first = False
            else:
                out.write(', ')
            compile_table_name(bdb, seltab.table, out) # XXX subquery
            if seltab.name is not None:
                out.write(' as ')
                compile_name(bdb, seltab.name, out)
    if select.condition is not None:
        out.write(' where ')
        compile_row_expression(bdb, select.condition, select, out)
    if select.group is not None:
        assert 0 < len(select.group)
        first = True
        for key in select.group:
            if first:
                out.write(' group by ')
                first = False
            else:
                out.write(', ')
            compile_row_expression(bdb, key, select, out)
    if select.order is not None:
        assert 0 < len(select.order)
        first = True
        for order in select.order:
            if first:
                out.write(' order by ')
                first = False
            else:
                out.write(', ')
            compile_row_expression(bdb, order.expression, select, out)
            if order.sense == ast.ORD_ASC:
                pass
            elif order.sense == ast.ORD_DESC:
                out.write(' desc')
            else:
                assert False    # XXX
    if select.limit is not None:
        out.write(' limit ')
        compile_row_expression(bdb, select.limit.limit, select, out)
        if select.limit.offset is not None:
            out.write(' offset ')
            compile_row_expression(bdb, select.limit.offset, select, out)

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
        bql_compiler = BQLCompiler_Row(select)
        compile_expression(bdb, selcol.expression, bql_compiler, out)
        if selcol.name is not None:
            out.write(' as ')
            compile_name(bdb, selcol.name, out)
    else:
        assert False            # XXX

class BQLCompiler_Row(object):
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
            out.write('row_column_predictive_probability(%s, %s, %s)' %
                (table_id, rowid_col, colno))
        elif isinstance(bql, ast.ExpBQLProb):
            colno = core.bayesdb_column_number(bdb, table_id, bql.column)
            out.write('column_value_probability(%s, %s, ' % (table_id, colno))
            compile_expression(bdb, bql.value, self, out)
            out.write(')')
        elif isinstance(bql, ast.ExpBQLTyp):
            if bql.column is None:
                out.write('row_typicality(%s, rowid)' % (table_id,))
            else:
                colno = core.bayesdb_column_number(bdb, table_id, bql.column)
                out.write('column_typicality(%s, %s)' % (table_id, colno))
        elif isinstance(bql, ast.ExpBQLSim):
            out.write('row_similarity(%s, rowid, ' % (table_id,))
            compile_expression(bdb, bql.rowid, self, out)
            out.write(', ')
            compile_column_lists(bdb, table_id, bql.column_lists, self, out)
            out.write(')')
        elif isinstance(bql, ast.ExpBQLDepProb):
            compile_bql_2col_2(bdb, table_id, 'column_dependence_probability',
                bql, out)
        elif isinstance(bql, ast.ExpBQLMutInf):
            compile_bql_2col_2(bdb, table_id, 'column_mutual_information', bql,
                out)
        elif isinstance(bql, ast.ExpBQLCorrel):
            compile_bql_2col_2(bdb, table_id, 'column_correlation', bql, out)
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
            raise NotImplementedError('column list subqueries')
        elif isinstance(collist, ast.ColListSav):
            raise NotImplementedError('saved column lists')
        else:
            assert False        # XXX

def compile_bql_2col_2(bdb, table_id, bqlfn, bql, out):
    assert bql.column0 is not None
    assert bql.column1 is not None
    colno0 = core.bayesdb_column_number(bdb, table_id, bql.column0)
    colno1 = core.bayesdb_column_number(bdb, table_id, bql.column1)
    out.write('%s(%s, %s, %s)' % (bqlfn, table_id, colno0, colno1))

def compile_row_expression(bdb, exp, query, out):
    bql_compiler = BQLCompiler_Row(query)
    compile_expression(bdb, exp, bql_compiler, out)

def compile_expression(bdb, exp, bql_compiler, out):
    if isinstance(exp, ast.ExpLit):
        compile_literal(bdb, exp.value, out)
    elif isinstance(exp, ast.ExpCol):
        compile_table_column(bdb, exp.table, exp.column, out)
    elif isinstance(exp, ast.ExpSub):
        with compiling_paren(bdb, out, '(', ')'):
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
            with compiling_paren(bdb, out, '(', ')'):
                compile_subquery(bdb, exp.query, bql_compiler, out)
    elif isinstance(exp, ast.ExpCast):
        with compiling_paren(bdb, out, 'CAST(', ')'):
            compile_expression(bdb, exp.expression, bql_compiler, out)
            out.write(' AS ')
            compile_type(bdb, exp.type, out)
    elif isinstance(exp, ast.ExpExists):
        with compiling_paren(bdb, out, '(', ')'):
            out.write('EXISTS ')
            with compiling_paren(bdb, out, '(', ')'):
                compile_subquery(bdb, exp.query, bql_compiler, out)
    elif isinstance(exp, ast.ExpCase):
        with compiling_paren(bdb, out, '(', ')'):
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
        out.write('null')
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
