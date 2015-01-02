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

import bayeslite.ast as ast
import bayeslite.core as core

def compile_query(bdb, query, out):
    if isinstance(query, ast.Select):
        compile_select(bdb, query, out)
    else:
        assert False        # XXX

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
        compile_expression(bdb, select.condition, out)
    if select.group is not None:
        assert 0 < len(select.group)
        first = True
        for key in select.group:
            if first:
                out.write(' group by ')
                first = False
            else:
                out.write(', ')
            compile_expression(bdb, key, out)
    if select.order is not None:
        assert 0 < len(select.order)
        first = True
        for order in select.order:
            if first:
                out.write(' order by ')
                first = False
            else:
                out.write(', ')
            compile_expression(bdb, order.expression, out)
            if order.sense == ast.ORD_ASC:
                pass
            elif order.sense == ast.ORD_DESC:
                out.write(' desc')
            else:
                assert order.sense == ast.ORD_ASC or \
                    order.sense == ast.ORD_DESC
    if select.limit is not None:
        out.write(' limit ')
        compile_expression(bdb, select.limit.limit, out)
        if select.limit.offset is not None:
            out.write(' offset ')
            compile_expression(bdb, select.limit.offset, out)

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
        compile_expression(bdb, selcol.expression, out)
        if selcol.name is not None:
            out.write(' as ')
            compile_name(bdb, selcol.name, out)
    else:
        # Must be a BQL function.
        if select.tables is None:
            raise ValueError('BQL select without table: %s' % (select,))
        elif len(select.tables) != 1:
            assert 1 < len(select.tables)
            raise ValueError('BQL select with >1 table: %s' % (select,))
        if not isinstance(select.tables[0].table, str): # XXX name
            raise ValueError('Subquery in BQL select: %s' % (select,))
        table_id = core.bayesdb_table_id(bdb, select.tables[0].table)
        rowid_col = 'rowid'     # XXX Don't hard-code this.
        if isinstance(selcol, ast.SelBQLPredProb):
            colno = core.bayesdb_column_number(bdb, table_id, selcol.column)
            out.write('row_column_predictive_probability(%s, %s, %s)' %
                (table_id, rowid_col, colno))
        elif isinstance(selcol, ast.SelBQLProb):
            colno = core.bayesdb_column_number(bdb, table_id, selcol.column)
            out.write('column_value_probability(%s, %s, ' % (table_id, colno))
            compile_expression(bdb, selcol.value, out)
            out.write(')')
        elif isinstance(selcol, ast.SelBQLTypRow):
            out.write('row_typicality(%s, rowid)' % (table_id,))
        elif isinstance(selcol, ast.SelBQLTypCol):
            colno = core.bayesdb_column_number(bdb, table_id, selcol.column)
            out.write('column_typicality(%s, %s)' % (table_id, colno))
        elif isinstance(selcol, ast.SelBQLSim):
            out.write('row_similarity(%s, rowid, ' % (table_id,))
            compile_expression(bdb, selcol.rowid, out)
            out.write(', ')
            compile_column_lists(bdb, table_id, selcol.column_lists, out)
            out.write(')')
        elif isinstance(selcol, ast.SelBQLDepProb):
            compile_bql_2col(bdb, table_id, 'column_dependence_probability',
                selcol, out)
        elif isinstance(selcol, ast.SelBQLMutInf):
            compile_bql_2col(bdb, table_id, 'column_mutual_information',
                selcol, out)
        elif isinstance(selcol, ast.SelBQLCorrel):
            compile_bql_2col(bdb, table_id, 'column_correlation', selcol, out)
        else:
            assert False

def compile_column_lists(bdb, table_id, column_lists, out):
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
            assert False

def compile_bql_2col(bdb, table_id, bqlfn, selcol, out):
    colno0 = core.bayesdb_column_number(bdb, table_id, selcol.column0)
    colno1 = core.bayesdb_column_number(bdb, table_id, selcol.column1)
    out.write('%s(%s, %s, %s)' % (bqlfn, table_id, colno0, colno1))

def compile_expression(bdb, exp, out):
    if isinstance(exp, ast.ExpLit):
        compile_literal(bdb, exp.value, out)
    elif isinstance(exp, ast.ExpCol):
        compile_table_column(bdb, exp.table, exp.column, out)
    elif isinstance(exp, ast.ExpSub):
        out.write('(')
        compile_query(bdb, exp.query, out)
        out.write(')')
    elif isinstance(exp, ast.ExpApp):
        compile_name(bdb, exp.operator, out)
        out.write('(')
        first = True
        for operand in exp.operands:
            if first:
                first = False
            else:
                out.write(', ')
            compile_expression(bdb, operand, out)
        out.write(')')
    elif isinstance(exp, ast.ExpOp):
        compile_op(bdb, exp, out)
    elif isinstance(exp, ast.ExpCollate):
        out.write('(')
        compile_expression(bdb, exp.expression, out)
        out.write(' COLLATE ')
        compile_name(bdb, exp.collation, out)
        out.write(')')
    elif isinstance(exp, ast.ExpCast):
        out.write('CAST(')
        compile_expression(bdb, exp.expression, out)
        out.write(' AS ')
        compile_type(bdb, exp.type, out)
        out.write(')')
    else:
        assert False            # XXX

def compile_op(bdb, op, out):
    out.write('(')
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
            compile_expression(bdb, op.operands[r], out)
            r += 1
        else:
            assert d == '%' or d == 's'
        i = j
    assert r == len(op.operands)
    out.write(')')

operator_fmts = {
    ast.OP_BOOLOR:      '%s OR %s',
    ast.OP_BOOLAND:     '%s AND %s',
    ast.OP_BOOLNOT:     'NOT %s',
    ast.OP_IS:          '%s IS %s',
    ast.OP_MATCH:       '%s MATCH %s',
    ast.OP_LIKE:        '%s LIKE %s',
    ast.OP_LIKE_ESC:    '%s LIKE %s ESCAPE %s',
    ast.OP_BETWEEN:     '%s BETWEEN %s AND %s',
    ast.OP_NOTBETWEEN:  '%s NOT BETWEEN %s AND %s',
    ast.OP_IN:          '%s IN %s',
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
        out.write("'" + lit.value.replace("'", "''") + "'")
    else:
        assert False            # XXX

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
        out.write('(')
        first = True
        for a in type.args:
            if first:
                first = False
            else:
                out.write(', ')
            assert isinstance(a, int)
            out.write(str(a))
        out.write(')')
