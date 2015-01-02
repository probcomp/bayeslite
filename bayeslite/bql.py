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

def compile_bql(bdb, phrases, out):
    for phrase in phrases:
        if isinstance(phrase, ast.Select):
            compile_select(bdb, phrase, out)
        else:
            assert False        # XXX
        out.write(';')

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
    else:
        assert False            # XXX

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
