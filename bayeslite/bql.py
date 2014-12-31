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

def compile_select(bdb, select, out):
    assert isinstance(select, ast.Select)
    out.write('select')
    if select.quantifier == ast.SELQUANT_DISTINCT:
        out.write(' distinct')
    elif select.quantifier != ast.SELQUANT_ALL:
        raise ValueError('Invalid select: %s' % (select,))
    if isinstance(select.output, ast.SelCols):
        assert 0 < len(select.output.columns)
        first = True
        for selcol in select.output.columns:
            if first:
                out.write(' ')
                first = False
            else:
                out.write(', ')
            compile_select_column(bdb, selcol, out)
    else:
        # Must be a BQL function.
        out.write(' ')
        if select.tables is None:
            raise ValueError('BQL select without table: %s' % (select,))
        elif len(select.tables) != 1:
            assert 1 < len(select.tables)
            raise ValueError('BQL select with >1 table: %s' % (select,))
        if not isinstance(select.tables[0].table, str): # XXX name
            raise ValueError('Subquery in BQL select: %s' % (select,))
        table_id = core.bayesdb_table_id(bdb, select.tables[0].table)
        rowid_col = 'rowid'     # XXX Don't hard-code this.
        if isinstance(select.output, ast.SelBQLPredProb):
            colno = core.bayesdb_column_number(bdb, table_id,
                select.output.column)
            out.write('row_column_predictive_probability(%s, %s, %s)' %
                (table_id, rowid_col, colno))
        else:
            raise ValueError('Unknown BQL function in select: %s' % (select,))
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
                compile_name(bdb, seltabl.name, out)
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
            if order.sense == ORD_ASC:
                pass
            elif order.sense == ORD_DESC:
                out.write(' desc')
            else:
                assert order.sense == ORD_ASC or order.sense == ORD_DESC
    if select.limit is not None:
        out.write(' limit ')
        compile_expression(bdb, select.limit.limit, out)
        if select.limit.offset is not None:
            out.write(' offset ')
            compile_expression(bdb, select.limit.offset, out)
    out.write(';')

def compile_select_column(bdb, selcol, out):
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
        assert isinstance(selcol, SelColAll) or isinstance(selcol, SelColExp)

def compile_expression(bdb, exp, out):
    if isinstance(exp, ast.ExpLit):
        compile_literal(bdb, exp.value, out)
    elif isinstance(exp, ast.ExpCol):
        compile_table_column(bdb, exp.table, exp.column, out)
    else:
        assert False            # XXX

def compile_literal(bdb, lit, out):
    if isinstance(lit, ast.LitNull):
        out.write('NULL')
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
    compile_table_name(bdb, table_name, out)
    out.write('.')
    compile_column_name(bdb, table_name, column_name, out)

def compile_column_name(bdb, _table_name, column_name, out):
    compile_name(bdb, column_name, out)
