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

from collections import namedtuple

QueryAction = namedtuple('QueryAction', [
    'action',                   # QACT_*
    'query',                    # Select or Infer or ... XXX
])
QACT_FREQ = 'freq'
QACT_HIST = 'hist'
QACT_SUMMARIZE = 'summarize'
QACT_PLOT = 'plot'

def is_query(phrase):
    if isinstance(phrase, Select):      return True
    return False

Select = namedtuple('Select', [
    'quantifier',               # SELQUANT_*
    'columns',                  # [(SelCol or SelBQL)*]
    'tables',                   # [SelTab] or None (scalar)
    'condition',                # Exp* or None (unconditional)
    'group',                    # [Exp*] or None (unaggregated)
    'order',                    # [Ord] or None (unordered)
    'limit',                    # Lim or None (unlimited)
])

SELQUANT_DISTINCT = 'distinct'
SELQUANT_ALL = 'all'

SelColAll = namedtuple('SelColAll', [
    'table',                    # XXX name
])
SelColExp = namedtuple('SelColExp', [
    'expression',               # Exp*
    'name',                     # XXX name
])

SelBQLPredProb = namedtuple('SelBQLPredProb', ['column'])
SelBQLProb = namedtuple('SelBQLProb', ['column', 'value'])
SelBQLTypRow = namedtuple('SelBQLTypRow', []) # XXX Accept rowid?
SelBQLTypCol = namedtuple('SelBQLTypCol', ['column'])
SelBQLSim = namedtuple('SelBQLSim', ['rowid', 'column_lists'])
SelBQLDepProb = namedtuple('SelBQLDepProb', ['column0', 'column1'])
SelBQLMutInf = namedtuple('SelBQLMutInf', ['column0', 'column1'])
SelBQLCorrel = namedtuple('SelBQLCorrel', ['column0', 'column1'])

ColListAll = namedtuple('ColListAll', [])
ColListLit = namedtuple('ColListLit', ['columns'])
ColListSub = namedtuple('ColListSub', ['query']) # subquery
ColListSav = namedtuple('ColListSav', ['name']) # saved

SelTab = namedtuple('SelTab', [
    'table',                    # XXX subquery or XXX name
    'name',                     # XXX name
])

Ord = namedtuple('Ord', ['expression', 'sense'])
ORD_ASC = True
ORD_DESC = False

Lim = namedtuple('Lim', ['limit', 'offset'])

ExpLit = namedtuple('ExpLit', ['value'])
ExpCol = namedtuple('ExpCol', ['table', 'column'])
# XXX We will need some kind of type-checking to distinguish
# subqueries for column lists from subqueries for table rows.
ExpSub = namedtuple('ExpSub', ['query'])
ExpCollate = namedtuple('ExpCollate', ['expression', 'collation'])
ExpApp = namedtuple('ExpApp', ['operator', 'operands'])
ExpOp = namedtuple('ExpOp', ['operator', 'operands'])

def op(operator, *operands):
    return ExpOp(operator, operands)

OP_BOOLOR = 'BOOLOR'
OP_BOOLAND = 'BOOLAND'
OP_BOOLNOT = 'BOOLNOT'
OP_IS = 'IS'
OP_MATCH = 'MATCH'
OP_LIKE = 'LIKE'
OP_LIKE_ESC = 'LIKE_ESC'
OP_BETWEEN = 'BETWEEN'
OP_NOTBETWEEN = 'NOTBETWEEN'
OP_IN = 'IN'
OP_ISNULL = 'ISNULL'
OP_NOTNULL = 'NOTNULL'
OP_NEQ = 'NEQ'
OP_EQ = 'EQ'
OP_LT = 'LT'
OP_LEQ = 'LEQ'
OP_GEQ = 'GEQ'
OP_GT = 'GT'
OP_BITAND = 'BITAND'
OP_BITIOR = 'BITIOR'
OP_LSHIFT = 'LSHIFT'
OP_RSHIFT = 'RSHIFT'
OP_ADD = 'ADD'
OP_SUB = 'SUB'
OP_MUL = 'MUL'
OP_DIV = 'DIV'
OP_REM = 'REM'
OP_CONCAT = 'CONCAT'
OP_BITNOT = 'BITNOT'

LitNull = namedtuple('LitNull', ['value'])
LitInt = namedtuple('LitInt', ['value'])
LitFloat = namedtuple('LitFloat', ['value'])
LitString = namedtuple('LitString', ['value'])
