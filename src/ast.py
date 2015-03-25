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

Parametrized = namedtuple('Parametrized', [
    'phrase',                   # command or query
    'n_numpar',                 # number of numeric parameters
    'nampar_map',               # map from parameter name to number
])

### Transactions

Begin = namedtuple('Begin', [])
Rollback = namedtuple('Rollback', [])
Commit = namedtuple('Commit', [])

### SQL Data Definition Language subset

# XXX Pass through other SQL DDL and DML commands.

CreateTableAs = namedtuple('CreateTableAs', [
    # XXX Database name, &c.
    'temp',                     # boolean
    'ifnotexists',              # boolean
    'name',                     # XXX name
    'query',                    # query
])
CreateTableSim = namedtuple('CreateTableSim', [
    # XXX Database name, &c.
    'temp',                     # boolean
    'ifnotexists',              # boolean
    'name',                     # XXX name
    'simulation',               # Simulate
])
DropTable = namedtuple('DropTable', [
    # XXX Database name, &c.
    'ifexists',
    'name'
])

### BQL Model Definition Language

CreateGen = namedtuple('CreateGen', [
    'name',                     # XXX name
    'ifnotexists',              # boolean
    'table',                    # XXX name
    'metamodel',                # XXX name
    'schema',                   # GenSchema
])
DropGen = namedtuple('DropGen', [
    'ifexists',                 # boolean
    'name',                     # XXX name
])
RenameGen = namedtuple('RenameGen', [
    'oldname',                  # XXX name
    'newname',                  # XXX name
])

GenSchema = namedtuple('GenSchema', [
    'columns'                   # [GenColumn]
])
GenColumn = namedtuple('GenColumn', [
    'name',                     # XXX name
    'stattype',                 # XXX name
])

### BQL Model Analysis Language

InitModels = namedtuple('InitModels', [
    'ifnotexists',
    'generator',
    'nmodels',
    'config',
])
AnalyzeModels = namedtuple('AnalyzeModels', [
    'generator',
    'modelnos',
    'iterations',
    'seconds',
    'wait',
])
DropModels = namedtuple('DropModels', [
    'generator',
    'modelnos',
])

Simulate = namedtuple('Simulate', [
    'columns',                  # [XXX name]
    'generator',                # XXX name
    'constraints',              # [(XXX name, Exp*)]
    'nsamples',                 # Exp* or None
])

def is_query(phrase):
    if isinstance(phrase, Select):      return True
    if isinstance(phrase, Estimate):    return True
    if isinstance(phrase, EstCols):     return True
    if isinstance(phrase, EstPairCols): return True
    if isinstance(phrase, EstPairRow):  return True
    # SIMULATE is *not* a normal query: it can appear only on the
    # right-hand side of `CREATE TABLE foo AS ...'.
    return False

Select = namedtuple('Select', [
    'quantifier',               # SELQUANT_*
    'columns',                  # [SelCol*]
    'tables',                   # [SelTab] or None (scalar)
    'condition',                # Exp* or None (unconditional)
    'grouping',                 # Grouping or None
    'order',                    # [Ord] or None (unordered)
    'limit',                    # Lim or None (unlimited)
])

Estimate = namedtuple('Estimate', [
    'quantifier',               # SELQUANT_*
    'columns',                  # [SelCol*]
    'generator',                # XXX name
    'condition',                # Exp* or None (unconditional)
    'grouping',                 # Grouping or None
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

SelTab = namedtuple('SelTab', [
    'table',                    # XXX subquery or XXX name
    'name',                     # XXX name
])

EstCols = namedtuple('EstCols', [
    'columns',                  # [(Exp*, XXX name)]
    'generator',                # XXX name
    'condition',                # Exp* or None (unconditional)
    'order',                    # [Ord] or None (unordered)
    'limit',                    # Lim or None (unlimited),
    'save_name',                # XXX name or None (don't save)
])

EstPairCols = namedtuple('EstPairCols', [
    'columns',                  # Exp*
    'generator',                # XXX name
    'subcolumns',               # ColList* or None
    'condition',                # Exp* or None (unconditional)
    'order',                    # [Ord] or None (unordered)
    'limit',                    # Lim or None (unlimited),
    'save_name',                # XXX name or None (don't save)
])

EstPairRow = namedtuple('EstPairRow', [
    'expression',               # Exp*
    'generator',                # XXX name
    'condition',                # Exp* or None (unconditional)
    'order',                    # [Ord] or None (unordered)
    'limit',                    # Lim or None (unlimited),
    'save_name',                # XXX name or None (don't save)
])

ColListAll = namedtuple('ColListAll', [])
ColListLit = namedtuple('ColListLit', ['columns'])
ColListSub = namedtuple('ColListSub', ['query']) # subquery
ColListSav = namedtuple('ColListSav', ['name']) # saved

Grouping = namedtuple('Grouping', ['keys', 'condition'])

Ord = namedtuple('Ord', ['expression', 'sense'])
ORD_ASC = True
ORD_DESC = False

Lim = namedtuple('Lim', ['limit', 'offset'])

ExpLit = namedtuple('ExpLit', ['value'])
ExpNumpar = namedtuple('ExpNumpar', ['number'])
ExpNampar = namedtuple('ExpNampar', ['number', 'name'])
ExpCol = namedtuple('ExpCol', ['table', 'column'])
# XXX We will need some kind of type-checking to distinguish
# subqueries for column lists from subqueries for table rows.
ExpSub = namedtuple('ExpSub', ['query'])
ExpCollate = namedtuple('ExpCollate', ['expression', 'collation'])
ExpIn = namedtuple('ExpIn', ['expression', 'positive', 'query'])
ExpCast = namedtuple('ExpCast', ['expression', 'type'])
ExpExists = namedtuple('ExpExists', ['query'])
ExpApp = namedtuple('ExpApp', ['distinct', 'operator', 'operands'])
ExpAppStar = namedtuple('ExpAppStar', ['operator'])
# Else clause is called `otherwise' because we can't use a Python keyword.
ExpCase = namedtuple('ExpCase', ['key', 'whens', 'otherwise'])
ExpOp = namedtuple('ExpOp', ['operator', 'operands'])

def op(operator, *operands):
    return ExpOp(operator, operands)

OP_BOOLOR = 'BOOLOR'
OP_BOOLAND = 'BOOLAND'
OP_BOOLNOT = 'BOOLNOT'
OP_IS = 'IS'
OP_ISNOT = 'ISNOT'
OP_LIKE = 'LIKE'
OP_NOTLIKE = 'NOTLIKE'
OP_LIKE_ESC = 'LIKE_ESC'
OP_NOTLIKE_ESC = 'NOTLIKE_ESC'
OP_GLOB = 'GLOB'
OP_NOTGLOB = 'NOTGLOB'
OP_GLOB_ESC = 'GLOB_ESC'
OP_NOTGLOB_ESC = 'NOTGLOB_ESC'
OP_REGEXP = 'REGEXP'
OP_NOTREGEXP = 'NOTREGEXP'
OP_REGEXP_ESC = 'REGEXP_ESC'
OP_NOTREGEXP_ESC = 'NOTREGEXP_ESC'
OP_MATCH = 'MATCH'
OP_NOTMATCH = 'NOTMATCH'
OP_MATCH_ESC = 'MATCH_ESC'
OP_NOTMATCH_ESC = 'NOTMATCH_ESC'
OP_BETWEEN = 'BETWEEN'
OP_NOTBETWEEN = 'NOTBETWEEN'
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

ExpBQLPredProb = namedtuple('ExpBQLPredProb', ['column'])
ExpBQLProb = namedtuple('ExpBQLProb', ['column', 'value'])
ExpBQLTyp = namedtuple('ExpBQLTyp', ['column'])
ExpBQLSim = namedtuple('ExpBQLSim', ['condition', 'column_lists'])
ExpBQLDepProb = namedtuple('ExpBQLDepProb', ['column0', 'column1'])
ExpBQLMutInf = namedtuple('ExpBQLMutInf', ['column0', 'column1', 'nsamples'])
ExpBQLCorrel = namedtuple('ExpBQLCorrel', ['column0', 'column1'])
ExpBQLInfer = namedtuple('ExpBQLInfer', ['column', 'confidence'])

def is_bql(exp):
    if isinstance(exp, ExpBQLPredProb): return True
    if isinstance(exp, ExpBQLProb):     return True
    if isinstance(exp, ExpBQLTyp):      return True
    if isinstance(exp, ExpBQLSim):      return True
    if isinstance(exp, ExpBQLDepProb):  return True
    if isinstance(exp, ExpBQLMutInf):   return True
    if isinstance(exp, ExpBQLCorrel):   return True
    if isinstance(exp, ExpBQLInfer):    return True
    return False

LitNull = namedtuple('LitNull', ['value'])
LitInt = namedtuple('LitInt', ['value'])
LitFloat = namedtuple('LitFloat', ['value'])
LitString = namedtuple('LitString', ['value'])

Type = namedtuple('Type', ['names', 'args'])
