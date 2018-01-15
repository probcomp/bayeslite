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

CreateTabAs = namedtuple('CreateTabAs', [
    # XXX Database name, &c.
    'temp',                     # boolean
    'ifnotexists',              # boolean
    'name',                     # XXX name
    'query',                    # query
])
CreateTabCsv = namedtuple('CreateTabCsv', [
    # XXX Database name, &c.
    'temp',                     # boolean
    'ifnotexists',              # boolean
    'name',                     # XXX name
    'csv',                      # csv filename string
])
DropTab = namedtuple('DropTab', [
    # XXX Database name, &c.
    'ifexists',
    'name'
])
AlterTab = namedtuple('AlterTab', [
    # XXX Database name, &c.
    'table',                    # XXX name
    'commands',                 # AlterTab*
])
AlterTabRenameTab = namedtuple('AlterTabRenameTab', [
    'name',                     # XXX name
])
AlterTabRenameCol = namedtuple('AlterTabRenameCol', [
    'old',                      # XXX name
    'new',                      # XXX name
])

### BQL Model Definition Language
GuessSchema = namedtuple('GuessSchema', [
    'table'
])
CreatePop = namedtuple('CreatePop', [
    'ifnotexists',
    'name',
    'table',
    'schema',
])
DropPop = namedtuple('DropPop', [
    'ifexists',
    'name',                     # XXX name
])
PopModelVars = namedtuple('PopModelVars', [
    'names',                    # XXX names
    'stattype',
])
PopGuessVars = namedtuple('PopGuessVars', [
    'names',                    # XXX names
])
PopIgnoreVars = namedtuple('PopIgnoreVars', [
    'names',                    # XXX names
])
AlterPop = namedtuple('AlterPop', [
    'population',               # XXX name
    'commands',                 # AlterPop*
])
AlterPopRenamePop = namedtuple('AlterPopRenamePop', [
    'name',                     # XXX name
])
AlterPopAddVar = namedtuple('AlterPopAddVar', [
    'name',                     # XXX name
    'stattype',
])
AlterPopStatType = namedtuple('AlterGenRenameGen', [
    'names',
    'stattype',
])

CreateGen = namedtuple('CreateGen', [
    'name',                     # XXX name
    'ifnotexists',              # boolean
    'population',               # XXX name
    'backend',                  # XXX name
    'schema',                   # nested list of tokens
])
DropGen = namedtuple('DropGen', [
    'ifexists',                 # boolean
    'name',                     # XXX name
])
AlterGen = namedtuple('AlterGen', [
    'generator',                # XXX name
    'modelnos',                 # list of int or None
    'commands',                 # AlterGen*
])
AlterGenRenameGen = namedtuple('AlterGenRenameGen', [
    'name',                     # XXX name
])
AlterGenGeneric = namedtuple('AlterGenGeneric', [
    'command',
])

### BQL Model Analysis Language

InitModels = namedtuple('InitModels', [
    'ifnotexists',              # boolean
    'generator',                # XXX name
    'nmodels',                  # list of int or None
])
AnalyzeModels = namedtuple('AnalyzeModels', [
    'generator',                # XXX name
    'modelnos',                 # list of int or None
    'iterations',               # int
    'seconds',                  # int
    'ckpt_iterations',          # int
    'ckpt_seconds',             # int
    'program',                  # string to sub-parser
])
DropModels = namedtuple('DropModels', [
    'generator',                # XXX name
    'modelnos',                 # list of int or None
])

Regress = namedtuple('Regress', [
    'target',                   # XXX name
    'givens',                   # [(XXX name, Exp*)] or None
    'nsamp',                    # int or None
    'population',               # XXX name
    'generator',                # XXX name
    'modelnos'                  # list of int or None
])

Simulate = namedtuple('Simulate', [
    'columns',                  # [SelCol*]
    'population',               # XXX name
    'generator',                # XXX name
    'modelnos',                 # List or None
    'constraints',              # [(XXX name, Exp*)]
    'nsamples',                 # Exp* or None
    'accuracy',                 # int or None
])

SimulateModels = namedtuple('SimulateModels', [
    'columns',                  # [SelCol*]
    'population',               # XXX name
    'generator',                # XXX name or None
])

# Same as SimulateModels, but with compound expressions, not limited
# to BQL functions.
SimulateModelsExp = namedtuple('SimulateModelsExp', [
    'columns',                  # [SelCol*]
    'population',               # XXX name
    'generator',                # XXX name or None
])

def is_query(phrase):
    if isinstance(phrase, Select):
        return True
    if isinstance(phrase, Estimate):
        return True
    if isinstance(phrase, EstBy):
        return True
    if isinstance(phrase, EstCols):
        return True
    if isinstance(phrase, EstPairCols):
        return True
    if isinstance(phrase, EstPairRow):
        return True
    if isinstance(phrase, InferAuto):
        return True
    if isinstance(phrase, InferExplicit):
        return True
    if isinstance(phrase, Simulate):
        return True
    if isinstance(phrase, SimulateModels):
        return True
    if isinstance(phrase, SimulateModelsExp):
        return True
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
    'population',               # XXX name
    'generator',                # XXX name
    'modelnos',                 # List or None
    'condition',                # Exp* or None (unconditional)
    'grouping',                 # Grouping or None
    'order',                    # [Ord] or None (unordered)
    'limit',                    # Lim or None (unlimited)
])

EstBy = namedtuple('EstBy', [
    'quantifier',               # SELQUANT_*
    'columns',                  # [(Exp*, XXX name)]
    'population',               # XXX name
    'generator',                # XXX name
    'modelnos',                 # List or None
])

SELQUANT_DISTINCT = 'distinct'
SELQUANT_ALL = 'all'

SelColAll = namedtuple('SelColAll', [
    'table',                    # XXX name
])
SelColSub = namedtuple('SelColSub', [
    'table',                    # XXX name
    'query',                    # XXX subquery
])
SelColExp = namedtuple('SelColExp', [
    'expression',               # Exp*
    'name',                     # XXX name
])

PredCol = namedtuple('PredCol', [
    'column',                   # XXX name
    'name',                     # XXX name
    'confname',                 # XXX name
    'nsamples',                 # Exp* or None
])

SelTab = namedtuple('SelTab', [
    'table',                    # XXX subquery or XXX name
    'name',                     # XXX name
])

InferAuto = namedtuple('InferAuto', [
    'columns',                  # [InfCol* or PredCol]
    'confidence',               # Exp* or None (implied 0)
    'nsamples',                 # Exp* or None
    'population',               # XXX name
    'generator',                # XXX name
    'modelnos',                 # List or None
    'condition',                # Exp* or None (unconditional)
    'grouping',                 # Grouping or None
    'order',                    # [Ord] or None (unordered)
    'limit',                    # Lim or None (unlimited)
])

InferExplicit = namedtuple('InferExplicit', [
    'columns',                  # [SelCol* or PredCol]
    'population',               # XXX name
    'generator',                # XXX name
    'modelnos',                 # List or None
    'condition',                # Exp* or None (unconditional)
    'grouping',                 # Grouping or None
    'order',                    # [Ord] or None (unordered)
    'limit',                    # Lim or None (unlimited)
])

InfColAll = namedtuple('InfColAll', [])
InfColOne = namedtuple('InfColOne', [
    'column',                   # XXX name
    'name',                     # XXX name or None
])

EstCols = namedtuple('EstCols', [
    'columns',                  # [SelCol*]
    'population',               # XXX name
    'generator',                # XXX name
    'modelnos',                 # List or None
    'condition',                # Exp* or None (unconditional)
    'order',                    # [Ord] or None (unordered)
    'limit',                    # Lim or None (unlimited),
])

EstPairCols = namedtuple('EstPairCols', [
    'columns',                  # [SelCol*]
    'population',               # XXX name
    'subcolumns',               # ColList* or None
    'generator',                # XXX name
    'modelnos',                 # List or None
    'condition',                # Exp* or None (unconditional)
    'order',                    # [Ord] or None (unordered)
    'limit',                    # Lim or None (unlimited),
])

EstPairRow = namedtuple('EstPairRow', [
    'columns',                  # [SelCol*]
    'population',               # XXX name
    'generator',                # XXX name
    'modelnos',                 # List or None
    'condition',                # Exp* or None (unconditional)
    'order',                    # [Ord] or None (unordered)
    'limit',                    # Lim or None (unlimited),
])

ColListAll = namedtuple('ColListAll', [])
ColListLit = namedtuple('ColListLit', ['columns'])
ColListSub = namedtuple('ColListSub', ['query']) # subquery

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
ExpInQuery = namedtuple('ExpInQuery', ['expression', 'positive', 'query'])
ExpInExp = namedtuple('ExpInExp', ['expression', 'positive', 'expressions'])
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
OP_NEGATE = 'NEGATE'
OP_PLUSID = 'PLUSID'

ExpBQLPredProb = namedtuple('ExpBQLPredProb', ['targets', 'constraints'])
ExpBQLProbDensity = namedtuple('ExpBQLProbDensity', ['targets', 'constraints'])
ExpBQLProbDensityFn = namedtuple('ExpBQLProbDensityFn', [
    'value', 'constraints'
])
ExpBQLSim = namedtuple('ExpBQLSim', [
    'ofcondition', 'tocondition', 'column'
])
ExpBQLPredRel = namedtuple('ExpBQLPredRel', [
    'ofcondition', 'tocondition', 'hypotheticals', 'column'
])
ExpBQLDepProb = namedtuple('ExpBQLDepProb', ['column0', 'column1'])
ExpBQLMutInf = namedtuple('ExpBQLMutInf', [
    'columns0', 'columns1', 'constraints', 'nsamples'
])
ExpBQLCorrel = namedtuple('ExpBQLCorrel', ['column0', 'column1'])
ExpBQLCorrelPval = namedtuple('ExpBQLCorrelPval', ['column0', 'column1'])
ExpBQLPredict = namedtuple('ExpBQLPredict', [
    'column', 'confidence', 'nsamples',
])
ExpBQLPredictConf = namedtuple('ExpBQLPredictConf', ['column', 'nsamples'])
ExpBQLProbEst = namedtuple('ExpBQLProbEst', ['expression'])

def is_bql(exp):
    if isinstance(exp, ExpBQLPredProb):
        return True
    if isinstance(exp, ExpBQLProbDensity):
        return True
    if isinstance(exp, ExpBQLProbDensityFn):
        return True
    if isinstance(exp, ExpBQLSim):
        return True
    if isinstance(exp, ExpBQLPredRel):
        return True
    if isinstance(exp, ExpBQLDepProb):
        return True
    if isinstance(exp, ExpBQLMutInf):
        return True
    if isinstance(exp, ExpBQLCorrel):
        return True
    if isinstance(exp, ExpBQLCorrelPval):
        return True
    if isinstance(exp, ExpBQLPredict):
        return True
    if isinstance(exp, ExpBQLPredictConf):
        return True
    if isinstance(exp, ExpBQLProbEst):
        return True
    return False

LitNull = namedtuple('LitNull', ['value'])
LitInt = namedtuple('LitInt', ['value'])
LitFloat = namedtuple('LitFloat', ['value'])
LitString = namedtuple('LitString', ['value'])

Type = namedtuple('Type', ['names', 'args'])
