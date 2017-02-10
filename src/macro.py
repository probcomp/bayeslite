# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2017, MIT Probabilistic Computing Project
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

"""Ad-hoc AST macro expansions.

The organization of this is a temporary kludge, not a general-purpose
BQL macro expansion mechanism.
"""

import bayeslite.ast as ast


def expand_probability_estimate(probest, population, generator):
    simmodels = ast.SimulateModelsExp([ast.SimCol(probest.expression, 'x')],
        population, generator)
    select = ast.Select(ast.SELQUANT_ALL,
        [ast.SelColExp(ast.ExpApp(False, 'AVG', [ast.ExpCol(None, 'x')]),
            None)],
        [ast.SelTab(simmodels, None)],
        None, None, None, None)
    return ast.ExpSub(select)


def expand_simulate_models(sim):
    assert isinstance(sim, ast.SimulateModelsExp)
    if all(isinstance(c.col, ast.ExpCol) or ast.is_bql(c.col)
           for c in sim.columns):
        return ast.SimulateModels(sim.columns, sim.population, sim.generator)
    simcols = []
    selcols = [_expand_simmodel_column(c, simcols) for c in sim.columns]
    subsim = ast.SimulateModels(simcols, sim.population, sim.generator)
    seltab = ast.SelTab(subsim, None)
    return ast.Select(
        ast.SELQUANT_ALL, selcols, [seltab], None, None, None, None)

def _expand_simmodel_column(c, simcols):
    exp = _expand_simmodel_exp(c.col, simcols)
    name = c.name
    return ast.SelColExp(exp, name)

def _expand_simmodel_exp(exp, simcols):
    if isinstance(exp, ast.ExpCol) or ast.is_bql(exp):
        tmpname = 'v%d' % (len(simcols),)
        simcols.append(ast.SimCol(exp, tmpname))
        return ast.ExpCol(None, tmpname)
    elif isinstance(exp, ast.ExpLit) or \
         isinstance(exp, ast.ExpNumpar) or \
         isinstance(exp, ast.ExpNampar):
        return exp
    elif isinstance(exp, ast.ExpSub) or \
         isinstance(exp, ast.ExpExists):
        # XXX Not really right -- need to provide correct scoping.
        return exp              # XXX subquery scoping
    elif isinstance(exp, ast.ExpCollate):
        subexp = _expand_simmodel_exp(exp.expression, simcols)
        return ast.ExpCollate(subexp, exp.collation)
    elif isinstance(exp, ast.ExpIn):
        subexp = _expand_simmodel_exp(exp.expression, simcols)
        subquery = exp.subquery         # XXX subquery scoping
        return ast.ExpIn(subexp, exp.positive, subquery)
    elif isinstance(exp, ast.ExpCast):
        subexp = _expand_simmodel_exp(exp.expression, simcols)
        return ast.ExpCast(subexp, exp.type)
    elif isinstance(exp, ast.ExpApp):
        operands = [_expand_simmodel_exp(operand, simcols)
            for operand in exp.operands]
        return ast.ExpApp(exp.distinct, exp.operator, operands)
    elif isinstance(exp, ast.ExpAppStar):
        return exp
    elif isinstance(exp, ast.ExpCase):
        raise NotImplementedError("I'm too lazy to do CASE right now.")
    elif isinstance(exp, ast.ExpOp):
        operands = [_expand_simmodel_exp(operand, simcols)
            for operand in exp.operands]
        return ast.ExpOp(exp.operator, operands)
    else:
        assert False, 'Invalid expression: %s' % (repr(exp),)
