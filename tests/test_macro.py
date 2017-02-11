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

import bayeslite.ast as ast
import bayeslite.macro as macro


def test_expand_probability_estimate():
    expression = ast.ExpOp(ast.OP_LT, [
        ast.ExpBQLMutInf(
            ['c0'],
            ['c1', 'c2'],
            [('c3', ast.ExpLit(ast.LitInt(3)))],
            None),
        ast.ExpLit(ast.LitFloat(0.1)),
    ])
    probest = ast.ExpBQLProbEst(expression)
    assert macro.expand_probability_estimate(probest, 'p', 'g') == \
        ast.ExpSub(
            ast.Select(ast.SELQUANT_ALL,
                [ast.SelColExp(
                    ast.ExpApp(False, 'AVG', [ast.ExpCol(None, 'x')]),
                    None)],
                [ast.SelTab(
                    ast.SimulateModelsExp([ast.SelColExp(expression, 'x')],
                        'p', 'g'),
                    None)],
                None, None, None, None))

def test_simulate_models_trivial():
    e = ast.ExpBQLMutInf(['c0'], ['c1', 'c2'],
        [('c3', ast.ExpLit(ast.LitInt(3)))],
        None)
    simmodels = ast.SimulateModelsExp([ast.SelColExp(e, 'x')], 'p', 'g')
    assert macro.expand_simulate_models(simmodels) == \
        ast.SimulateModels([ast.SelColExp(e, 'x')], 'p', 'g')


def test_simulate_models_nontrivial():
    # XXX test descent into ExpLit
    # XXX test descent into ExpNumpar
    # XXX test descent into ExpNampar
    # XXX test descent into ExpCol
    # XXX test descent into ExpSub
    # XXX test descent into ExpCollate
    # XXX test descent into ExpIn
    # XXX test descent into ExpCast
    # XXX test descent into ExpExists
    # XXX test descent into ExpApp
    # XXX test descent into ExpAppStar
    # XXX test descent into ExpCase
    mutinf0 = ast.ExpBQLMutInf(['c0'], ['c1', 'c2'],
        [('c3', ast.ExpLit(ast.LitInt(3)))],
        None)
    mutinf1 = ast.ExpBQLMutInf(['c4', 'c5'], ['c6'],
        [('c7', ast.ExpLit(ast.LitString('ergodic')))],
        100)
    probdensity = ast.ExpBQLProbDensity(
        [('x', ast.ExpLit(ast.LitFloat(1.2)))],
        # No conditions for now -- that changes the weighting of the average.
        [])
    expression0 = ast.ExpOp(ast.OP_LT, [
        mutinf0,
        ast.ExpOp(ast.OP_MUL, [ast.ExpLit(ast.LitFloat(0.1)), mutinf1]),
    ])
    expression1 = probdensity
    simmodels = ast.SimulateModelsExp(
        [
            ast.SelColExp(expression0, 'quagga'),
            ast.SelColExp(expression1, 'eland'),
        ], 'p', 'g')
    assert macro.expand_simulate_models(simmodels) == \
        ast.Select(ast.SELQUANT_ALL,
            [
                ast.SelColExp(
                    ast.ExpOp(ast.OP_LT, [
                        ast.ExpCol(None, 'v0'),
                        ast.ExpOp(ast.OP_MUL, [
                            ast.ExpLit(ast.LitFloat(0.1)),
                            ast.ExpCol(None, 'v1'),
                        ])
                    ]),
                    'quagga'),
                ast.SelColExp(ast.ExpCol(None, 'v2'), 'eland'),
            ],
            [ast.SelTab(
                ast.SimulateModels(
                    [
                        ast.SelColExp(mutinf0, 'v0'),
                        ast.SelColExp(mutinf1, 'v1'),
                        ast.SelColExp(probdensity, 'v2'),
                    ], 'p', 'g'),
                None)],
            None, None, None, None)
