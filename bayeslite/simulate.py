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

import itertools

import bayeslite.ast as ast
import bayeslite.core as core
import bayeslite.bqlfn as bqlfn

from bayeslite.exception import BQLError


def simulate_models_rows(bdb, simulation):
    assert all(isinstance(c, ast.SelColExp) for c in simulation.columns)
    population_id = core.bayesdb_get_population(
        bdb, simulation.population)
    generator_id = None
    if simulation.generator is not None:
        if not core.bayesdb_has_generator(
                bdb, population_id, simulation.generator):
            raise BQLError(bdb, 'No such generator: %r' %
                (simulation.generator,))
        generator_id = core.bayesdb_get_generator(
            bdb, population_id, simulation.generator)
    def retrieve_literal(expression):
        assert isinstance(expression, ast.ExpLit)
        lit = expression.value
        if isinstance(lit, ast.LitNull):
            return None
        elif isinstance(lit, ast.LitInt):
            return lit.value
        elif isinstance(lit, ast.LitFloat):
            return lit.value
        elif isinstance(lit, ast.LitString):
            return lit.value
        else:
            assert False
    def retrieve_variable(var):
        if not core.bayesdb_has_variable(
                bdb, population_id, generator_id, var):
            raise BQLError(bdb, 'No such population variable: %s' % (var,))
        return core.bayesdb_variable_number(
            bdb, population_id, generator_id, var)
    def simulate_column(exp):
        if isinstance(exp, ast.ExpCol):
            # XXX This is wrong -- it returns independent samples from
            # the marginals of each variable, not one sample from the
            # joint on all variables.
            if False:
                raise BQLError(bdb,
                    'SIMULATE FROM MODELS OF can\'t sample conditional')
                # XXX Gotta weight each model by probability of
                # constraints.
                constraints = [
                    (retrieve_variable(v), retrieve_literal(e))
                    for v, e in simulation.constraints
                ]
            else:
                constraints = []
            colnos = [retrieve_variable(exp.column)]
            accuracy = 1        # XXX Allow nontrivial accuracy?
            samples = bqlfn.bayesdb_simulate(
                bdb, population_id, constraints, colnos,
                generator_id=generator_id, numpredictions=1,
                accuracy=accuracy)
            return [sample[0] for sample in samples]
        elif isinstance(exp, ast.ExpBQLDepProb):
            raise BQLError(bdb,
                'DEPENDENCE PROBABILITY simulation still unsupported.')
        elif isinstance(exp, ast.ExpBQLProbDensity):
            raise BQLError(bdb,
                'PROBABILITY DENSITY OF simulation still unsupported.')
        elif isinstance(exp, ast.ExpBQLMutInf):
            colnos0 = [retrieve_variable(c) for c in exp.columns0]
            colnos1 = [retrieve_variable(c) for c in exp.columns1]
            constraint_args = ()
            if exp.constraints is not None:
                constraint_args = tuple(itertools.chain.from_iterable([
                    [retrieve_variable(colname), retrieve_literal(expr)]
                    for colname, expr in exp.constraints
                ]))
            nsamples = exp.nsamples and retrieve_literal(exp.nsamples)
            # One mi_list per generator of the population.
            #
            # XXX fsaad@20170625: Setting modelnos = None arbitrarily, figure
            # out how to set the modelnos argument.
            mi_lists = bqlfn._bql_column_mutual_information(
                bdb, population_id, generator_id, None, colnos0, colnos1,
                nsamples, *constraint_args)
            return list(itertools.chain.from_iterable(mi_lists))
        else:
            raise BQLError(bdb,
                'Only constants can be simulated: %s.' % (simulation,))
    columns = [simulate_column(c.expression) for c in simulation.columns]
    # All queries must return the same number of rows, equal to the number of
    # models of all generators implied by the query.
    assert all(len(column) == len(columns[0]) for column in columns)
    # Convert the columns into rows.
    return zip(*columns)
