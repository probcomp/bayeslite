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
    assert all(isinstance(c, ast.SimCol) for c in simulation.columns)
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
        if not core.bayesdb_has_variable(bdb, population_id, generator_id, var):
            raise BQLError(bdb, 'No such population variable: %s' % (var,))
        return core.bayesdb_variable_number(
            bdb, population_id, generator_id, var)
    def simulate_column(phrase):
        if isinstance(phrase, ast.ExpBQLDepProb):
            raise BQLError(bdb,
                'DEPENDENCE PROBABILITY simulation still unsupported.')
        elif isinstance(phrase, ast.ExpBQLProbDensity):
            raise BQLError(bdb,
                'PROBABILITY DENSITY OF simulation still unsupported.')
        elif isinstance(phrase, ast.ExpBQLMutInf):
            colnos0 = [retrieve_variable(c) for c in phrase.columns0]
            colnos1 = [retrieve_variable(c) for c in phrase.columns1]
            constraint_args = ()
            if phrase.constraints is not None:
                constraint_args = tuple(itertools.chain.from_iterable([
                    [retrieve_variable(colname), retrieve_literal(expr)]
                    for colname, expr in phrase.constraints
                ]))
            nsamples = phrase.nsamples and retrieve_literal(phrase.nsamples)
            # One mi_list per generator of the population.
            mi_lists = bqlfn._bql_column_mutual_information(
                bdb, population_id, generator_id, colnos0, colnos1, nsamples,
                *constraint_args)
            return list(itertools.chain.from_iterable(mi_lists))
        else:
            raise BQLError(bdb,
                'Only constants can be simulated: %s.' % (simulation,))
    columns = [simulate_column(c.col) for c in simulation.columns]
    # All queries must return the same number of rows, equal to the number of
    # models of all generators implied by the query.
    assert all(len(column) == len(columns[0]) for column in columns)
    # Convert the columns into rows.
    return zip(*columns)
