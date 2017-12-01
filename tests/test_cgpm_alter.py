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

import contextlib
import pytest

import bayeslite.core

from bayeslite import bayesdb_register_backend
from bayeslite.exception import BQLError
from bayeslite.math_util import abserr
from bayeslite.backends.cgpm_backend import CGPM_Backend

from test_cgpm import cgpm_dummy_satellites_bdb


@contextlib.contextmanager
def cgpm_dummy_satellites_pop_bdb():
    with cgpm_dummy_satellites_bdb() as bdb:
        bdb.execute('''
            create population satellites for satellites_ucs with schema(
                model apogee as numerical;
                model class_of_orbit as categorical;
                model country_of_operator as categorical;
                model launch_mass as numerical;
                model perigee as numerical;
                model period as numerical
            )
        ''')
        backend = CGPM_Backend(dict(), multiprocess=0)
        bayesdb_register_backend(bdb, backend)
        yield bdb


def test_cgpm_alter_basic():
    with cgpm_dummy_satellites_pop_bdb() as bdb:
        bdb.execute('''
            create generator g0 for satellites using cgpm(
                subsample 10
            );
        ''')
        bdb.execute('initialize 4 models for g0')

        population_id = bayeslite.core.bayesdb_get_population(
            bdb, 'satellites')
        generator_id = bayeslite.core.bayesdb_get_generator(
            bdb, population_id, 'g0')

        # Make all variables independent in models 0-1.
        bdb.execute('''
            alter generator g0 models (0,1)
                ensure variables * independent;
        ''')
        dependencies = bdb.execute('''
            estimate dependence probability from pairwise variables of
            satellites modeled by g0 using models 0-1;
        ''').fetchall()
        for _, var0, var1, value in dependencies:
            assert value == int(var0 == var1)

        # Make all variables dependent in all models.
        bdb.execute('''
            alter generator g0
                ensure variables * dependent;
        ''')
        dependencies = bdb.execute('''
            estimate dependence probability from pairwise variables of
            satellites
        ''').fetchall()
        for _, _, _, value in dependencies:
            assert value == 1

        # Make all variables independent again.
        bdb.execute('''
            alter generator g0
                ensure variables * independent;
        ''')

        # Move apogee, perigee->view(period), launch_mass->view(class_of_orbit)
        # All other variables should remain independent.
        bdb.execute('''
            alter generator g0
                ensure variables (apogee, period, perigee) in context of period,
                ensure variable launch_mass in context of class_of_orbit;
        ''')
        dependencies = bdb.execute('''
            estimate dependence probability from pairwise variables of
            satellites
        ''').fetchall()
        blocks = [
            ['apogee','perigee','period'],
            ['launch_mass', 'class_of_orbit']
        ]
        for _, var0, var1, value in dependencies:
            if var0 in blocks[0] and var1 in blocks[0]:
                assert value == 1.
            elif var0 in blocks[1] and var1 in blocks[1]:
                assert value == 1.
            elif var0 == var1:
                assert value == 1.
            else:
                assert value == 0.

        # Move period to a singleton in model 3.
        bdb.execute('''
            alter generator g0 model (3)
                ensure variable period in singleton view
        ''')
        dependencies = bdb.execute('''
            estimate dependence probability with period from variables of
            satellites modeled by g0 using models 0,1,2
        ''')
        for _, var0, value in dependencies:
            if var0 in blocks[0]:
                assert value == 1
            else:
                assert value == 0
        dependencies = bdb.execute('''
            estimate dependence probability with period from variables of
            satellites modeled by g0 using models 3
        ''')
        for _, var0, value in dependencies:
            assert value == 0

        # Change the column crp concentration.
        bdb.execute('''
            alter generator g0
                set view concentration parameter to 1000
        ''')
        engine = bdb.backends['cgpm']._engine(bdb, generator_id)
        for state in engine.states:
            assert abserr(1./1000, state.alpha()) < 1e-5

        # Change row crp concentration in view of period.
        engine = bdb.backends['cgpm']._engine(bdb, generator_id)
        varno = bayeslite.core.bayesdb_variable_number(
            bdb, population_id, generator_id, 'period')
        initial_alphas = [s.view_for(varno).alpha() for s in engine.states]
        bdb.execute('''
            alter generator g0 models (1,3)
                set row cluster concentration parameter
                    within view of period to 12;
        ''')
        for i, state in enumerate(engine.states):
            view_alpha = state.view_for(varno).alpha()
            if i in [1,3]:
                assert abserr(1./12, view_alpha) < 1e-5
            else:
                assert abserr(initial_alphas[i], view_alpha) < 1e-8

        # Run 10 steps of analysis.
        bdb.execute('analyze g0 for 4 iteration wait (optimized)')

        # Retrieve rows in the subsample.
        cursor = bdb.execute('''
            SELECT table_rowid FROM  bayesdb_cgpm_individual
            WHERE generator_id = ?
        ''', (generator_id,))
        subsample_rows = [c[0] for c in cursor]

        # Move all rows to same cluster in view of country_of_operator.
        bdb.execute('''
            alter generator g0
                ensure rows * in cluster of row %s
                within context of country_of_operator
        ''' % (subsample_rows[0],))

        similarities = bdb.execute('''
            estimate similarity in the context of country_of_operator
            from pairwise satellites
        ''')
        for row0, row1, value in similarities:
            if row0 in subsample_rows and row1 in subsample_rows:
                assert value == 1.
            else:
                assert value is None

def test_cgpm_alter_errors():
    with cgpm_dummy_satellites_pop_bdb() as bdb:
        # Prepare the bdb.
        bdb.execute('''
            create generator g0 for satellites using cgpm(
                subsample 10
            );
        ''')
        bdb.execute('initialize 4 models for g0')
        # Retrieve rows in the subsample.
        population_id = bayeslite.core.bayesdb_get_population(bdb, 'satellites')
        generator_id = bayeslite.core.bayesdb_get_generator(
            bdb, population_id, 'g0')
        cursor = bdb.execute('''
            SELECT table_rowid FROM  bayesdb_cgpm_individual
            WHERE generator_id = ?
        ''', (generator_id,))
        subsample_rows = [c[0] for c in cursor]
        # Invoke errors.
        with pytest.raises(BQLError):
            # ensure variables accepts * only, not named variables.
            bdb.execute('''
                alter generator g0 models (0,1)
                    ensure variables (period_minutes, perigee) dependent;
            ''')
        with pytest.raises(BQLError):
            # Non existent target variable apogees.
            bdb.execute('''
                alter generator g0
                    ensure variables (perigee, apogees) in context of period
            ''')
        with pytest.raises(BQLError):
            # Non existent context variable periods
            bdb.execute('''
                alter generator g0
                    ensure variables (perigee, apogee) in context of periods
            ''')
        with pytest.raises(BQLError):
            # Non existent context variable periods
            bdb.execute('''
                alter generator g0
                    ensure variables (perigee, apogee) in context of periods
            ''')
        with pytest.raises(BQLError):
            # Non existent target row 1000.
            bdb.execute('''
                alter generator g0
                    ensure rows (1, 1000) in cluster of row %d
                        within context of country_of_operator
            ''' % (subsample_rows[0],))
        with pytest.raises(BQLError):
            # Non existent reference row 1000.
            bdb.execute('''
                alter generator g0
                    ensure rows (%s) in cluster of row 1000
                        within context of country_of_operator
            ''' % (subsample_rows[0],))
        with pytest.raises(BQLError):
            # Non existent context variable country_of_operators.
            bdb.execute('''
                alter generator g0
                    ensure rows (%s) in cluster of row %s
                        within context of country_of_operators
            ''' % (subsample_rows[0], subsample_rows[1]))
        with pytest.raises(BQLError):
            # Non existent context variable periods.
            bdb.execute('''
                alter generator g0 models (1,3)
                    set row cluster concentration parameter
                        within view of periods to 12;
            ''')
