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

import pytest

from bayeslite import BQLError
from bayeslite import bayesdb_register_metamodel
from bayeslite.metamodels.cgpm_metamodel import CGPM_Metamodel

from test_cgpm import cgpm_dummy_satellites_bdb


def test_regress_bonanza():
    with cgpm_dummy_satellites_bdb() as bdb:
        bayesdb_register_metamodel(
            bdb, CGPM_Metamodel(dict(), multiprocess=0))
        bdb.execute('''
            CREATE POPULATION satellites FOR satellites_ucs WITH SCHEMA(
                MODEL apogee AS NUMERICAL;
                MODEL class_of_orbit AS NOMINAL;
                MODEL country_of_operator AS NOMINAL;
                MODEL launch_mass AS NUMERICAL;
                MODEL perigee AS NUMERICAL;
                MODEL period AS NUMERICAL
            )
        ''')
        bdb.execute('''
            CREATE METAMODEL m FOR satellites WITH BASELINE crosscat;
        ''')
        bdb.execute('INITIALIZE 2 MODELS FOR m;')

        def check_regression_variables(results, numericals, nominals):
            seen = set()
            for r in results:
                assert len(r) == 2
                variable = r[0]
                assert variable not in seen
                assert variable in numericals or \
                    any(variable.startswith('%s_dum_' % (nominal,))
                        for nominal in nominals)
                seen.add(variable)

        # Regression on 1 numerical variable.
        results = bdb.execute('''
            REGRESS apogee GIVEN (perigee) USING 12 SAMPLES BY satellites;
        ''').fetchall()
        assert len(results) == 2
        check_regression_variables(results, ['intercept', 'perigee'], [])

        # Regression on 1 nominal variable.
        results = bdb.execute('''
            REGRESS apogee GIVEN (country_of_operator)
            USING 12 SAMPLES BY satellites;
        ''').fetchall()
        check_regression_variables(
            results, ['intercept'], ['country_of_operator'])

        # Regression on 1 nominal + 1 numerical variable.
        bdb.execute('''
            REGRESS apogee GIVEN (perigee, country_of_operator)
            USING 12 SAMPLES BY satellites;
        ''').fetchall()
        check_regression_variables(
            results, ['intercept', 'perigee'], ['country_of_operator'])

        # Regression on all variables.
        results = bdb.execute('''
            REGRESS apogee GIVEN (*) USING 12 SAMPLES BY satellites;
        ''', (3,)).fetchall()
        check_regression_variables(
            results,
            ['intercept', 'perigee', 'launch_mass', 'period',],
            ['country_of_operator', 'class_of_orbit',],
        )

        # Regression on column selector subexpression with a binding.
        results = bdb.execute('''
            REGRESS apogee GIVEN (
                satellites.(
                    ESTIMATE * FROM VARIABLES OF satellites
                    ORDER BY dependence probability with apogee DESC
                    LIMIT ?
                )
            )
            USING 12 SAMPLES BY satellites;
        ''', (3,)).fetchall()

        cursor = bdb.execute('''
            ESTIMATE * FROM VARIABLES OF satellites
                ORDER BY dependence probability with apogee DESC
                LIMIT ?
        ''', (3,)).fetchall()
        top_variables = [c[0] for c in cursor]
        nominals = [
            var for var in top_variables
            if var in ['country_of_operator', 'class_of_orbit',]
        ]
        numericals = [var for var in top_variables if var not in nominals]
        check_regression_variables(
            results, numericals + ['intercept'], nominals)

        # Cannot mix * with other variables.
        with pytest.raises(BQLError):
            bdb.execute('''
                REGRESS apogee GIVEN (*, class_of_orbit)
                USING 1 SAMPLES BY satellites;
            ''').fetchall()

        # Not enough data for regression, 1 unique nominal variable.
        with pytest.raises(ValueError):
            bdb.execute('''
                REGRESS apogee GIVEN (class_of_orbit)
                USING 1 SAMPLES BY satellites;
            ''').fetchall()
