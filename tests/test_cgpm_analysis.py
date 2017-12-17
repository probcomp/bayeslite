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

import bayeslite.core

from bayeslite import bayesdb_register_backend
from bayeslite.exception import BQLError
from bayeslite.backends.cgpm_backend import CGPM_Backend

from test_cgpm import cgpm_dummy_satellites_bdb

# This test suite needs expanding to confirm the subproblems are compiled to the
# right inference kernels in cgpm_backend, but the information is quite nested
# so it is not trivial to retrieve this information. Considering refactoring the
# interpreter to make unit testing easier.


def test_analysis_subproblems_basic():
    with cgpm_dummy_satellites_bdb() as bdb:
        bdb.execute('''
            CREATE POPULATION satellites FOR satellites_ucs WITH SCHEMA(
                SET STATTYPE OF apogee TO NUMERICAL;
                SET STATTYPE OF class_of_orbit TO NOMINAL;
                SET STATTYPE OF country_of_operator TO NOMINAL;
                SET STATTYPE OF launch_mass TO NUMERICAL;
                SET STATTYPE OF perigee TO NUMERICAL;
                SET STATTYPE OF period TO NUMERICAL
            )
        ''')
        bayesdb_register_backend(bdb, CGPM_Backend(dict(), multiprocess=0))
        bdb.execute('''
            CREATE GENERATOR g0 FOR satellites USING cgpm(
                SUBSAMPLE 10
            );
        ''')
        bdb.execute('INITIALIZE 4 MODELS FOR g0')

        # Test each subproblem individually except for variable hyperparameters.
        for optimized in ['', 'OPTIMIZED;',]:
            for subproblem in [
                'variable clustering',
                'variable clustering concentration',
                'row clustering',
                'row clustering concentration',
            ]:
                bdb.execute('''
                    ANALYZE g0 MODELS 0,1 FOR 4 ITERATION(
                        SUBPROBLEM %s;
                        %s
                    );
                ''' % (subproblem, optimized))

        # Test variable hyperparameters.
        bdb.execute('''
            ANALYZE g0 FOR 1 ITERATION (
                VARIABLES period, launch_mass;
                SUBPROBLEM variable hyperparameters;
            )
        ''')
        with pytest.raises(BQLError):
            # OPTIMIZED backend does not support variable hyperparameters.
            bdb.execute('''
                ANALYZE g0 FOR 1 SECONDS (
                    SUBPROBLEM variable hyperparameters;
                    OPTIMIZED;
                )
            ''')

        # Test rows.
        generator_id = bayeslite.core.bayesdb_get_generator(bdb, None, 'g0')
        cursor = bdb.execute('''
            SELECT table_rowid FROM  bayesdb_cgpm_individual
            WHERE generator_id = ?
        ''', (generator_id,))
        subsample_rows = [c[0] for c in cursor]
        bad_rows = [i for i in xrange(20) if i not in subsample_rows]
        for optimized in ['', 'OPTIMIZED;']:
            bdb.execute('''
                ANALYZE g0 MODEL 3 FOR 1 ITERATION (
                    VARIABLES class_of_orbit;
                    ROWS %s;
                    SUBPROBLEMS (
                        row clustering,
                        row clustering concentration
                    );
                    %s
            )
            ''' % (','.join(map(str, subsample_rows)), optimized))
            with pytest.raises(BQLError):
                # Fail on rows not in the population or subsample.
                bdb.execute('''
                    ANALYZE g0 MODEL 3 FOR 1 ITERATION (
                        VARIABLES class_of_orbit;
                        ROWS %s;
                        SUBPROBLEMS (
                            row clustering,
                            row clustering concentration
                        );
                        %s
                )
                ''' % (','.join(map(str, bad_rows)), optimized))
