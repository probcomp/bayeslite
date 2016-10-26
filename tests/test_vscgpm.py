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

import math
import numpy as np
import pytest

from cgpm.utils import general as gu

import bayeslite.core as core

from bayeslite import bayesdb_open
from bayeslite import bayesdb_register_metamodel
from bayeslite.exception import BQLError
from bayeslite.metamodels.cgpm_metamodel import CGPM_Metamodel

# ------------------------------------------------------------------------------
# XXX Kepler source code.

kepler_source = """
[define make_cgpm (lambda ()
  (do
      ; Kepler's law.
    (assume keplers_law
      (lambda (apogee perigee)
        (let ((GM 398600.4418) (earth_radius 6378)
              (a (+ (* .5 (+ (abs apogee) (abs perigee))) earth_radius)))
          (/ (* (* 2 3.1415) (sqrt (/ (pow a 3) GM))) 60))))

    ; Internal samplers.
    (assume crp_alpha .5)

    (assume get_cluster_sampler
        (make_crp crp_alpha))

    (assume get_error_sampler
        (mem (lambda (cluster)
            (make_nig_normal 1 1 1 1))))

    ; Output simulators.
    (assume simulate_cluster_id
      (mem (lambda (rowid apogee perigee)
        (tag (atom rowid) (atom 1)
          (get_cluster_sampler)))))

    (assume simulate_error
      (mem (lambda (rowid apogee perigee)
          (let ((cluster_id (simulate_cluster_id rowid apogee perigee)))
            (tag (atom rowid) (atom 2)
              ((get_error_sampler cluster_id)))))))

    (assume simulate_period
      (mem (lambda (rowid apogee perigee)
        (+ (keplers_law apogee perigee)
           (simulate_error rowid apogee perigee)))))

    ; Simulators.
    (assume simulators (list simulate_cluster_id
                             simulate_error
                             simulate_period))))]

; Output observers.
[define observe_cluster_id
  (lambda (rowid apogee perigee value label)
      (observe (simulate_cluster_id ,rowid ,apogee ,perigee)
               (atom value) ,label))]

[define observe_error
  (lambda (rowid apogee perigee value label)
      (observe (simulate_error ,rowid ,apogee ,perigee) value ,label))]

[define observe_period
  (lambda (rowid apogee perigee value label)
    (let ((theoretical_period (run (sample (keplers_law ,apogee ,perigee))))
          (error (- value theoretical_period)))
      (observe_error rowid apogee perigee error label)))]

; List of observers.
[define observers (list observe_cluster_id
                        observe_error
                        observe_period)]

; List of inputs.
[define inputs (list 'apogee
                     'perigee)]

; Transition operator.
[define transition
  (lambda (N)
    (mh default one (* N 1000)))]
"""

def test_cgpm_extravaganza__ci_slow():
    try:
        from cgpm.regressions.forest import RandomForest
        from cgpm.regressions.linreg import LinearRegression
        from cgpm.venturescript.vscgpm import VsCGpm
    except ImportError:
        pytest.skip('no sklearn or venturescript')
        return
    with bayesdb_open(':memory:', builtin_metamodels=False) as bdb:
        # XXX Use the real satellites data instead of this bogosity?
        bdb.sql_execute('''
            CREATE TABLE satellites_ucs (
                name,
                apogee,
                class_of_orbit,
                country_of_operator,
                launch_mass,
                perigee,
                period
            )
        ''')
        for l, f in [
            ('geo', lambda x, y: x + y**2),
            ('leo', lambda x, y: math.sin(x + y)),
        ]:
            for x in xrange(1000):
                for y in xrange(10):
                    countries = ['US', 'Russia', 'China', 'Bulgaria']
                    country = countries[bdb._np_prng.randint(0, len(countries))]
                    name = 'sat-%s-%d' % (
                        country, bdb._np_prng.randint(0, 10**8))
                    mass = bdb._np_prng.normal(1000, 50)
                    bdb.sql_execute('''
                        INSERT INTO satellites_ucs
                            (name, country_of_operator, launch_mass,
                                class_of_orbit, apogee, perigee, period)
                            VALUES (?,?,?,?,?,?,?)
                    ''', (name, country, mass, l, x, y, f(x, y)))

        bdb.execute('''
            CREATE POPULATION satellites FOR satellites_ucs (
                name IGNORE;
                apogee NUMERICAL;
                class_of_orbit CATEGORICAL;
                country_of_operator CATEGORICAL;
                launch_mass NUMERICAL;
                perigee NUMERICAL;
                period NUMERICAL
            )
        ''')

        bdb.execute('''
            ESTIMATE CORRELATION FROM PAIRWISE VARIABLES OF satellites
            ''').fetchall()

        cgpm_registry = {
            'venturescript': VsCGpm,
            'linreg': LinearRegression,
            'forest': RandomForest,
        }
        cgpmt = CGPM_Metamodel(cgpm_registry)
        bayesdb_register_metamodel(bdb, cgpmt)

        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE METAMODEL g0 FOR satellites USING cgpm (
                    SET CATEGORY MODEL FOR apoge TO NORMAL
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE METAMODEL g0 FOR satellites USING cgpm (
                    OVERRIDE MODEL FOR perigee GIVEN apoge USING linreg
                )
            ''')
        with pytest.raises(BQLError):
            bdb.execute('''
                CREATE METAMODEL g0 FOR satellites USING cgpm (
                    LATENT apogee NUMERICAL
                )
            ''')

        bdb.execute('''
            CREATE METAMODEL g0 FOR satellites USING cgpm (
                SET CATEGORY MODEL FOR apogee TO NORMAL;

                LATENT kepler_cluster_id NUMERICAL;
                LATENT kepler_noise NUMERICAL;

                OVERRIDE MODEL FOR kepler_cluster_id, kepler_noise, period
                GIVEN apogee, perigee
                USING venturescript (source = "{}");

                OVERRIDE MODEL FOR
                    perigee
                GIVEN apogee USING linreg;

                OVERRIDE MODEL FOR class_of_orbit
                GIVEN apogee, period, perigee, kepler_noise
                USING forest (k = 4);

                SUBSAMPLE 100,
            )
        '''.format(kepler_source))

        population_id = core.bayesdb_get_population(bdb, 'satellites')
        generator_id = core.bayesdb_get_generator(bdb, population_id, 'g0')
        assert core.bayesdb_generator_column_numbers(bdb, generator_id) == \
            [-2, -1, 1, 2, 3, 4, 5, 6]
        assert core.bayesdb_variable_numbers(bdb, population_id, None) == \
            [1, 2, 3, 4, 5, 6]
        assert core.bayesdb_variable_numbers(
                bdb, population_id, generator_id) == \
            [-2, -1, 1, 2, 3, 4, 5, 6]

        # -- MODEL country_of_operator GIVEN class_of_orbit USING forest;
        bdb.execute('INITIALIZE 1 MODELS FOR g0')
        bdb.execute('ANALYZE g0 FOR 1 iteration WAIT (;)')
        bdb.execute('''
            ANALYZE g0 FOR 1 iteration WAIT (VARIABLES kepler_cluster_id)
        ''')
        bdb.execute('''
            ANALYZE g0 FOR 1 iteration WAIT (
                SKIP kepler_cluster_id, kepler_noise, period;
            )
        ''')
        # OPTIMIZED uses the lovecat backend.
        bdb.execute('ANALYZE g0 FOR 20 iteration WAIT (OPTIMIZED)')
        with pytest.raises(Exception):
            # Disallow both SKIP and VARIABLES clauses.
            #
            # XXX Catch a more specific exception.
            bdb.execute('''
                ANALYZE g0 FOR 1 ITERATION WAIT (
                    SKIP kepler_cluster_id;
                    VARIABLES apogee, perigee;
                )
            ''')
        bdb.execute('''
            ANALYZE g0 FOR 1 iteration WAIT (
                SKIP kepler_cluster_id, kepler_noise, period;
            )
        ''')
        bdb.execute('ANALYZE g0 FOR 1 ITERATION WAIT')

        bdb.execute('''
            ESTIMATE DEPENDENCE PROBABILITY
                OF kepler_cluster_id WITH period WITHIN satellites
                MODELLED BY g0
        ''').fetchall()
        bdb.execute('''
            ESTIMATE PREDICTIVE PROBABILITY OF apogee FROM satellites LIMIT 1
        ''').fetchall()
        bdb.execute('''
            ESTIMATE PREDICTIVE PROBABILITY OF kepler_cluster_id
                FROM satellites MODELLED BY g0 LIMIT 1
        ''').fetchall()
        bdb.execute('''
            ESTIMATE PREDICTIVE PROBABILITY OF kepler_noise
                FROM satellites MODELLED BY g0 LIMIT 1
        ''').fetchall()
        bdb.execute('''
            ESTIMATE PREDICTIVE PROBABILITY OF period
                FROM satellites LIMIT 1
        ''').fetchall()
        bdb.execute('''
            INFER EXPLICIT
                    PREDICT kepler_cluster_id CONFIDENCE kepler_cluster_id_conf
                FROM satellites MODELLED BY g0 LIMIT 2;
        ''').fetchall()
        bdb.execute('''
            INFER EXPLICIT PREDICT kepler_noise CONFIDENCE kepler_noise_conf
                FROM satellites MODELLED BY g0 LIMIT 2;
        ''').fetchall()
        bdb.execute('''
            INFER EXPLICIT PREDICT apogee CONFIDENCE apogee_conf
                FROM satellites MODELLED BY g0 LIMIT 1;
        ''').fetchall()
        bdb.execute('''
            ESTIMATE PROBABILITY OF period = 42
                    GIVEN (apogee = 8 AND perigee = 7)
                BY satellites
        ''').fetchall()

        bdb.execute('''
            SIMULATE kepler_cluster_id, apogee, perigee, period
                FROM satellites MODELLED BY g0 LIMIT 4
        ''').fetchall()

        bdb.execute('DROP MODELS FROM g0')
        bdb.execute('DROP METAMODEL g0')
        bdb.execute('DROP POPULATION satellites')
        bdb.execute('DROP TABLE satellites_ucs')
