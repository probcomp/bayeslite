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

from __future__ import division

from bayeslite import bayesdb_register_metamodel
from bayeslite.metamodels.cgpm_metamodel import CGPM_Metamodel

from test_cgpm import cgpm_dummy_satellites_bdb


def test_quick():
    with cgpm_dummy_satellites_bdb() as bdb:
        bayesdb_register_metamodel(
            bdb, CGPM_Metamodel(dict(), multiprocess=0))
        bdb.execute('''
            CREATE POPULATION satellites FOR satellites_ucs WITH SCHEMA(
                MODEL apogee AS NUMERICAL;
                MODEL class_of_orbit AS CATEGORICAL;
                MODEL country_of_operator AS CATEGORICAL;
                MODEL launch_mass AS NUMERICAL;
                MODEL perigee AS NUMERICAL;
                MODEL period AS NUMERICAL
            )
        ''')
        bdb.execute('CREATE METAMODEL m FOR satellites WITH BASELINE crosscat;')
        bdb.execute('INITIALIZE 2 MODELS FOR m;')
        # bdb.execute('''
        #     REGRESS apogee GIVEN (*) USING 10 SAMPLES BY m;
        # ''')
        bdb.execute('''
            REGRESS apogee GIVEN (
                apogee, country_of_operator,
                satellites.(ESTIMATE * FROM VARIABLES OF satellites LIMIT 2))
            USING 10 SAMPLES BY satellites;
        ''')
