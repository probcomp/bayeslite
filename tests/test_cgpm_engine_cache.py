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

from StringIO import StringIO

import bayeslite
import tempfile

import test_csv


def test_engine_increment_stamp():
    """Confirm the engine stamp is incremented appropriately."""
    with bayeslite.bayesdb_open(':memory:') as bdb:
        bayeslite.bayesdb_read_csv(bdb, 't', StringIO(test_csv.csv_data),
            header=True, create=True)
        bdb.execute('''
            CREATE POPULATION p FOR t (
                age NUMERICAL;
                gender CATEGORICAL;
                salary NUMERICAL;
                height IGNORE;
                division CATEGORICAL;
                rank CATEGORICAL
            )
        ''')
        bdb.execute('CREATE GENERATOR m FOR p WITH BASELINE crosscat;')
        cgpm_backend = bdb.backends['cgpm']
        population_id = bayeslite.core.bayesdb_get_population(bdb, 'p')
        generator_id = bayeslite.core.bayesdb_get_generator(
            bdb, population_id, 'm')
        # The engine stamp should be at zero without models.
        assert cgpm_backend._engine_stamp(bdb, generator_id) == 0
        # The engine stamp should equal after initializing models.
        bdb.execute('INITIALIZE 2 MODELS FOR m;')
        assert cgpm_backend._engine_stamp(bdb, generator_id) == 1
        # No caching on initialize.
        assert cgpm_backend._get_cache_entry(bdb, generator_id, 'engine') \
            is None
        # The engine stamp should increment after analysis.
        bdb.execute('ANALYZE m FOR 1 ITERATIONS WAIT;')
        assert cgpm_backend._engine_stamp(bdb, generator_id) == 2
        # Caching on analyze.
        assert cgpm_backend._get_cache_entry(bdb, generator_id, 'engine') \
            is not None
        # Wipe the cache, run a simulation, and confirm the caching.
        cgpm_backend._del_cache_entry(bdb, generator_id, 'engine')
        assert cgpm_backend._get_cache_entry(bdb, generator_id, 'engine') \
            is None
        bdb.execute('SIMULATE age FROM p LIMIT 1;').fetchall()
        assert cgpm_backend._get_cache_entry(bdb, generator_id, 'engine') \
            is not None


def test_engine_stamp_two_clients():
    """Confirm analysis by one worker makes cache in other worker stale."""
    with tempfile.NamedTemporaryFile(prefix='bayeslite') as f:
        with bayeslite.bayesdb_open(f.name) as bdb0:
            bayeslite.bayesdb_read_csv(bdb0, 't', StringIO(test_csv.csv_data),
                header=True, create=True)
            bdb0.execute('''
                CREATE POPULATION p FOR t (
                    age NUMERICAL;
                    gender CATEGORICAL;
                    salary NUMERICAL;
                    height IGNORE;
                    division CATEGORICAL;
                    rank CATEGORICAL
                )
            ''')

            bdb0.execute('CREATE GENERATOR m FOR p WITH BASELINE crosscat;')
            cgpm_backend = bdb0.backends['cgpm']
            population_id = bayeslite.core.bayesdb_get_population(bdb0, 'p')
            generator_id = bayeslite.core.bayesdb_get_generator(
                bdb0, population_id, 'm')

            assert cgpm_backend._engine_stamp(bdb0, generator_id) == 0

            with bayeslite.bayesdb_open(f.name) as bdb1:
                bdb1.execute('INITIALIZE 1 MODEL FOR m')
                assert cgpm_backend._engine_stamp(bdb0, generator_id) == 1
                assert cgpm_backend._engine_stamp(bdb1, generator_id) == 1

            bdb0.execute('ANALYZE m FOR 1 ITERATION WAIT')
            assert cgpm_backend._engine_stamp(bdb0, generator_id) == 2
            assert cgpm_backend._get_cache_entry(
                bdb0, generator_id, 'engine') is not None

            with bayeslite.bayesdb_open(f.name) as bdb2:
                bdb2.execute('ANALYZE m FOR 1 ITERATION WAIT')
                assert cgpm_backend._engine_stamp(bdb2, generator_id) == 3
                assert cgpm_backend._engine_stamp(bdb0, generator_id) == 3

            # Engine in cache of bdb0 should be stale, since bdb2 analyzed.
            assert cgpm_backend._engine_latest(bdb0, generator_id) is None
