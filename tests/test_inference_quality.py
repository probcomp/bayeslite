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

import collections
import itertools
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import random
import struct
from scipy import stats
import tempfile
import time

import bayeslite
from bayeslite import read_pandas
from bayeslite.backends.loom_backend import LoomBackend

def axis_aligned_gaussians(means, size, seed=None):
    if seed is not None:
        np.random.seed(seed)
    return [np.random.multivariate_normal(mean, [[0,1], [1,0]], size=size)
            for mean
            in means]

def mix(gaussians, p, seed=None):
    assert len(gaussians) == len(p)
    assert reduce(lambda sum, n: sum + n, p) == 1

    if seed is not None:
        np.random.seed(seed)
        choices = np.random.choice([0,1], size=len(gaussians[0]), p=p)
    return [gaussians[choices[i]][i] for i in range(len(choices))]

def temp_file_path(suffix):
    """Returns a random file path within '/tmp'."""
    _, temp_filename = tempfile.mkstemp(suffix=suffix)
    os.remove(temp_filename)
    return temp_filename

def register_loom(bdb):
    loom_store_path = temp_file_path('.bdb')
    loom_backend = LoomBackend(loom_store_path=loom_store_path)
    bayeslite.bayesdb_register_backend(bdb, loom_backend)

def distance(a, b):
    '''
    Computes the euclidian distance between points `a` and `b`.
    '''
    # coerce to `np.ndarray`s if necessary
    a = a if isinstance(a, np.ndarray) else np.array(a)
    b = b if isinstance(a, np.ndarray) else np.array(b)
    return abs(np.linalg.norm(a-b))

def prepare_bdb(bdb, samples, table, seed):
    quoted_table = bayeslite.bql_quote_name(table)
    dataframe = pd.DataFrame(data=samples)
    read_pandas.bayesdb_read_pandas_df(bdb, 'data', dataframe, create=True)

    bdb.execute('''
        CREATE POPULATION FOR %s WITH SCHEMA (
            GUESS STATTYPES OF (*)
        )
    ''' % (quoted_table,))
    bdb.execute('CREATE GENERATOR FOR %s USING loom;' % (quoted_table,))
    bdb.execute('INITIALIZE 4 MODELS FOR %s;' % (quoted_table,))
    bdb.execute('ANALYZE %s FOR 100 ITERATIONS;' % (quoted_table,))

def insert_row(bdb, table, x, y):
    quoted_table = bayeslite.bql_quote_name(table)
    query = '''INSERT INTO {table} ("0", "1") VALUES (?, ?)'''.format(table=quoted_table)
    bdb.sql_execute(query, bindings=(x, y))
    return bdb.sql_execute('SELECT last_insert_rowid()').next()[0]

def simulate_from_rowid(bdb, table, column, rowid, limit=1000):
    quoted_table = bayeslite.bql_quote_name(table)
    quoted_column = bayeslite.bql_quote_name(str(column))
    cursor = bdb.execute('SIMULATE {column} FROM {table} GIVEN rowid={rowid} LIMIT {limit}'.format(
        column=quoted_column,
        table=quoted_table,
        rowid=rowid,
        limit=limit
    ))
    return [float(x[0]) for x in cursor]

def test_mix_ratio():
    means = ((0,20), (20,0))
    sample_size = 100
    mix_ratio = [0.7, 0.3]
    seed = 9999
    table = 'data'

    sample_gaussians = axis_aligned_gaussians(means, sample_size, seed=seed)
    samples = mix(sample_gaussians, mix_ratio, seed=seed)

    packed_seed = struct.pack('<QQQQ', seed, seed, seed, seed)
    with bayeslite.bayesdb_open(seed=packed_seed) as bdb:
        register_loom(bdb)
        prepare_bdb(bdb, samples, table, seed)

        cursor = bdb.execute('SIMULATE "0", "1" FROM data LIMIT ?;', (sample_size,))
        simulated_samples = [sample for sample in cursor]

    counts = collections.Counter(
        (0 if distance((x,y), means[0]) < distance((x,y), means[1]) else 1
         for x, y
         in simulated_samples)
    )
    simulated_mix_ratio = [counts[key] / float(len(simulated_samples)) for key in counts]

    for i in range(len(means)):
        difference = abs(mix_ratio[i] - simulated_mix_ratio[i])
        assert difference < 0.1

def test_simulate_y_from_partially_populated_row():
    means = ((0,20), (20,0))
    sample_size = 50
    mix_ratio = [0.7, 0.3]
    seed = 9999
    table = 'data'

    sample_gaussians = axis_aligned_gaussians(means, sample_size, seed=seed)
    samples = mix(sample_gaussians, mix_ratio, seed=seed)

    packed_seed = struct.pack('<QQQQ', seed, seed, seed, seed)
    with bayeslite.bayesdb_open(seed=packed_seed) as bdb:
        register_loom(bdb)
        prepare_bdb(bdb, samples, table, seed)

        rowid = insert_row(bdb, table, means[0][0], None)
        simulated_samples = simulate_from_rowid(bdb, table, 1, rowid, limit=sample_size)

    y_samples = [y for x, y in sample_gaussians[0]]
    statistic, p_value = stats.ks_2samp(y_samples, simulated_samples)
    assert(statistic < 0.1 or p_value > 0.01)

