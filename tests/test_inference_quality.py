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


def distance(a, b):
    '''
    Computes the euclidian distance between points `a` and `b`.
    '''
    # coerce to `np.ndarray`s if necessary
    a = a if isinstance(a, np.ndarray) else np.array(a)
    b = b if isinstance(a, np.ndarray) else np.array(b)
    return abs(np.linalg.norm(a-b))

def gen_gaussians_and_samples(means, sample_size, mix_ratio, seed):
    sample_gaussians = axis_aligned_gaussians(means, sample_size, seed=seed)
    samples = mix(sample_gaussians, mix_ratio, seed=seed)

    packed_seed = struct.pack('<QQQQ', seed, seed, seed, seed)
    with bayeslite.bayesdb_open(seed=packed_seed) as bdb:
        loom_store_path = temp_file_path('.bdb')
        loom_backend = LoomBackend(loom_store_path=loom_store_path)
        bayeslite.bayesdb_register_backend(bdb, loom_backend)

        dataframe = pd.DataFrame(data=samples)
        read_pandas.bayesdb_read_pandas_df(bdb, 'data', dataframe, create=True)

        bdb.execute('''
            CREATE POPULATION FOR data WITH SCHEMA (
                GUESS STATTYPES OF (*)
            )
        ''')
        bdb.execute('CREATE GENERATOR FOR data USING loom;')
        bdb.execute('INITIALIZE 4 MODELS FOR data;')
        bdb.execute('ANALYZE data FOR 10 ITERATIONS;')

        cursor = bdb.execute('SIMULATE "0", "1" FROM data LIMIT ?;', (sample_size,))
        simulated_samples = [sample for sample in cursor]
    return samples, simulated_samples

def test_inference_quality():
    means = ((0,20), (20,0)) # means of the two gaussians
    sample_size = 100
    mix_ratio = [0.7, 0.3]
    seed = 9999
    samples, simulated_samples = gen_gaussians_and_samples(means, sample_size, mix_ratio, seed)

    counts = collections.Counter(
        (0 if distance((x,y), means[0]) < distance((x,y), means[1]) else 1
         for x, y
         in simulated_samples)
    )
    simulated_mix_ratio = [counts[key] / float(len(simulated_samples)) for key in counts]

    for i in range(len(means)):
        difference = abs(mix_ratio[i] - simulated_mix_ratio[i])
        assert difference < 0.1
