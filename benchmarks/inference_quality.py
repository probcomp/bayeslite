import os
import struct
import time
import pytest

from sklearn.neighbors.kde import KernelDensity
import numpy as np
import pandas as pd

import bayeslite
from bayeslite import bayesdb_open
from bayeslite import bayesdb_register_backend
from iventure.utils_bql import query

try:
    from bayeslite.backends.loom_backend import LoomBackend

except ImportError:
    pytest.skip('Failed to import Loom.')


# TODO: Make these function argumemts!
NCOLS = 500
NCENTERS = 10
MEANS = [
    [np.random.uniform(-20, 20) for _ in range(NCOLS)]
    for _ in range(NCENTERS)
]
COVS = [
    np.diag(np.random.uniform(0, 5, (NCOLS,)))
    for _ in range(NCENTERS)
]


def sample_gaussian():
    # Meh. choice works onlhy on 1-d arrays.
    parameter_index = np.random.choice(range(NCENTERS))
    return np.random.multivariate_normal(
        MEANS[parameter_index],
        COVS[parameter_index]
    )

def data_gen_sample(N):
    """Sample from a mixture model"""
    return np.asarray([sample_gaussian() for _ in range(N)])

def get_KDE_logpdf(data):
    """Get an approximation of the logpdf as a function of a data point and
    return it."""
    kde = KernelDensity(kernel='gaussian', bandwidth=0.2).fit(data)
    return kde.score_samples

def data_gen_logpdf(data):
    # TODO: Add in the the true lopdf!
    return get_KDE_logpdf(data)

def compute_KL(data, logpdf_p, logpdf_q):
    N = data.shape[0]
    return np.sum(logpdf_p(data) - logpdf_q(data))/N

def prep_bdb(seed, backend, number_iterations, N):
    """ This functions prepare a bdb object.
        It reads the csv file, creates the population and runs analysis.
    """
    # TODO: extract out as a function.
    file_name_template =\
        'backend={backend}_number_iterations={number_iterations}_seed={seed}'
    file_name_stem = file_name_template.format(
        seed=seed,
        backend=backend,
        number_iterations=number_iterations,
    )
    bdb_file_name = 'bdb/' + file_name_stem + '.bdb'
    if os.path.exists(bdb_file_name):
        os.remove(bdb_file_name)
    # TODO: check setting of the seed -- is this really correct?
    byte_str_seed = struct.pack(
        '<QQQQ',
        seed,
        seed,
        seed,
        seed,
    )
    bdb = bayesdb_open(bdb_file_name, seed=byte_str_seed)

    # Generate datafile
    csv_file_name = 'csv/' + file_name_stem + '.csv'
    data = data_gen_sample(N)
    df = pd.DataFrame(data=data)
    df.to_csv(csv_file_name, index=False)

    bdb.execute('''
        CREATE TABLE synthetic_data FROM '{csv_file_name}';
    '''.format(csv_file_name=csv_file_name)
    )
    bdb.execute('''
        CREATE POPULATION FOR synthetic_data  WITH SCHEMA(
            GUESS STATTYPES OF (*)
        );
    ''')
    if backend == 'loom':
        bayesdb_register_backend(bdb,
              LoomBackend(loom_store_path='/tmp/loom.store'))
    return bdb, file_name_stem, data

def analyze_bdb(bdb, backend, number_iterations):
    """Initialize models and analyze data."""
    if backend == 'loom':
        bdb.execute('CREATE GENERATOR FOR synthetic_data USING loom;')
    elif backend == 'cgpm':
        bdb.backends['cgpm'].set_multiprocess(False)
        bdb.execute('CREATE GENERATOR FOR synthetic_data;')
    else:
        raise ValueError('Backend unknown: %s' % (backend,))
    bdb.execute('INITIALIZE 1 MODELS FOR synthetic_data;')
    starting_time = time.time()
    bdb.execute('''
            ANALYZE synthetic_data FOR {number_iterations} ITERATION;
    '''.format(number_iterations=number_iterations)
    )
    time_elapsed = time.time() - starting_time
    return bdb, time_elapsed

def simulate(bdb, M):
    df = query(bdb, '''
        SIMULATE {cols} FROM synthetic_data LIMIT {m};
    '''.format(m=M,  cols=','.join(['"' +str(i) + '"' for i in range(NCOLS)])))
    return df.values

def mkdir(target_dir):
    """Make a directory if it does not exist."""
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)


@pytest.mark.parametrize('seed', [1, 2, 3])
@pytest.mark.parametrize('backend', [
    #'loom',
    'cgpm'
])
#@pytest.mark.parametrize('number_iterations', [700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500])
@pytest.mark.parametrize('number_iterations', [
    1,
    5,
    10,
    20,
    30,
    40,
    50,
])
def test_run_experiment(seed, backend, number_iterations):

    # Ensure all the target directories exist:
    mkdir('bdb')
    mkdir('csv')
    mkdir('simulations')
    mkdir('results')
    np.random.seed(seed)
    # XXX
    N = 1000
    M = 1000
    bdb, file_name_stem, training_data = prep_bdb(
        seed,
        backend,
        number_iterations,
        N,
    )
    bdb, time_elapsed = analyze_bdb(bdb, backend, number_iterations)

    assert isinstance(time_elapsed, float)

    simulated_data = simulate(bdb, M)

    simulation_file = 'simulations/' +file_name_stem + '.csv'
    pd.DataFrame(data=simulated_data).to_csv(simulation_file, index=False)
    new_data_from_generator = data_gen_sample(M)

    # get logpdf for both, true data
    # XXX double check what is P and what is Q.
    logpdf_p = data_gen_logpdf(new_data_from_generator)
    logpdf_q = get_KDE_logpdf(simulated_data)

    kl = compute_KL(new_data_from_generator, logpdf_p, logpdf_q)
    result_file = 'results/' + file_name_stem + '.csv'
    pd.DataFrame({'Kl':[kl], 'seconds':[time_elapsed]}).to_csv(result_file, index=False)


