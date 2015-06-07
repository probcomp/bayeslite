import bayeslite.crosscat
import bayeslite.core as core
import exputils as eu

import numpy as np
import pandas as pd

from crosscat.MultiprocessingEngine import MultiprocessingEngine
from crosscat.tests.quality_tests import synthetic_data_generator as sdg
from bayeslite.shell.pretty import pp_cursor

import sys
import tempfile
import json

DO_PLOT = True
try:
    from matplotlib import pyplot as plt
    from matplotlib import gridspec
    try:
        import seaborn as sns
    except:
        pass
except ImportError:
    DO_PLOT = False


def pprint(cursor):
    return pp_cursor(sys.stdout, cursor)

def permute_no(dataset, n_model, colno, coltypes):
    return permute(dataset, n_model, colno, coltypes, rows = False, cols = False)

def permute_rows(dataset, n_model, colno, coltypes):
    return permute(dataset, n_model, colno, coltypes, rows = True, cols = False)

def permute_cols(dataset, n_model, colno, coltypes):
    return permute(dataset, n_model, colno, coltypes, rows=False, cols=True)

def permute_full(dataset, n_model, colno, coltypes):
    return permute(dataset, n_model, colno, coltypes, rows=True, cols=True)

def permute(dataset, n_model, colno, coltypes, rows=False, cols=False):
    new_datasets, new_colno, new_coltypes = [], [], []
    for _ in xrange(n_model):
        if cols:
            permutation = np.random.permutation(dataset.shape[1])
        else:
            permutation = np.arange(dataset.shape[1])

        D = dataset[:,permutation]
        if rows:
            np.random.shuffle(D)
        new_datasets.append(D)
        
        C = np.asarray([coltypes[i] for i in permutation])
        new_coltypes.append(C)

        N = np.where(permutation == colno)[0][0]
        new_colno.append(N)

    return new_datasets, new_colno, new_coltypes

def train_models(args, permute = None):

    bdb = bayeslite.bayesdb_open()
    engine = bayeslite.crosscat.CrosscatMetamodel(
            MultiprocessingEngine())
    bayeslite.bayesdb_register_metamodel(bdb, engine)

    perm_func_lookup = {
    'full_permute':permute_full,
    'row_permute':permute_rows,
    'col_permute':permute_cols,
    'no_permute':permute_no
    }; pfunc = perm_func_lookup[permute]
    datasets, colnos, coltypes = pfunc(args['dataset'], args['n_model'], args['colno'], args['coltypes'])

    model_hypers = {i:[] for i in xrange(args['n_model'])}
    for k in xrange(args['n_model']):        
        print "ANALYZING MODEL {}: Iterations".format(k),
        sys.stdout.flush()

        temp = tempfile.NamedTemporaryFile()
        eu.data_to_csv(np.asarray(datasets[k]), temp.name)
        
        btable = 'hyper{}'.format(k)
        generator = 'hyper_cc{}'.format(k)
        bayeslite.bayesdb_read_csv_file(bdb, btable, temp.name, header=True, create=True)
        temp.close()

        C = ['c{} {}'.format(s, coltypes[k][s]) for s in xrange(len(coltypes[k]))]
        bql = '''
        CREATE GENERATOR {} FOR {}
            USING crosscat (
                {}
            );
        '''.format(generator, btable, str(C)[2:-2].replace('\'',''))
        bdb.execute(bql)

        generator_id = core.bayesdb_get_generator(bdb, generator)

        bql = '''
        INITIALIZE 1 MODELS FOR {}
        '''.format(generator)
        bdb.execute(bql)

        total_iters = args['step_size']
        while (total_iters <= args['target_iters']):
            print total_iters,
            sys.stdout.flush()
            
            bql = '''
            ANALYZE {} FOR {} ITERATIONS WAIT;
            '''.format(generator, args['step_size'])
            bdb.execute(bql)

            sql = '''
            SELECT theta_json FROM bayesdb_crosscat_theta WHERE generator_id = {}
            '''.format(generator_id)
            cursor = bdb.sql_execute(sql)
            (theta_json,) = cursor.fetchall()[0]
            theta = json.loads(theta_json)
            
            model_hypers[k].append(theta['X_L']['column_hypers'][colnos[k]])
            total_iters += args['step_size']
        print

    bdb.close()
    return model_hypers

def runner(args):
    np.random.seed(args['seed'])
    results = {}
    results['args'] = args
    perm_types = ['no_permute', 'row_permute', 'col_permute', 'full_permute']
    for perm in perm_types:
        print '\n=================={}==================\n'.format(perm)
        results[perm] = train_models(args, permute = perm)

    return results

def plot(result, filename=None):

    args = result['args']
    n_model = args['n_model']
    step_size = args['step_size']
    target_iters = args['target_iters']

    fig, axes = plt.subplots(2,2)
    axes = axes.ravel()

    permutations = ['no_permute','row_permute','col_permute','full_permute']
    for (ax, title) in zip(axes, permutations):
        ax.set_title(title)
        ax.set_xlabel('Number of Iterations')
        ax.set_ylabel(r'Hyperparameter $\mu$')
        ax.grid()
        
        hypers = result[title]
        averages = []
        xs = np.arange(step_size, target_iters + 1, step_size)
        for i in hypers:
            vals = [hypers[i][k]['mu'] for k in xrange(len(hypers[i]))]
            ax.plot(xs, vals, alpha=0.4)
            averages.append(vals)
        averages = np.mean(np.asarray(averages), axis = 0)
        ax.plot(xs, averages, linestyle='--', label='mean', alpha = 1, color = 'black')
        ax.legend()
    
    if not DO_PLOT:
        import time
        filename = 'exp_hyperparams_results_' + str(time.time()) + '.png'

    if filename is None:
        plt.show()
    else:
        plt.savefig(filename)

if __name__ == '__main__':
    args = {
    'n_model' : 10,
    'n_samples' : 150,
    'step_size' : 5,
    'target_iters' : 150,
    'seed' : 410
    }

    # GENERATE SYNTHETIC DATA
    coltypes = ['NUMERICAL','NUMERICAL','CATEGORICAL', 'NUMERICAL', 'NUMERICAL','NUMERICAL']
    cctypes = ['continuous','continuous','multinomial', 'continuous', 'multinomial','multinomial']
    distargs = [None, None, dict(K=9), None, dict(K=7), dict(K=4)]
    cols_to_views = [0, 0, 0, 1, 1, 2]
    cluster_weights = [[.1, .7, .2],[.5, .5],[.4, .4, .2]]
    separation = [0.4, 0.4, 0.7]
    sdata = sdg.gen_data(cctypes, 
        args['n_samples'],
        cols_to_views, 
        cluster_weights, 
        separation, 
        seed=args['seed'], distargs=distargs, 
        return_structure=True)

    args['dataset'] = np.asarray(sdata[0])
    args['coltypes'] = coltypes
    args['colno'] = 3
    
    result = runner(args)
    plot(result)
