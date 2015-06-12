import bayeslite.crosscat
import bayeslite.core as core
import exputils as eu

import numpy as np
import scipy.integrate as integrate
import scipy.stats as stats

from crosscat.MultiprocessingEngine import MultiprocessingEngine
from crosscat.tests.quality_tests import synthetic_data_generator as sdg
from crosscat.tests.quality_tests import quality_test_utils as qtu
from crosscat.tests.component_model_extensions import *
from bayeslite.shell.pretty import pp_cursor
from bayeslite.sqlite3_util import sqlite3_quote_name as quote

import sys
import tempfile
import pickle
import time

DO_PLOT = True
try:
    import matplotlib
    from matplotlib import pyplot as plt
    from matplotlib import gridspec
    try:
        import seaborn as sns
        plt.rc('font', weight='bold')
    except:
        pass
except ImportError:
    DO_PLOT = False

def pprint(cursor):
    return pp_cursor(sys.stdout, cursor)

def runner(args):

    n_model = args['n_model']
    dataset = args['dataset']
    distargs = args['distargs']
    component_params = args['component_params']
    component_weights = args['component_weights']
    target_samples = args['target_samples']
    checkpoints = args['checkpoints']
    col_types = args['col_types']
    num_cols = len(col_types)

    table = 'test'
    temp = tempfile.NamedTemporaryFile()
    eu.data_to_csv(np.asarray(dataset), temp.name)

    bdb = bayeslite.bayesdb_open()
    engine = bayeslite.crosscat.CrosscatMetamodel(
            MultiprocessingEngine())
    bayeslite.bayesdb_register_metamodel(bdb, engine)
    bayeslite.bayesdb_read_csv_file(bdb, table, temp.name,
        header=True, create=True)
    temp.close()

    col_names = ['c{}'.format(s) for s in xrange(num_cols)]
    col_names_types = str([col_names[s]+' '+col_types[s]
        for s in xrange(num_cols)])

    bql = '''
        CREATE DEFAULT GENERATOR {} FOR {}
        USING crosscat (
            {} );
        '''.format(quote(table+'_cc'), quote(table),
    col_names_types[2:-2].replace('\'',''))
    bdb.execute(bql)

    bql = '''
        INITIALIZE {} MODELS FOR {}
        '''.format(n_model, quote(table))
    bdb.execute(bql)

    print 'ANALYZING {} MODELS WITH {} SAMPLES'.format(n_model,target_samples)
    print '\tITERATION CHECKPOINTS {}'.format(checkpoints)

    print '\t(numerical integration will take a while...)'

    # KL_vals[v,j] = KL div of variable v (wrt synthetic distr) 
    # when CC ANALZYED for j iterations
    KL_vals = np.zeros((num_cols, len(checkpoints)))

    last_iters = 0; k = 0
    for current_iters in checkpoints:
        print '\n\tAT ITERATION {}'.format(current_iters)

        bql = '''
            ANALYZE {} FOR {} ITERATIONS WAIT;
            '''.format(quote(table), current_iters - last_iters)
        bdb.execute(bql)

        kl = 0
        for i in xrange(num_cols):
            print '\t\tINTEGRATING COL {} ({})'.format(i,col_types[i])
            
            if col_types[i] == 'NUMERICAL':
                
                # kernel for KL divergence integral
                def kl_func(x):
                    # compute bayesdb log density
                    bql = '''
                        ESTIMATE PROBABILITY OF {}=? FROM {} LIMIT 1
                        '''.format(quote(col_names[i]),quote(table))
                    crs = bdb.execute(bql, (x,))
                    log_q = np.log(crs.fetchall()[0][0])

                    # compute actual log density
                    log_p = qtu.get_mixture_pdf(np.asarray([x]),
                        ContinuousComponentModel.p_ContinuousComponentModel,
                        component_params[i],
                        component_weights[i])
                    return np.exp(log_p) * (log_p - log_q)

                kl, error = integrate.quad(kl_func, -np.inf, np.inf)

            # evaluate KL divergence by discrete entropy
            elif col_types[i] == 'CATEGORICAL':
                num_classes = distargs[i]

                # compute crosscat pmf
                q_pmf = []
                for x in xrange(num_classes):
                    bql = '''
                        ESTIMATE PROBABILITY OF {}=? FROM {} LIMIT 1
                        '''.format(quote(col_names[i]),quote(table))
                    crs = bdb.execute(bql, (x,))
                    q_pmf.append(crs.fetchall()[0][0])

                # compute actual pmf
                p_pmf = np.exp(qtu.get_mixture_pdf(
                    np.asarray(xrange(num_classes)),
                    MultinomialComponentModel.p_MultinomialComponentModel,
                    component_params[i],
                    component_weights[i]))

                kl = stats.entropy(p_pmf, qk = q_pmf)

            # cannot support cyclic data types for now
            else:
                raise Exception('Calculating KL divergence for column type' \
                    'is not supported'.format(col_types[i]))

            KL_vals[i, k] = kl

        last_iters = current_iters
        k += 1

    bdb.close()
    
    result = {}
    result['args'] = args
    result['KL_vals'] = KL_vals
    
    picklename = 'exp_kl_divergence' + str(time.time()) + '.pkl'
    pickle.dump(result, file(picklename,'w'))

    return result

def plot(result, filename=None):
    args = result['args']
    target_samples = args['target_samples']
    checkpoints = args['checkpoints']
    col_types = args['col_types']
    KL_vals = result['KL_vals']

    fig, ax = plt.subplots()
    ax.set_xlabel('Number of Iterations', fontsize =16, fontweight = 'bold')
    ax.set_ylabel('D(p||q)', fontsize=16, fontweight = 'bold')
    ax.set_title('KL Divergence of Predictive Distribution\n' +
        'from True Distribution ({} Samples)'.format(target_samples),
        fontsize = 20,
        fontweight = 'bold') 

    for (i, col_kl) in enumerate(KL_vals):
        ax.semilogx(checkpoints, col_kl, 
            marker='o',
            label = "{} {}".format(i,col_types[i]),
            basex=2)

    ax.legend(loc = 'best')
    ax.grid()

    picklename = 'exp_kl_divergence' + str(time.time()) + '.pkl'
    pickle.dump(ax, file(picklename,'w'))

    if not DO_PLOT:
        filename = 'exp_kl_divergence' + str(time.time()) + '.png'

    if filename is None:
        plt.show(block=False)
    else:
        plt.savefig(filename)


if __name__ == '__main__':

    # initiaze experiment arguments
    args = {
    'n_model': 5,
    'checkpoints': [0] + [2**i for i in xrange(9)],
    'target_samples': 250,
    'seed' : 448,
    }

    # generate synthetic data
    cols_to_views = [
        0, 0, 0, 0, 0,
        1, 1, 1, 1,
        2, 2, 2,
        3,
        4]

    cctypes = [
        'multinomial','continuous','multinomial','continuous','multinomial',
        'multinomial','continuous','continuous','multinomial',
        'continuous','continuous','continuous',
        'continuous',
        'continuous']

    distargs = [
        dict(K=9), None, dict(K=9), None, dict(K=7),
        dict(K=4), None, None, dict(K=9),
        None, None, None,
        None,
        None]

    component_weights = [
        [.2, .3, .5],
        [.9, .1],
        [.4, .4, .2],
        [.8, .2],
        [.4, .5, .1]]

    separation = [0.8, 0.9, 0.65, 0.7, 0.75]

    # cols_to_views = [0]
    # cctypes = ['multinomial']
    # distargs = [dict(K=9)]
    # component_weights = [[.2, .3, .5]]
    # separation = [0.8]

    synethic_data = sdg.gen_data(cctypes,
        args['target_samples'],
        cols_to_views,
        component_weights,
        separation,
        seed=args['seed'], distargs=distargs,
        return_structure=True)

    # include dataset metadata for KL computations
    args['dataset'] = np.asarray(synethic_data[0])
    args['component_params'] = synethic_data[2]['component_params']
    args['component_weights'] = [component_weights[c] for c in cols_to_views]
    args['distargs'] = [None if x is None else x['K'] for x in distargs]
    args['col_types'] = ['NUMERICAL' if s == 'continuous' else 'CATEGORICAL'
        for s in cctypes]

    # result = runner(args)
    
    
    picklename = 'exp_kl_divergence1434147218.51.pkl'
    result = pickle.load(file(picklename))
    plot(result)
