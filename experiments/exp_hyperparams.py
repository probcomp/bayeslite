import bayeslite.crosscat
import bayeslite.core as core
import exputils as eu

import numpy as np
import pandas as pd
import scipy.stats as stats
from scipy.special import gammaln

from crosscat.MultiprocessingEngine import MultiprocessingEngine
from crosscat.tests.quality_tests import synthetic_data_generator as sdg
from bayeslite.shell.pretty import pp_cursor
from bayeslite.sqlite3_util import sqlite3_quote_name

import sys
import tempfile
import json
import itertools

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

def train_models_by_sample(args):
    bdb = bayeslite.bayesdb_open()
    engine = bayeslite.crosscat.CrosscatMetamodel(
            MultiprocessingEngine())
    bayeslite.bayesdb_register_metamodel(bdb, engine)
    colno = args['colno']
    coltypes = args['coltypes']
    model_hypers = {i:[] for i in xrange(args['n_model'])}

    total_samples = args['initial_size']
    while (total_samples <= args['target_samples']):

        temp = tempfile.NamedTemporaryFile()
        eu.data_to_csv(np.asarray(args['dataset'][:total_samples]), temp.name)
        
        btable = 'hyper{}'.format(total_samples)
        generator = 'hyper{}_cc'.format(total_samples)
        bayeslite.bayesdb_read_csv_file(bdb, btable, temp.name, header=True, create=True)
        temp.close()

        C = ['c{} {}'.format(s, coltypes[s]) for s in xrange(len(coltypes))]
        bql = '''
        CREATE GENERATOR {} FOR {}
            USING crosscat (
                {}
            );
        '''.format(generator, sqlite3_quote_name(btable), str(C)[2:-2].replace('\'',''))
        bdb.execute(bql)

        bql = '''
        INITIALIZE {} MODELS FOR {}
        '''.format(args['n_model'], generator)
        bdb.execute(bql)

        print "ANALYZING {} MODELS WITH {} SAMPLES FOR {} ITERATIONS".format(
            args['n_model'], 
            total_samples, 
            args['target_iters'])
        
        bql = '''
        ANALYZE {} FOR {} ITERATIONS WAIT;
        '''.format(generator, args['target_iters'])
        bdb.execute(bql)

        generator_id = core.bayesdb_get_generator(bdb, generator)
        sql = '''
        SELECT theta_json FROM bayesdb_crosscat_theta WHERE generator_id = {}
        '''.format(generator_id)
        cursor = bdb.sql_execute(sql)
        for (k, (theta_json,)) in enumerate(cursor):
            theta = json.loads(theta_json)
            model_hypers[k].append(theta['X_L']['column_hypers'][colno])
        
        total_samples += args['step_size']

    bdb.close()
    return model_hypers

def train_models_by_iter(args):
    bdb = bayeslite.bayesdb_open()
    engine = bayeslite.crosscat.CrosscatMetamodel(
            MultiprocessingEngine())
    bayeslite.bayesdb_register_metamodel(bdb, engine)
    colno = args['colno']
    coltypes = args['coltypes']
    model_hypers = {i:[] for i in xrange(args['n_model'])}

    temp = tempfile.NamedTemporaryFile()
    eu.data_to_csv(np.asarray(args['dataset']), temp.name)
    
    btable = 'hyper{}'.format(args['n_model'])
    generator = 'hyper{}_cc'.format(args['n_model'])
    bayeslite.bayesdb_read_csv_file(bdb, btable, temp.name, header=True, create=True)
    qt_btable = sqlite3_quote_name(btable)
    temp.close()

    C = ['c{} {}'.format(s, coltypes[s]) for s in xrange(len(coltypes))]
    bql = '''
    CREATE GENERATOR {} FOR {}
        USING crosscat (
            {}
        );
    '''.format(generator, qt_btable, str(C)[2:-2].replace('\'',''))
    bdb.execute(bql)

    bql = '''
    INITIALIZE {} MODELS FOR {}
    '''.format(args['n_model'], generator)
    bdb.execute(bql)

    print "ANALYZING {} MODELS WITH {} SAMPLES FOR {} ITERATIONS".format(args['n_model'],
        args['target_samples'],
        args['target_iters'])
    total_iters = args['initial_size']
    while (total_iters <= args['target_iters']):
        print total_iters,
        sys.stdout.flush()
    
        bql = '''
        ANALYZE {} FOR {} ITERATIONS WAIT;
        '''.format(generator, args['step_size'])
        bdb.execute(bql)

        generator_id = core.bayesdb_get_generator(bdb, generator)
        sql = '''
        SELECT theta_json FROM bayesdb_crosscat_theta WHERE generator_id = {}
        '''.format(generator_id)
        cursor = bdb.sql_execute(sql)
        for (k, (theta_json,)) in enumerate(cursor):
            theta = json.loads(theta_json)
            model_hypers[k].append(theta['X_L']['column_hypers'][colno])
        total_iters += args['step_size']
    
    print
    bdb.close()
    return model_hypers

def runner(args):     
    results = {}
    results['args'] = args
    if args['byiter']:
        results['hypers'] = train_models_by_iter(args)
    else:
        results['hypers'] = train_models_by_sample(args)
    return results

def plot(result, filename=None):
    args = result['args']
    n_model = args['n_model']
    step_size = args['step_size']
    target_iters = args['target_iters']
    target_samples = args['target_samples']
    actual_params =  args['actual_component_params']
    actual_weights = args['actual_component_weights']

    actual_mus, actual_rhos = zip(*[ (h['mu'],h['rho']) for h in actual_params])
    mu_min, mu_max, mu_av = min(actual_mus), max(actual_mus), np.dot(actual_weights, actual_mus)
    rho_min, rho_max, rho_av = min(actual_rhos), max(actual_rhos), np.dot(actual_weights, actual_rhos)

    if args['byiter']:
        xs = np.arange(step_size, target_iters + 1, step_size)
        train_type = 'Iterations'
        train_count = target_iters
        fixed_type = 'Samples'
        fixed_count = target_samples
    else:
        xs = np.arange(step_size, target_samples + 1, step_size)
        train_type = 'Samples'
        train_count = target_samples
        fixed_type = 'Iterations'
        fixed_count = target_iters

    RESOLUTION = 70
    mus = np.linspace(mu_min-np.abs(mu_min), mu_max+np.abs(mu_min), RESOLUTION)
    rhos = np.linspace(0.4*rho_min, 1.6*rho_max, RESOLUTION)
    
    # prepare list of grids (one for each step)
    grid_pdfs = {i:None for i in xs}

    # prepare list of lines (one for each step)
    line_pdfs = np.zeros( (len(actual_params), len(xs)) )

    for counter in xrange(len(xs)):
        print "PLOTTING: {}".format(xs[counter])
        
        # prepare the grid
        grid = np.zeros((len(mus),len(rhos)))
            
        for model in xrange(args['n_model']):
            
            # obtain hyperparams at this stage in training
            h = result['hypers'][model][counter]
            m, s, r, nu = h['mu'], h['s'], h['r'], h['nu']

            # compute the density on the grid
            for i, (mu, rho) in enumerate(itertools.product(mus, rhos)):
                log_pdf_gamma = stats.gamma.logpdf(rho,a=nu/2.,scale=2./s)
                log_pdf_normal = stats.norm.logpdf(mu,loc=m,scale = np.sqrt(1./(r*rho)))
                log_pdf_joint = log_pdf_gamma + log_pdf_normal
                grid.flat[i] += log_pdf_joint

            # compute the density on the actual params
            for i, (mu, rho) in enumerate(zip(actual_mus,actual_rhos)):
                log_pdf_gamma = stats.gamma.logpdf(rho,a=nu/2.,scale=2./s)
                log_pdf_normal = stats.norm.logpdf(mu,loc=m,scale = np.sqrt(1./(r*rho)))
                log_pdf_joint = log_pdf_gamma + log_pdf_normal
                line_pdfs[i,counter] += log_pdf_joint

        # normalize density on the grid
        grid_pdfs[xs[counter]] = grid / -args['n_model']

        # normalize density on the actual params
        line_pdfs[:,counter] /= -args['n_model']

    # plot density of actual parameters
    fig, ax = plt.subplots()
    ax.set_xlabel('Number of {}'.format(train_type), fontweight = 'bold')
    ax.set_ylabel(r'$\log P(\mu,\rho | D)$', fontweight = 'bold')
    ax.set_title(r'Log Density of Mixture Component Mean and Precison $(\mu,\rho)$'+'\n'+
        'Under Posterior Dirichlet Process Base Distribution ({} {})'.format(fixed_count, fixed_type),
        fontweight = 'bold')

    for (i,row) in enumerate(line_pdfs):
        ax.plot(xs, row, label = r'$w_{}={}$'.format(i, actual_weights[i]), color = 'rbgmcyk'[i])
    ax.legend(loc=3)

    # plot density of the grid
    fig, axes = plt.subplots(nrows = 2, ncols = len(xs)/2, sharex=True, sharey=True)
    fig.suptitle(r'Log Density of Mixture Component Mean and Precison $(\mu,\rho)$'+'\n'+
        'Under Posterior Dirichlet Process Base Distribution ({} {})'.format(fixed_count, fixed_type),
        fontweight = 'bold')

    for counter, ax in zip(xs, axes.flat):
        ax.set_title(r'{} {}'.format(counter, train_type))
        ax.set_xlabel(r'$\mu$', fontweight = 'bold')
        ax.set_ylabel(r'$\rho$', fontweight = 'bold')
        heatmap = ax.pcolormesh(mus, rhos, grid_pdfs[counter],
            cmap=plt.cm.brg)
        ax.axis('tight')
        
        for weight, hypers in zip(actual_weights, actual_params):
            ax.annotate('x'+'\n'+str(weight),color='white',fontsize=12,xy=(hypers['mu'],hypers['rho']))
        
        ax.annotate('x', fontsize=12,xy=(mu_av,rho_av),color='white')
        
    cax,kw = matplotlib.colorbar.make_axes([ax for ax in axes.flat])
    cbar = fig.colorbar(heatmap, cax = cax, **kw)
    cbar.set_label(r'$\log P(\mu,\rho | D)$', fontweight = 'bold')

    if not DO_PLOT:
        import time
        filename = 'exp_hyperparams_results_' + str(time.time()) + '.png'

    if filename is None:
        plt.show(block=False)
    else:
        plt.savefig(filename)

if __name__ == '__main__':
    args = {
    'n_model' : 5,
    'step_size' : 2,
    'initial_size':5,
    'target_samples': 100,
    'target_iters' : 250,
    'seed' : 448,
    'byiter': True
    }

    # GENERATE SYNTHETIC DATA
    cctypes = ['continuous','continuous','multinomial', 'continuous', 'multinomial','multinomial',
    'continuous','continuous','multinomial', 'continuous', 'continuous','continuous','continuous','continuous']
    distargs = [None, None, dict(K=9), None, dict(K=7), dict(K=4),
    None, None, dict(K=9), None, None, None, None, None]
    cols_to_views = [0, 0, 0, 1, 1, 2, 1, 0, 2, 3, 1, 0, 4, 4]
    cluster_weights = [[.3, .3, .4],[.9,0.1],[.4, .4, .2],[.8, .2],[0.4,0.5,0.1]]
    separation = [0.6, 0.9, 0.5, 0.6, 0.15]
    sdata = sdg.gen_data(cctypes,
        1000,
        cols_to_views, 
        cluster_weights, 
        separation, 
        seed=args['seed'], distargs=distargs, 
        return_structure=True)

    coltypes = ['NUMERICAL' if s == 'continuous' else 'CATEGORICAL' for s in cctypes]
    args['coltypes'] = coltypes
    args['colno'] = 1

    args['actual_component_params'] = sdata[2]['component_params'][args['colno']]
    args['actual_component_weights'] = cluster_weights[cols_to_views[args['colno']]]

    args['dataset'] = np.asarray(sdata[0])
    result = runner(args)
    plot(result)
