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
    import matplotlib.patches as mpatches
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
    target_samples = args['target_samples']
    target_iterations = args['target_iterations']
    component_params = args['component_params']
    component_weights = args['component_weights']
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

    print 'ANALYZING {} MODELS WITH {} SAMPLES FOR {} ITERATIONS'.format(
        n_model,target_samples,target_iterations)
    
    bql = '''
        ANALYZE {} FOR {} ITERATIONS WAIT;
        '''.format(quote(table), target_iterations)
    bdb.execute(bql)

    XS = {i:None for i in xrange(num_cols)}
    CC = {i:None for i in xrange(num_cols)}
    SY = {i:None for i in xrange(num_cols)}

    for c in xrange(num_cols):
        print 'COMPUTING POSTERIOR PREDICTIVE FOR COLUMN {} ({})'.format(c,
            col_types[c])

        if col_types[c] == 'NUMERICAL':
            # find support to evaluate pdf (+/- 2 stds)
            mus, stds = zip(*[ (h['mu'],np.sqrt(1./h['rho'])) for 
                h in component_params[c]])
            widths = [(mus[i]-2*stds[i],mus[i]+2*stds[i] )
                for i in xrange(len(mus))]
            left = np.min(widths,axis=0)[0]
            right = np.max(widths,axis=0)[1]
            XS[c] = np.linspace(left,right, 500)

            # pdf under true model
            SY[c] = np.exp(qtu.get_mixture_pdf(
                        XS[c],
                        ContinuousComponentModel.p_ContinuousComponentModel,
                        component_params[c],
                        component_weights[c]))

        elif col_types[c] == 'CATEGORICAL':
            # find support to evaluate pmf (discrete range)
            num_classes = distargs[c]
            XS[c] = np.arange(num_classes)

            # pmf under true model
            SY[c] = np.exp(qtu.get_mixture_pdf(
                        np.asarray(XS[c]),
                        MultinomialComponentModel.p_MultinomialComponentModel,
                        component_params[c],
                        component_weights[c]))

        # cannot support cyclic data types for now
        else:
            raise Exception('Computing probability density for column type' \
                'is not supported'.format(col_types[c]))

        # pdf under predictive cc model
        CC[c] = []
        for x in XS[c]:
            bql = '''
                ESTIMATE PROBABILITY OF {}=? FROM {} LIMIT 1
                '''.format(quote(col_names[c]),quote(table))
            crs = bdb.execute(bql, (x,))
            CC[c].append(crs.fetchall()[0][0])

    bdb.close()

    result = {}
    result['args'] = args
    result['XS']  = XS
    result['CC'] = CC
    result['SY'] = SY
    
    picklename = 'exp_predictive' + str(time.time()) + '.pkl'
    pickle.dump(result, file(picklename,'w'))
    
    return result

def plot(result, filename=None):
    args = result['args']

    n_model = args['n_model']
    dataset = args['dataset']
    distargs = args['distargs']
    target_samples = args['target_samples']
    target_iterations = args['target_iterations']
    component_params = args['component_params']
    component_weights = args['component_weights']
    col_types = args['col_types']
    num_cols = len(col_types)

    XS = result['XS']
    CC = result['CC']
    SY = result['SY']

    fig, axes = plt.subplots(2, num_cols/2)
    fig.subplots_adjust(hspace=0.3, wspace=0.3)
    fig.suptitle('Comparison of True and Posterior Predictive\n'
            'Distributions ({} Samples, {} Iterations)'.format(
            target_samples, target_iterations), 
            fontweight = 'bold',
            fontsize = 20) 

    for c, ax in zip(xrange(num_cols), axes.ravel()):
        
        
        if c % (num_cols/2) == 0:
            ax.set_ylabel('Density',
            fontsize=16,
            fontweight = 'bold', labelpad=20)
        
        if col_types[c] == 'NUMERICAL':
            ax.fill_between(XS[c], CC[c], y2 = 0,
                alpha=.5,
                color='red')
            ax.fill_between(XS[c], SY[c], y2 = 0,
                alpha=.5,
                color='blue')
            ax.set_ylim([0, ax.get_ylim()[1]])

        elif col_types[c] == 'CATEGORICAL':
            ax.bar(XS[c], CC[c], 0.35,
                align='center', 
                alpha=.5,
                color = 'red')
            ax.bar(np.asarray(XS[c])+0.35, SY[c], 0.35,
                align='center',
                alpha=.5,
                color = 'blue')
        else:
            raise Exception('Plotting density of column type' \
                'is not supported'.format(col_types[c]))

        ax.set_xlabel(r'$x$', labelpad=0)
        ax.set_xticklabels([])
        ax.grid(True)

    red_patch = mpatches.Patch(color='red', alpha=0.5)
    blue_patch = mpatches.Patch(color='blue', alpha=0.5)
    plt.figlegend( (red_patch,blue_patch), ('Predictive PDF','True PDF'),
        'center',prop={'size':'large'})

    plt.show(block=False)

    if not DO_PLOT:
        filename = 'exp_predictive' + str(time.time()) + '.png'

    if filename is None:
        plt.show(block=False)
    else:
        plt.savefig(filename)


if __name__ == '__main__':

    # initiaze experiment arguments
    args = {
    'n_model': 5,
    'target_iterations': 20,
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

    result = runner(args)
    
    # picklename = 'exp_predictive1434146762.92.pkl'
    # result = pickle.load(file(filename))
    plot(result)
