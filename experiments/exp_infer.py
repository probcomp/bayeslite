
import bayeslite
import bayeslite.core
import bayeslite.crosscat
import exputils as eu
import numpy as np
import tempfile
import random
import os

from bayeslite.sqlite3_util import sqlite3_quote_name as quote
from crosscat.MultiprocessingEngine import MultiprocessingEngine

DHA_CSV = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                       'tests', 'dha.csv'))

DO_PLOT = True
try:
    from matplotlib import pyplot as plt
    try:
        import seaborn as sns
    except:
        pass
except ImportError:
    DO_PLOT = False


def impute_missing_values(bdb, generator, key, cols, indices):
    """ Imputes the missing values in col from generator and returns their
    values and confidences in a list ordered by indices (key values).
    """

    confs = []
    values = []
    for col in cols:
        bql = '''
        CREATE TEMP TABLE tt AS
            INFER EXPLICIT {}, {}, PREDICT {} AS pred CONFIDENCE conf
            FROM {}
            WHERE {} IS NULL;
        '''.format(key, col, col, generator, col)
        bdb.execute(bql)

        for idx in indices[col]:
            sql = 'SELECT * FROM tt WHERE {} = ?'.format(key)
            row = bdb.sql_execute(sql, (idx,)).fetchall()

            conf = row[0][-1]
            value = row[0][-2]

            confs.append(conf)
            values.append(value)

        bdb.sql_execute('DROP TABLE tt;')

    return values, confs


def run(args):
    result = {}
    result['args'] = args
    result['iters'] = [0]
    result['confs'] = []

    btable = 'dha_infer'
    generator = 'dha_infer_cc'

    all_cols = eu.read_csv_header(DHA_CSV)
    del all_cols[all_cols.index('NAME')]

    temp = tempfile.NamedTemporaryFile()
    indices, values = eu.remove_csv_values(DHA_CSV, temp.name, all_cols,
                                           args['prop_missing'])

    impute_cols = indices.keys()

    with bayeslite.bayesdb_open() as bdb:
        engine = bayeslite.crosscat.CrosscatMetamodel(MultiprocessingEngine())
        bayeslite.bayesdb_register_metamodel(bdb, engine)
        bayeslite.bayesdb_read_csv_file(bdb, btable, temp.name, header=True,
                                        create=True)

        for col in impute_cols:
            sql = '''
            UPDATE {} SET {} = NULL WHERE {} = ?
            '''.format(quote(btable), quote(col), quote(col))
            bdb.sql_execute(sql, ('NaN',))

        bql = '''
        CREATE GENERATOR {} FOR {}
            USING crosscat (
                GUESS (*),
                Name KEY
            );
        '''.format(generator, btable)
        bdb.execute(bql)

        bql = '''
        INITIALIZE {} MODELS FOR {}
        '''.format(args['n_model'], generator)
        bdb.execute(bql)

        imputed, confs = impute_missing_values(bdb, generator, 'Name',
                                               impute_cols, indices)
        result['confs'].append(confs)

        itr = 0
        while itr < args['n_iter']:
            bql = '''
            ANALYZE {} FOR {} ITERATIONS WAIT;
            '''.format(generator, args['step'])
            bdb.execute(bql)

            itr += args['step']

            imputed, confs = impute_missing_values(bdb, generator, 'Name',
                                                   impute_cols, indices)
            print "."

            result['confs'].append(confs)
            result['iters'].append(itr)

    return result


def plot_scatter(result, filename=None):
    iterations = []
    confidence = []
    conf_mean = np.mean(np.array(result['confs']), axis=1)
    for itr, confs in zip(result['iters'], result['confs']):
        for conf in confs:
            # add some jitter to iterations
            iterations.append(itr + random.random()-.5)
            confidence.append(conf)

    plt.figure(facecolor='white')
    plt.scatter(iterations, confidence, color='black', alpha=.5)
    plt.plot(result['iters'], conf_mean, color='dodgerblue', alpha=.8, lw=3,
             label='mean')
    plt.xlabel('# Iterations')
    plt.ylabel('Max confidence')
    plt.ylim([-.1, 1.1])
    plt.legend(loc=0)

    if not DO_PLOT:
        import time
        filename = 'exp_infer_results_' + str(time.time()) + '.png'

    if filename is None:
        plt.show()
    else:
        plt.savefig(filename)


def plot_line(result, filename=None):
    confs = np.array(result['confs'])
    conf_mean = np.mean(confs, axis=1)

    plt.figure(facecolor='white')
    for col in range(confs.shape[1]):
        plt.plot(result['iters'], confs[:, col], color='black', alpha=.5)
    plt.plot(result['iters'], conf_mean, color='dodgerblue', alpha=.8, lw=3,
             label='Mean')
    plt.xlabel('# Iterations')
    plt.ylabel('Max confidence')
    plt.ylim([-.1, 1.1])
    plt.legend(loc=0)

    if not DO_PLOT:
        import time
        filename = 'exp_infer_results_' + str(time.time()) + '.png'

    if filename is None:
        plt.show()
    else:
        plt.savefig(filename)


if __name__ == '__main__':
    args = {
        'n_iter': 500,
        'n_model': 8,
        'prop_missing': .005,
        'step': 10,
    }
    result = run(args)
    plot_line(result)
    plot_scatter(result)
