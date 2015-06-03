import bayeslite
import bayeslite.crosscat
import exputils as eu
import tempfile
import numpy as np

from crosscat.MultiprocessingEngine import MultiprocessingEngine

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


def run(args):
    result = {}
    result['args'] = args
    result['n_distractors'] = {}
    deptype = args['dependent_type'].lower()
    n = args['n']
    clarity = args['clarity']
    if deptype == 'corr':
        deppair_0 = eu.gen_correlated_data(n, clarity)
        deppair_1 = eu.gen_correlated_data(n, clarity)
    else:
        deppair_0 = eu.gen_zero_correlation_data(deptype, n, clarity)
        deppair_1 = eu.gen_zero_correlation_data(deptype, n, clarity)

    for i, ndiscols in enumerate(args['n_distractors']):
        noise = np.random.randn(n, ndiscols)
        data = np.hstack((deppair_0, deppair_1, noise))
        temp = tempfile.NamedTemporaryFile()
        eu.data_to_csv(data, temp.name)

        btable = 'ndistable'
        generator = btable + '_cc'

        with bayeslite.bayesdb_open() as bdb:
            engine = bayeslite.crosscat.CrosscatMetamodel(
                MultiprocessingEngine())
            bayeslite.bayesdb_register_metamodel(bdb, engine)
            bayeslite.bayesdb_read_csv_file(bdb, btable, temp.name,
                                            header=True, create=True)

            bql = '''
            CREATE GENERATOR {} FOR {}
                USING crosscat (
                   GUESS(*)
                );
            '''.format(generator, btable)
            bdb.execute(bql)

            bql = '''
            INITIALIZE {} MODELS FOR {};
            '''.format(args['n_model'], generator)
            bdb.execute(bql)

            total_itr = 0
            while total_itr < args['n_iter']:
                bql = '''
                ANALYZE {} for {} ITERATIONS WAIT;
                '''.format(generator, args['step'])
                bdb.execute(bql)

                total_itr += args['step']

                bql = '''
                CREATE TEMP TABLE depprob AS
                    ESTIMATE PAIRWISE DEPENDENCE PROBABILITY FROM {};
                '''.format(generator)
                bdb.execute(bql)

                # dependence of first dependent pair
                bql = '''
                SELECT dp FROM depprob
                    WHERE name_0 = col_0 AND name_1 = col_1;
                '''
                c = bdb.execute(bql)
                dep_pair_0 = c.fetchall()[0]

                # dependence of second dependent pair
                bql = '''
                SELECT dp FROM depprob
                    WHERE name_0 = col_2 AND name_1 = col_3;
                '''
                c = bdb.execute(bql)
                dep_pair_1 = c.fetchall()[0]

                # everytthing else
                bql = '''
                SELECT dp FROM depprob
                    WHERE name_0 = col_2 AND name_1 = col_3;
                '''
                c = bdb.execute(bql)
                indep_pairs = c.fetchall()

            result['n_distractor'][ndiscols]['dep_pair_0'].append(dep_pair_0)
            result['n_distractor'][ndiscols]['dep_pair_1'].append(dep_pair_1)
            result['n_distractor'][ndiscols]['indep_pairs'].append(indep_pairs)

            temp.close()

    return result


def plot(result, filename=None):
    args = result['args']
    num_tests = len(result['n_distractor'])
    gs = gridspec.GridSpec(num_tests, 2)
    plt.figure(facecolor='white', figsize=(10, num_tests*3))
    plt.tight_layout()
    x = range(result['args']['step'], result['args']['n_iter']+1,
              result['args']['step'])
    for i, (ndiscols, data) in enumerate(result['n_distractor'].iteritems()):
        ax = plt.subplot(gs[i, :])
        dep_pair_0 = np.array(data['dep_pair_0'])
        dep_pair_1 = np.array(data['dep_pair_1'])
        indep_pairs = np.array(data['indep_pairs'])

        ax.plot(x, indep_pairs, lw=1, c='gray', alpha=.3, label='Noise pair')
        ax.plot(x, dep_pair_0, lw=1, c='dodgerblue', alpha=.3, label='Dependent pair 0')
        ax.plot(x, dep_pair_1, lw=1, c='gray', alpha=.3, label='Depende pair 1')
        ax.ylim([0, 1])
        ax.set_xlabel('Iteration')
        ax.set_ylabel('Dependence Probability')

    plt.suptitle('N=%i, n_models=%i, n_iter=%i' %
                 (args['n'], args['n_model'], args['n_iter']))

    if not DO_PLOT:
        import time
        filename = 'exp_haystacks_results_' + str(time.time()) + '.png'

    if filename is None:
        plt.show()
    else:
        plt.savefig(filename)


if __name__ == '__main__':
    args = {
        'n_iter': 200,
        'n_model': 8,
        'n': 500,
        'clarity': .95
        }
    result = run(args)
    plot(result)
