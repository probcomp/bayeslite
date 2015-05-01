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
    import pdb; pdb.set_trace()
    DO_PLOT = False


def run(args):
    result = {}
    result['args'] = args
    result['shapes'] = {}
    shapes = ['dots', 'x', 'sine', 'diamond', 'ring']
    for i, shape in enumerate(shapes):
        data = eu.gen_zero_correlation_data(shape, args['n'], args['clarity'])
        temp = tempfile.NamedTemporaryFile()
        eu.data_to_csv(data, temp.name)

        result['shapes'][shape] = {}
        result['shapes'][shape]['original'] = data

        btable = shape
        generator = shape + '_cc'

        with bayeslite.bayesdb_open() as bdb:
            engine = bayeslite.crosscat.CrosscatMetamodel(
                MultiprocessingEngine())
            bayeslite.bayesdb_register_metamodel(bdb, engine)
            bayeslite.bayesdb_read_csv_file(bdb, btable, temp.name,
                                            header=True, create=True)

            bql = '''
            CREATE GENERATOR {} FOR {}
                USING crosscat (
                   x NUMERICAL,
                   y NUMERICAL
                );
            '''.format(generator, btable)
            bdb.execute(bql)

            bql = '''
            INITIALIZE {} MODELS FOR {};
            '''.format(args['n_model'], generator)
            bdb.execute(bql)

            bql = '''
            ANALYZE {} for {} ITERATIONS WAIT;
            '''.format(generator, args['n_iter'])
            bdb.execute(bql)

            bql = '''
            CREATE TEMP TABLE simres AS
                SIMULATE x, y FROM {}
                LIMIT {};
            '''.format(generator, args['n'])
            bdb.execute(bql)

            bql = 'SELECT * FROM simres;'
            with bdb.savepoint():
                c = bdb.execute(bql)
                simdata = np.array(c.fetchall())
                result['shapes'][shape]['simulated'] = simdata
            temp.close()

        # if DO_PLOT:
        #     plot(result)

    return result


def plot(result, filename=None):
    args = result['args']
    gs = gridspec.GridSpec(2, len(result['shapes']))
    plt.figure(tight_layout=True, facecolor='white', figsize=(10, 4))
    for i, (shape, data) in enumerate(result['shapes'].iteritems()):
        ax_org = plt.subplot(gs[0, i])
        ax_sim = plt.subplot(gs[1, i])

        xorg = data['original']
        xsim = data['simulated']

        ax_org.scatter(xorg[:, 0], xorg[:, 1], color='dodgerblue', alpha=.5)
        ax_sim.scatter(xsim[:, 0], xsim[:, 1], color='deeppink', alpha=.5)

        ax_sim.set_ylim(ax_org.get_ylim())
        ax_sim.set_xlim(ax_org.get_xlim())

        ax_org.set_yticklabels([])
        ax_org.set_xticklabels([])
        ax_sim.set_yticklabels([])
        ax_sim.set_xticklabels([])

        if i == 0:
            ax_org.set_ylabel('Original Data')
            ax_sim.set_ylabel('Recovered Data')
        ax_org.set_xlabel(shape)
    plt.suptitle('N=%i, n_models=%i, n_iter=%i' %
                 (args['n'], args['n_model'], args['n_iter']))

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
