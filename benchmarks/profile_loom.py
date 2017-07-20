import csv
import time

from itertools import chain

import matplotlib.pyplot as plt

from bayeslite import bayesdb_open
from bayeslite import bayesdb_register_metamodel
from bayeslite.metamodels.loom_metamodel import LoomMetamodel
from bayeslite.read_pandas import bayesdb_read_pandas_df
from iventure import utils_bql
from cgpm.utils.test import gen_data_table
from pandas import DataFrame

CSV_FILE = 'satellites.csv'

NUM_SAMPLES = 3
MAGNITUDE_RANGE = range(1, 6)
BASE = 10


def cgpm_sat_run(num_rows):
    with bayesdb_open(':memory:') as bdb:
        bdb.execute('create table big_t from \'%s\';' % (CSV_FILE))
        bdb.execute('''create table t as
            select * from big_t limit %d''' % (num_rows))
        utils_bql.nullify(bdb, 'sat', bdb.execute('SELECT "NaN"').fetchvalue())

        bdb.execute('''create population p
            for t with schema
            (guess stattypes for (*));
        ''')
        bdb.execute('create generator g for p with baseline crosscat')
        bdb.execute('initialize 1 model for g')

        start_analyze = time.time()
        bdb.execute('analyze g for 1 iteration wait')
        elapsed_analyze = time.time() - start_analyze

        start_pdf = time.time()
        bdb.execute('''estimate probability density of
                Country_of_Operator = "Denmark" from p''').fetchall()
        elapsed_pdf = time.time() - start_pdf

        start_sim = time.time()
        bdb.execute('simulate Country_of_Operator from p limit 5').fetchall()
        elapsed_sim = time.time() - start_sim

        bdb.execute('drop models from g')
        bdb.execute('drop generator g')
        bdb.execute('drop population p')
        bdb.execute('drop table t')
        bdb.execute('drop table big_t')

        return elapsed_analyze, elapsed_pdf, elapsed_sim


def loom_sat_run(num_rows):
    with bayesdb_open(':memory:') as bdb:
        bayesdb_register_metamodel(bdb, LoomMetamodel())

        bdb.execute('create table big_t from \'%s\';' % (CSV_FILE))
        bdb.execute('''create table t as
            select * from big_t limit %d''' % (num_rows))
        utils_bql.nullify(bdb, 'sat', bdb.execute('SELECT "NaN"').fetchvalue())

        bdb.execute('''create population p
            for t with schema
            (guess stattypes for (*);
                MODEL Operator_Owner, Contractor as unboundedcategorical);
        ''')
        bdb.execute('create generator g for p using loom')
        bdb.execute('initialize 1 model for g')

        start_analyze = time.time()
        bdb.execute('analyze g for 1 iteration wait')
        elapsed_analyze = time.time() - start_analyze

        start_pdf = time.time()
        bdb.execute('''estimate probability density of
                Country_of_Operator = "Denmark" from p''').fetchall()
        elapsed_pdf = time.time() - start_pdf

        start_sim = time.time()
        bdb.execute('simulate Country_of_Operator from p limit 5').fetchall()
        elapsed_sim = time.time() - start_sim

        bdb.execute('drop models from g')
        bdb.execute('drop generator g')
        bdb.execute('drop population p')
        bdb.execute('drop table t')
        bdb.execute('drop table big_t')

        return elapsed_analyze, elapsed_pdf, elapsed_sim


def sat_run():
    cgpm_data_analyze, cgpm_data_pdf, cgpm_data_sim = profile_run(cgpm_sat_run)

    csv_write(cgpm_data_analyze, 'cgpm-analyze')
    csv_write(cgpm_data_pdf, 'cgpm-pdf')
    csv_write(cgpm_data_sim, 'cgpm-sim')

    generate_fig(cgpm_data_analyze,
        'cgpm-analyze-sat',
        'CGPM Single Model Analysis Runtime on the Satellite Dataset',
        label1='cgpm',
        ylabel='Analysis Time (seconds)')
    generate_fig(cgpm_data_pdf,
        'cgpm-pdf-sat',
        'CGPM Single Model Logpdf Query Runtime on the Satellite Dataset',
        label1='cgpm',
        ylabel='Logpdf Query Time (seconds)')
    generate_fig(cgpm_data_sim,
        'cgpm-sim-sat',
        'CGPM Single Model Simulate Query Runtime on the Satellite Dataset',
        label1='cgpm',
        ylabel='Simulate Query Time (seconds)')

    loom_data_analyze, loom_data_pdf, loom_data_sim = profile_run(loom_sat_run)

    csv_write(loom_data_analyze, 'loom-analyze')
    csv_write(loom_data_pdf, 'loom-pdf')
    csv_write(loom_data_sim, 'loom-sim')

    generate_fig(loom_data_analyze,
        'loom-analyze-sat',
        '''CGPM Single Model Analysis Runtime
            \non the Satellite Dataset''',
        label1='loom',
        ylabel='Analysis Time (seconds)')
    generate_fig(loom_data_pdf,
        'loom-pdf-sat',
        '''Loom Single Model Logpdf Query Runtime
            \non the Satellite Dataset''',
        label1='loom',
        ylabel='Logpdf Query Time (seconds)')
    generate_fig(loom_data_sim,
        'loom-sim-sat',
        '''Loom Single Model Simulate Query Runtime
            \non the Satellite Dataset''',
        label1='loom',
        ylabel='Simulate Query Time (seconds)')

    generate_fig(
        loom_data_analyze,
        'all-analyze-sat',
        '''Loom vs CGPM: Single Model Analysis Runtime
            \non the Satellite Dataset''',
        label1='loom analysis',
        data2=cgpm_data_analyze,
        label2='cgpm analysis',
        ylabel='Analysis Time (seconds)')
    generate_fig(
        loom_data_pdf,
        'all-pdf-sat',
        '''Loom vs CGPM: Single Model Logpdf Query Runtime
            \non the Satellite Dataset''',
        label1='loom',
        data2=cgpm_data_pdf,
        label2='cgpm',
        ylabel='Logpdf Query Time (seconds)')
    generate_fig(
        loom_data_sim,
        'all-sim-sat',
        '''Loom vs CGPM: Single Model Simulate Query Runtime
            \non the Satellite Dataset''',
        label1='loom',
        data2=cgpm_data_sim,
        label2='cgpm',
        ylabel='Simulate Query Time (seconds)')
    plt.show()


def init_ovoc(num_rows, bdb):
    data = gen_data_table(
        num_rows,
        [1],
        [[1]],
        ['lognormal']*4,
        [[]]*4,
        [0]*4)
    data = list(data[0])
    p_data = {str(colno): data[colno]
            for colno in range(len(data))}
    frame = DataFrame(p_data)
    bayesdb_read_pandas_df(bdb, 't', frame, create=True)


def ovoc_run(num_rows,
        metamodel_phrase, metamodel_to_register=None):
    with bayesdb_open(':memory:') as bdb:
        if metamodel_to_register is not None:
            bayesdb_register_metamodel(bdb,
                    metamodel_to_register)

        init_ovoc(num_rows, bdb)

        bdb.execute('''create population p
            for t with schema
            (MODEL "0", "1", "2", "3" AS numerical);
        ''')
        bdb.execute('create generator g for p %s' % (metamodel_phrase))
        bdb.execute('initialize 1 model for g')

        start_analyze = time.time()
        bdb.execute('analyze g for 1 iteration wait')
        elapsed_analyze = time.time() - start_analyze

        start_sim = time.time()
        bdb.execute('simulate "1" from p limit 5').fetchall()
        elapsed_sim = time.time() - start_sim

        start_pdf = time.time()
        """bdb.execute('''estimate probability density of
                "0" = 1 from p''').fetchall()
        """
        elapsed_pdf = time.time() - start_pdf

        bdb.execute('drop models from g')
        bdb.execute('drop generator g')
        bdb.execute('drop population p')
        bdb.execute('drop table t')

        return elapsed_analyze, elapsed_pdf, elapsed_sim


def one_view_one_cluster():
    loom_data_analyze, loom_data_pdf, loom_data_sim = profile_run(
            ovoc_run, ['using loom', LoomMetamodel()])

    csv_write(loom_data_analyze, 'loom-analyze')
    csv_write(loom_data_pdf, 'loom-pdf')
    csv_write(loom_data_sim, 'loom-sim')

    generate_fig(loom_data_analyze,
        'loom-analyze-1v1c',
        '''Loom Single Model Analysis Runtime
            \non a One View One Cluster Dataset''',
        label1='loom',
        ylabel='Analysis Time (seconds)')
    generate_fig(loom_data_pdf,
        'loom-pdf-1v1c',
        '''Loom Single Model Logpdf Query Runtime
            \non a One View One Cluster Dataset''',
        label1='loom',
        ylabel='Logpdf Query Time (seconds)')
    generate_fig(loom_data_sim,
        'loom-sim-1v1c',
        '''Loom Single Model Simulate Query Runtime
            \non a One View One Cluster Dataset''',
        label1='loom',
        ylabel='Simulate Query Time (seconds)')

    cgpm_data_analyze, cgpm_data_pdf, cgpm_data_sim = profile_run(
            ovoc_run, ['with baseline crosscat'])

    csv_write(cgpm_data_analyze, 'cgpm-1v1c-analyze')
    csv_write(cgpm_data_pdf, 'cgpm-1v1c-pdf')
    csv_write(cgpm_data_sim, 'cgpm-1v1c-sim')

    generate_fig(cgpm_data_analyze,
        'cgpm-analyze-1v1c',
        '''CGPM Single Model Analysis Runtime
            \non a One View One Cluster Dataset''',
        label1='cgpm',
        ylabel='Analysis Time (seconds)')
    generate_fig(cgpm_data_pdf,
        'cgpm-pdf-1v1c',
        '''CGPM Single Model Logpdf Query Runtime
            \non a One View One Cluster Dataset''',
        label1='cgpm',
        ylabel='Logpdf Query Time (seconds)')
    generate_fig(cgpm_data_sim,
        'cgpm-sim-1v1c',
        '''CGPM Single Model Simulate Query Runtime
            \non a One View One Cluster Dataset''',
        label1='cgpm',
        ylabel='Simulate Query Time (seconds)')

    generate_fig(
        loom_data_analyze,
        'all-analyze-1v1c',
        '''Loom vs CGPM: Single Model Analysis Runtime
            \non a One view One Cluster Dataset''',
        label1='loom analysis',
        data2=cgpm_data_analyze,
        label2='cgpm analysis',
        ylabel='Analysis Time (seconds)')
    generate_fig(
        loom_data_pdf,
        'all-pdf-1v1c',
        '''Loom vs CGPM: Single Model Logpdf Query Runtime
            \non a One view One Cluster Dataset''',
        label1='loom',
        data2=cgpm_data_pdf,
        label2='cgpm',
        ylabel='Logpdf Query Time (seconds)')
    generate_fig(
        loom_data_sim,
        'all-sim-1v1c',
        '''Loom vs CGPM: Single Model Simulate Query Runtime
            \non a One view One Cluster Dataset''',
        label1='loom',
        data2=cgpm_data_sim,
        label2='cgpm',
        ylabel='Simulate Query Time (seconds)')
    plt.show()


def profile_run(run_function, run_args=[]):
    analyze_data, pdf_data, sim_data = {}, {}, {}
    for magnitude in MAGNITUDE_RANGE:
        num_rows = BASE**magnitude
        analyze_data[num_rows] = []
        pdf_data[num_rows] = []
        sim_data[num_rows] = []
        for n in range(NUM_SAMPLES):
            print('Running on %d rows; Trial %d' % (num_rows, n))
            analyze_time, pdf_time, sim_time = run_function(
                    num_rows, *run_args)

            analyze_data[num_rows].append(analyze_time)
            pdf_data[num_rows].append(pdf_time)
            sim_data[num_rows].append(sim_time)
            print('Analyze %f, LogPDF %f, Simulate %s' % (
                analyze_time, pdf_time, sim_time))

    print(analyze_data)
    print([sum(analyze_data)/float(len(analyze_data))
        for (num_rows, value_array) in analyze_data.iteritems()])
    print(pdf_data)
    print([sum(pdf_data)/float(len(pdf_data))
        for (num_rows, value_array) in pdf_data.iteritems()])
    print(sim_data)
    print([sum(sim_data)/float(len(sim_data))
        for (num_rows, value_array) in sim_data.iteritems()])
    return analyze_data, pdf_data, sim_data


def generate_fig(data1, file_postfix, title,
        label1='first', data2=None, label2='second',
        xlabel='Number of Training Rows',
        ylabel='Time (seconds)'):
    x_analyze_1 = list(chain(*([[num_rows]*len(data1[num_rows])
        for num_rows in data1.keys()])))
    y_analyze_1 = list(chain(*data1.values()))
    print(x_analyze_1)
    print(y_analyze_1)

    plt.figure()
    plt.scatter(x_analyze_1, y_analyze_1, c='b', label=label1)
    if data2 is not None:
        x_analyze_2 = list(chain(*([[num_rows]*len(data2[num_rows])
            for num_rows in data2.keys()])))
        y_analyze_2 = list(chain(*data2.values()))
        plt.scatter(x_analyze_2, y_analyze_2, c='r', label=label2)

    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.legend(loc='upper left')
    plt.savefig('%s-%s.png' % (time.time(), file_postfix))


def csv_write(data, postfix):
    with open(('%s-%s.csv' % (time.time(), postfix)), 'wb') as csvfile:
        writer = csv.writer(csvfile,
                delimiter=',')
        writer.writerow(data.keys())
        for row in zip(*data.values()):
            writer.writerow(row)


def main():
    one_view_one_cluster()


if __name__ == "__main__":
        main()
