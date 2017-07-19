import csv
import time

from itertools import chain

import matplotlib.pyplot as plt

from bayeslite import bayesdb_open
from bayeslite import bayesdb_register_metamodel
from bayeslite.metamodels.loom_metamodel import LoomMetamodel
from bayeslite.metamodels.cgpm_metamodel import CGPM_Metamodel
from iventure import utils_bql

CSV_FILE = 'satellites.csv'


def cgpm_run(num_rows):
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


def loom_run(num_rows):
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


NUM_SAMPLES = 3
MAGNITUDE_RANGE = range(1, 11)
BASE = 2

def profile_run(run_function):
    analyze_data, pdf_data, sim_data = {}, {}, {}
    for magnitude in MAGNITUDE_RANGE:
        num_rows = BASE**magnitude
        analyze_data[num_rows] = []
        pdf_data[num_rows] = []
        sim_data[num_rows] = []
        for n in range(NUM_SAMPLES):
            print('Running on %d rows; Trial %d' % (num_rows, n))
            analyze_time, pdf_time, sim_time = run_function(num_rows)

            analyze_data[num_rows].append(analyze_time)
            pdf_data[num_rows].append(pdf_time)
            sim_data[num_rows].append(sim_time)
            print('Analyze %f, LogPDF %f, Simulate %s' % (analyze_time, pdf_time, sim_time))

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
    #plt.show()

def csv_write(data, postfix):
    with open(('%s-%s.csv' % (time.time(), postfix)), 'wb') as csvfile:
        writer = csv.writer(csvfile,
                delimiter=',')
        writer.writerow(data.keys())
        for row in zip(*data.values()):
            writer.writerow(row)

def main():
    cgpm_data_analyze, cgpm_data_pdf, cgpm_data_sim = profile_run(cgpm_run)

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

    loom_data_analyze, loom_data_pdf, loom_data_sim = profile_run(loom_run)

    csv_write(loom_data_analyze, 'loom-analyze')
    csv_write(loom_data_pdf, 'loom-pdf')
    csv_write(loom_data_sim, 'loom-sim')

    generate_fig(loom_data_analyze,
        'loom-analyze-sat',
        'CGPM Single Model Analysis Runtime on the Satellite Dataset',
        label1='loom',
        ylabel='Analysis Time (seconds)')
    generate_fig(loom_data_pdf,
        'loom-pdf-sat',
        'Loom Single Model Logpdf Query Runtime on the Satellite Dataset',
        label1='loom',
        ylabel='Logpdf Query Time (seconds)')
    generate_fig(loom_data_sim,
        'loom-sim-sat',
        'Loom Single Model Simulate Query Runtime on the Satellite Dataset',
        label1='loom',
        ylabel='Simulate Query Time (seconds)')

    generate_fig(
        loom_data_analyze,
        'all-analyze-sat',
        'Loom vs CGPM: Single Model Analysis Runtime on the Satellite Dataset',
        label1='loom analysis',
        data2=cgpm_data_analyze,
        label2='cgpm analysis',
        ylabel='Analysis Time (seconds)')
    generate_fig(
        loom_data_pdf,
        'all-pdf-sat',
        'Loom vs CGPM: Single Model Logpdf Query Runtime on the Satellite Dataset',
        label1='loom',
        data2=cgpm_data_pdf,
        label2='cgpm',
        ylabel='Logpdf Query Time (seconds)')
    generate_fig(
        loom_data_sim,
        'all-sim-sat',
        'Loom vs CGPM: Single Model Simulate Query Runtime on the Satellite Dataset',
        label1='loom',
        data2=cgpm_data_sim,
        label2='cgpm',
        ylabel='Simulate Query Time (seconds)')
    plt.show()

if __name__ == "__main__":
        main()
