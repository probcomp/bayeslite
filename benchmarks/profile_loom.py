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
        CGPM_Metamodel({})

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

        start_query = time.time()
        bdb.execute('''estimate probability density of
                Country_of_Operator = 'USA' from p''').fetchall()
        elapsed_query = time.time() - start_query

        bdb.execute('drop models from g')
        bdb.execute('drop generator g')
        bdb.execute('drop population p')
        bdb.execute('drop table t')
        bdb.execute('drop table big_t')

        return elapsed_analyze, elapsed_query


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

        start_query = time.time()
        bdb.execute('''estimate probability density of
                Country_of_Operator = "USA" from p''').fetchall()
        elapsed_query = time.time() - start_query

        bdb.execute('drop models from g')
        bdb.execute('drop generator g')
        bdb.execute('drop population p')
        bdb.execute('drop table t')
        bdb.execute('drop table big_t')

        return elapsed_analyze, elapsed_query


NUM_SAMPLES = 5
MAGNITUDE_RANGE = range(1, 4)

def profile_run(run_function):
    analyze_data, query_data = {}, {}
    for magnitude in MAGNITUDE_RANGE:
        num_rows = 10**magnitude
        analyze_data[num_rows] = []
        query_data[num_rows] = []
        for n in range(NUM_SAMPLES):
            print('Running on %d rows; Trial %d' % (num_rows, n))
            analyze_time, query_time = run_function(num_rows)

            analyze_data[num_rows].append(analyze_time)
            query_data[num_rows].append(query_time)
            print('Analyze %f, Query %f' % (analyze_time, query_time))

    print(analyze_data)
    print([sum(analyze_data)/float(len(analyze_data))
        for (num_rows, value_array) in analyze_data.iteritems()])
    print(query_data)
    print([sum(query_data)/float(len(query_data))
        for (num_rows, value_array) in query_data.iteritems()])
    return analyze_data, query_data


def generate_fig(data1,
        file_postfix, label1='first', data2=None, label2='second'):
    x_analyze_1 = list(chain(*([[num_rows]*len(data1[num_rows])
        for num_rows in data1.keys()])))
    y_analyze_1 = list(chain(*data1.values()))

    plt.figure()
    plt.scatter(x_analyze_1, y_analyze_1, c='b', label=label1)
    if data2 is not None:
        x_analyze_2 = list(chain(*([[num_rows]*len(data2[num_rows])
            for num_rows in data2.keys()])))
        y_analyze_2 = list(chain(*data2.values()))
        plt.scatter(x_analyze_2, y_analyze_2, c='r', label=label2)

    plt.xlabel('Number of rows')
    plt.ylabel('Time (seconds)')
    plt.savefig('%s-%s.png' % (time.time(), file_postfix))
    plt.show()


def main():
    loom_data_analyze, loom_data_query = profile_run(loom_run)
    generate_fig(loom_data_analyze, 'loom-analyze-sat', label1='loom analysis')
    generate_fig(loom_data_query, 'loom-query-sat', label1='loom querying')

    cgpm_data_analyze, cgpm_data_query = profile_run(cgpm_run)
    generate_fig(cgpm_data_analyze, 'cgpm-analyze-sat', label1='cgpm analysis')
    generate_fig(cgpm_data_query, 'cgpm-query-sat', label1='cgpm querying')

    generate_fig(
        loom_data_analyze,
        'all-analyze-sat',
        label1='loom analysis',
        data2=cgpm_data_analyze,
        label2='cgpm analysis'
    )
    generate_fig(
        loom_data_query,
        'all-query-sat',
        label1='loom querying',
        data2=cgpm_data_query,
        label2='cgpm querying'
    )

if __name__ == "__main__":
        main()
