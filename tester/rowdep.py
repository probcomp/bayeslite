import bayeslite
from bdbcontrib import query

bdb = bayeslite.bayesdb_open()
bayeslite.bayesdb_read_csv_file(bdb, 'data', 'data.csv', header=True,
    create=True)

        # ROWS (3, 2) INDEPENDENT WITH RESPECT TO (x2),
        # ROWS (5, 1) INDEPENDENT WITH RESPECT TO (x1, x2),
query(bdb, '''
    CREATE GENERATOR data_cc FOR data USING crosscat(
        GUESS(*),
        ROWS (1, 2, 3) DEPENDENT WITH RESPECT TO (x1),
        id IGNORE,
        x1 NUMERICAL,
        x2 NUMERICAL,
        DEPENDENT (x1, x2),
        );''')

query(bdb, 'INITIALIZE 10 MODELS FOR data_cc')
query(bdb, 'ANALYZE data_cc FOR 10 ITERATION WAIT')
