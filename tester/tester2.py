import numpy as np
import bayeslite
import matplotlib.pyplot as plt
from bayeslite.metamodels import sdgpm
from bdbcontrib import query

import time
bdb = bayeslite.bayesdb_open(str(time.time()).split('.')[0] + '.bdb')
bayeslite.bayesdb_read_csv_file(bdb, 'data', 'arcsin.csv', header=True,
    create=True)

sdg = sdgpm.SdGpm()
bayeslite.bayesdb_register_metamodel(bdb, sdg)

query(bdb, '''
    CREATE GENERATOR sd FOR data USING sdgpm(
        columns(
            u NUMERICAL, x NUMERICAL),
        program(
            '
            (assume noise 0.05)
            ; rows
        (assume u (mem (lambda (rowid)
            (tag rowid (quote u)
                (uniform_continuous -3.14 3.14)))))
            (assume x (mem (lambda (rowid)
                (tag rowid (quote x)
                (normal (sin (u rowid)) noise)))))
            ; conventions
            (assume columns (list u x))
            '
    ));''')
query(bdb, 'INITIALIZE 10 MODELS FOR sd')

pdfs = []
for x in np.linspace(0,1,100):
    r = (query(bdb,
            'ESTIMATE PROBABILITY OF x = {} FROM COLUMNS OF sd'.format(x)))
    pdfs.extend((r.irow(0)[0], r.irow(1)[0]))

# query(bdb, 'CREATE TABLE sd_joint AS SIMULATE u, x FROM sd LIMIT 100')
# query(bdb, '''
#     CREATE TABLE sd_conditional AS
#         SIMULATE u FROM sd GIVEN x = 0 LIMIT 100''')

# query(bdb, '''
#     CREATE GENERATOR cc FOR sd_joint USING crosscat(
#         x NUMERICAL, u NUMERICAL);''')
# query(bdb, 'INITIALIZE 10 MODELS FOR cc;')
# query(bdb, 'ANALYZE cc FOR 10 ITERATIONS WAIT;')
# query(bdb, 'CREATE TABLE cc_joint AS SIMULATE u, x FROM cc LIMIT 100')
# query(bdb, '''
#     CREATE TABLE cc_conditional AS
#         SIMULATE u FROM cc GIVEN x = 0 LIMIT 100''')

# sd_joint = query(bdb, 'SELECT * FROM sd_joint')
# cc_joint = query(bdb, 'SELECT * FROM cc_joint')

# sd_conditional = query(bdb, 'SELECT * FROM sd_conditional')
# cc_conditional = query(bdb, 'SELECT * FROM cc_conditional')

# fig, ax = plt.subplots()
# ax.scatter(sd_joint['u'], sd_joint['x'], color='green',
#     label='Data Generator (u,x)')
# ax.scatter(sd_conditional['u'], [-0.1] * 100, color='red',
#     label='Data Generator (u|x=0)')

# ax.scatter(cc_joint['u'], cc_joint['x'], color='blue',
#     label='crosscat (u,x)')
# ax.scatter(cc_conditional['u'], [0.1] * 100, color='purple',
#     label='crosscat (u|x=0)')

# ax.set_xlabel('u')
# ax.set_xlabel('x')
# ax.legend()
