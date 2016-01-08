import bayeslite
import bdbcontrib

from bayeslite.metamodels import sdgpm

bdb = bayeslite.bayesdb_open()
bayeslite.bayesdb_read_csv_file(bdb, 'data', 'data.csv', header=True,
    create=True)

sdg = sdgpm.SdGpm()
bayeslite.bayesdb_register_metamodel(bdb, sdg)

bdbcontrib.query(bdb, '''
CREATE GENERATOR sd FOR data USING sdgpm(
    columns(
        x1 NUMERICAL, x2 NUMERICAL),
    program(
        '
        ; constants.
        (assume mu (list 10 -10 0))
        (assume var (list 1 2 1))
        ; global latents.
        (assume n_cluster 3)
        (assume r
            (tag (quote global) (quote r)
                (symmetric_dirichlet .5 n_cluster)))
        ; row latents.
        (assume k (mem (lambda (rowid)
            (tag (quote rowid) 'k
                (categorical r)))))
        ; columns
        (assume x1 (mem (lambda (rowid)
            (tag (quote rowid) (quote x1)
                (normal (lookup mu (k rowid)) (lookup var (k rowid)))))))
        (assume x2 (mem (lambda (rowid)
            (tag rowid (quote x2)
                (poisson (add 1 (k 1)))))))
        ; convenience
        (assume columns (list x1 x2))
        (assume get_cell (lambda (col rowid)
                ((lookup columns col) rowid)))
        '
        ));''')

bdbcontrib.query(bdb, 'INITIALIZE 10 MODELS FOR sd')
bdbcontrib.query(bdb, 'ANALYZE sd FOR 1 ITERATION WAIT')
bdbcontrib.query(bdb, 'SIMULATE x1 FROM sd LIMIT 2')
