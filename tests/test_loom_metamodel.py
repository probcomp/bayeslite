import pytest
import random
import time

import bayeslite.core as core

from bayeslite import BQLError
from bayeslite import bayesdb_open
from bayeslite import bayesdb_register_metamodel
from bayeslite.metamodels.loom_metamodel import LoomMetamodel

def test_loom():
    prng = random.Random(time.time())
    with bayesdb_open(':memory:') as bdb:
        bayesdb_register_metamodel(bdb, LoomMetamodel())
        bdb.sql_execute('create table t(x, y, z)')
        for x in xrange(1000):
            bdb.sql_execute('insert into t(x, y, z) values(?, ?, ?)',
                    (random.randrange(0, 200),
                        random.randrange(0, 100),
                        'a' if random.uniform(0, 1) > 0.5 else 'b'))
        bdb.execute('create population p for t(x numerical; y numerical; z categorical)')
        bdb.execute('create generator g for p using loom')
        bdb.execute('initialize 1 model for g')
        bdb.execute('analyze g for 1 iteration wait')
        bdb.execute('estimate similarity
            in the context of x from pairwise p limit 1').fetchall()
        bdb.execute('estimate probability density of x = 50 by p').fetchall()
        bdb.execute('estimate probability density of z = "a" by p').fetchall()
        print(bdb.execute('simulate x, y from p limit 10').fetchall())
        print(bdb.execute('estimate dependence probability
            from pairwise variables of p').fetchall())

