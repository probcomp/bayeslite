import pytest
import random
import time

import bayeslite.core as core

from bayeslite import BQLError
from bayeslite import bayesdb_open
from bayeslite import bayesdb_register_metamodel
from bayeslite.metamodels.truell import TruellMetamodel

def test_truell():
    prng = random.Random(time.time())
    with bayesdb_open(':memory:') as bdb:
        bayesdb_register_metamodel(bdb, TruellMetamodel())
        bdb.sql_execute('create table t(x, y)')
        for x in xrange(1000):
            bdb.sql_execute('insert into t(x, y) values(?, ?)',
                    (random.randrange(0, 200), random.randrange(0, 100),))
        bdb.execute('create population p for t(x numerical; y numerical)')
        bdb.execute('create generator g for p using truell')
        bdb.execute('initialize 1 model for g')
        bdb.execute('analyze g for 1 iteration wait')
        bdb.execute('estimate probability density of x = 50 and y = 50 from p').fetchall()
        bdb.execute('simulate x, y from p limit 10').fetchall()

