# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2016, MIT Probabilistic Computing Project
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from bayeslite import bayesdb_open
from bayeslite import bayesdb_register_metamodel
from bayeslite.metamodels.loom_metamodel import LoomMetamodel

PREDICT_RUNS = 100
X_MIN, Y_MIN = 0, 0
X_MAX, Y_MAX = 200, 100


def test_loom_four_var():
    """Test Loom on a four variable table.
    Table consists of:
    * x - a random int between 0 and 200
    * y - a random int between 0 and 100
    * xx - just 2*x
    * z - a categorical variable that has an even
    chance of being 'a' or 'b'

    Queries run and tested include:
    estimate similarity, estimate probability density, simulate,
    estimate mutual information, estimate dependence probability,
    infer explicit predict
    """

    with bayesdb_open(':memory:') as bdb:
        bayesdb_register_metamodel(bdb, LoomMetamodel())
        bdb.sql_execute('create table t(x, xx, y, z)')
        bdb.sql_execute('insert into t(x, xx, y, z) values(100, 200, 50, "a")')
        bdb.sql_execute('insert into t(x, xx, y, z) values(100, 200, 50, "a")')
        for index in xrange(100):
            x = bdb._prng.weakrandom_uniform(X_MAX)
            bdb.sql_execute('insert into t(x, xx, y, z) values(?, ?, ?, ?)',
                    (x, x*2,
                        int(bdb._prng.weakrandom_uniform(Y_MAX)),
                        'a' if bdb._prng.weakrandom_uniform(2) == 1 else 'b'))

        bdb.execute('''create population p for t(x numerical; xx numerical;
                y numerical; z categorical)''')
        bdb.execute('create generator g for p using loom')
        bdb.execute('initialize 2 model for g')
        bdb.execute('analyze g for 1 iteration wait')

        similarities = bdb.execute('estimate similarity \
            in the context of x from pairwise p limit 2').fetchall()
        assert similarities[0][2] > 1
        assert similarities[0][2] == similarities[1][2]

        impossible_density = bdb.execute(
                'estimate probability density of x = %d by p'
                % (X_MAX*2.5)).fetchall()
        assert impossible_density[0][0] < 0.0001

        possible_density = bdb.execute(
                'estimate probability density of x = %d  by p' %
                ((X_MAX-X_MIN)/2)).fetchall()
        assert possible_density[0][0] > 0.001

        categorical_density = bdb.execute('''estimate probability density of
                z = "a" by p''').fetchall()
        assert abs(categorical_density[0][0]-.5) < 0.2

        mutual_info = bdb.execute('''estimate mutual information as mutinf
                from pairwise columns of p order by mutinf''').fetchall()
        _, a, b, c = zip(*mutual_info)
        mutual_info_dict = dict(zip(zip(a, b), c))
        assert mutual_info_dict[('x', 'y')] < mutual_info_dict[
                ('x', 'xx')] < mutual_info_dict[('x', 'x')]

        simulated_data = bdb.execute('simulate x, y from p limit %d' %
                (PREDICT_RUNS)).fetchall()
        xs, ys = zip(*simulated_data)
        assert abs((sum(xs)/len(xs)) - (X_MAX-X_MIN)/2) < (X_MAX-X_MIN)/5
        assert abs((sum(ys)/len(ys)) - (Y_MAX-Y_MIN)/2) < (Y_MAX-Y_MIN)/5

        assert sum([1 if (x < Y_MIN or x > X_MAX)
            else 0 for x in xs]) < .5*PREDICT_RUNS
        assert sum([1 if (y < Y_MIN or y > Y_MAX)
            else 0 for y in ys]) < .5*PREDICT_RUNS

        dependence = bdb.execute('''estimate dependence probability
            from pairwise variables of p''').fetchall()
        for (_, col1, col2, d_val) in dependence:
            if col1 == col2:
                assert d_val == 1
            elif col1 in ['xx', 'x'] and col2 in ['xx', 'x']:
                assert d_val > 0.99
            else:
                assert d_val == 0

        predict_confidence = bdb.execute(
                'infer explicit predict x confidence x_c FROM p').fetchall()
        predictions, confidences = zip(*predict_confidence)
        assert abs((sum(predictions)/len(predictions))
                - (X_MAX-X_MIN)/2) < (X_MAX-X_MIN)/5
        assert sum([1 if (p < X_MIN or p > X_MAX)
            else 0 for p in predictions]) < .5*PREDICT_RUNS
        assert all([c == 0 for c in confidences])


def test_loom_one_numeric():
    """Simple test of the LoomMetamodel on a one variable table
    Only checks for errors from the Loom system.
    """
    with bayesdb_open(':memory:') as bdb:
        bayesdb_register_metamodel(bdb, LoomMetamodel())
        bdb.sql_execute('create table t(x)')
        for x in xrange(100):
            bdb.sql_execute('insert into t(x) values(?)', (x,))
        bdb.execute('create population p for t(x numerical)')
        bdb.execute('create generator g for p using loom')
        bdb.execute('initialize 1 model for g')
        bdb.execute('analyze g for 1 iteration wait')
        bdb.execute('estimate probability density of x = 50 from p').fetchall()
        bdb.execute('simulate x from p limit 1').fetchall()
        bdb.execute('drop models from g')
        bdb.execute('drop generator g')
        bdb.execute('drop population p')
        bdb.execute('drop table t')
