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

import contextlib
import shutil
import tempfile
import os.path

from bayeslite import bayesdb_open
from bayeslite import bayesdb_register_metamodel
from bayeslite.metamodels.loom_metamodel import LoomMetamodel

PREDICT_RUNS = 100
X_MIN, Y_MIN = 0, 0
X_MAX, Y_MAX = 200, 100

# TODO fix fail when two tests are run with the same prefix
# currently low priority since bql users will use timestamps as prefix


@contextlib.contextmanager
def tempdir(prefix):
    path = tempfile.mkdtemp(prefix=prefix)
    try:
        yield
    finally:
        if os.path.isdir(path):
            shutil.rmtree(path)


def test_loom_one_numeric():
    """Simple test of the LoomMetamodel on a one variable table
    Only checks for errors from the Loom system."""

    with tempdir('bayeslite-loom') as loom_store_path:
        with bayesdb_open(':memory:') as bdb:
            bayesdb_register_metamodel(bdb,
                LoomMetamodel(loom_store_path=loom_store_path))
            bdb.sql_execute('create table t(x)')
            for x in xrange(10):
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


def test_stattypes():
    """Test of the LoomMetamodel on a table with all possible datatypes
    Only checks for errors from the Loom system."""

    with tempdir('bayeslite-loom') as loom_store_path:
        with bayesdb_open(':memory:') as bdb:
            bayesdb_register_metamodel(bdb,
                LoomMetamodel(loom_store_path=loom_store_path))
            bdb.sql_execute('create table t(u, co, b, ca, cy, nu, no)')
            for x in xrange(10):
                cat_dict = ['a', 'b', 'c']
                bdb.sql_execute('''insert into t(u, co, b, ca, cy, nu, no)
                    values (?, ?, ?, ?, ?, ?, ?)''',
                    (
                        cat_dict[bdb._prng.weakrandom_uniform(3)],
                        bdb._prng.weakrandom_uniform(200),
                        bdb._prng.weakrandom_uniform(2),
                        cat_dict[bdb._prng.weakrandom_uniform(3)],
                        bdb._prng.weakrandom_uniform(1000)/4.0,
                        bdb._prng.weakrandom_uniform(1000)/4.0 - 100.0,
                        bdb._prng.weakrandom_uniform(1000)/4.0
                    ))
            bdb.execute('''create population p for t(
                u unboundedcategorical;
                co counts;
                b boolean;
                ca categorical;
                cy cyclic;
                nu numerical;
                no nominal)
            ''')
            bdb.execute('create generator g for p using loom')
            bdb.execute('initialize 1 model for g')
            bdb.execute('analyze g for 1 iteration wait')
            bdb.execute('''estimate probability density of
                nu = 50, u="a" from p''').fetchall()
            bdb.execute('''simulate u, co, b, ca, cy, nu, no
                from p limit 1''').fetchall()
            bdb.execute('drop models from g')
            bdb.execute('drop generator g')
            bdb.execute('drop population p')
            bdb.execute('drop table t')


def test_conversion():
    """Test the workflow of:
    1. inference in loom
    2. conversion to cgpm,
    3. querying in cgpm."""

    with tempdir('bayeslite-loom') as loom_store_path:
        with bayesdb_open(':memory:') as bdb:
            bayesdb_register_metamodel(bdb,
                LoomMetamodel(loom_store_path=loom_store_path))
            bdb.sql_execute('create table t(x, y)')
            for x in xrange(10):
                bdb.sql_execute('insert into t(x, y) values(?, ?)',
                (x, bdb._prng.weakrandom_uniform(200),))
            bdb.execute('create population p for t(x numerical; y numerical)')
            bdb.execute('create generator lm for p using loom')
            bdb.execute('initialize 1 model for lm')
            bdb.execute('analyze lm for 1 iteration wait')
            bdb.execute('convert lm to cp using cgpm')
            bdb.execute('''estimate probability density of
                    x = 50 from p modeled by cp''').fetchall()

            # Kinds/Views and partitions are the same,
            # so predictive relevance should be the same
            loom_relevance = bdb.execute('''ESTIMATE PREDICTIVE RELEVANCE
                TO EXISTING ROWS (rowid = 1)
                IN THE CONTEXT OF "x"
                FROM p
                MODELED BY lm''').fetchall()
            cgpm_relevance = bdb.execute('''ESTIMATE PREDICTIVE RELEVANCE
                TO EXISTING ROWS (rowid = 1)
                IN THE CONTEXT OF "x"
                FROM p
                MODELED BY cp''').fetchall()
            print(loom_relevance)
            print(cgpm_relevance)
            assert loom_relevance[0] == cgpm_relevance[0]

            bdb.execute('drop models from cp')
            bdb.execute('drop generator cp')
            bdb.execute('drop models from lm')
            bdb.execute('drop generator lm')
            bdb.execute('drop population p')
            bdb.execute('drop table t')


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
    with tempdir('bayeslite-loom') as loom_store_path:
        with bayesdb_open(':memory:') as bdb:
            bayesdb_register_metamodel(bdb, LoomMetamodel(loom_store_path=loom_store_path))
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

            relevance = bdb.execute('''ESTIMATE PREDICTIVE RELEVANCE
                TO EXISTING ROWS (rowid = 1)
                IN THE CONTEXT OF "x"
                FROM p
                WHERE rowid = 1''').fetchall()
            assert relevance[0][0] == 1

            similarities = bdb.execute('''estimate similarity
                in the context of x from pairwise p limit 2''').fetchall()
            assert similarities[0][2] > 1
            assert abs(
                similarities[0][2]-similarities[1][2])/float(
                similarities[1][2]) < 0.005

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
