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

import pytest

import bayeslite.core as core

from bayeslite import BQLError
from bayeslite import bayesdb_open
from bayeslite import bayesdb_register_metamodel
from bayeslite.metamodels.nig_normal import NIGNormalMetamodel

def test_nig_normal_smoke():
    with bayesdb_open(':memory:') as bdb:
        bayesdb_register_metamodel(bdb, NIGNormalMetamodel())
        bdb.sql_execute('create table t(x)')
        for x in xrange(100):
            bdb.sql_execute('insert into t(x) values(?)', (x,))
        bdb.execute('create population p for t(x numerical)')
        bdb.execute('create generator g for p using nig_normal')
        bdb.execute('initialize 1 model for g')
        bdb.execute('analyze g for 1 iteration wait')
        bdb.execute('estimate probability of x = 50 from p').fetchall()
        bdb.execute('simulate x from p limit 1').fetchall()
        bdb.execute('drop models from g')
        bdb.execute('drop generator g')
        bdb.execute('drop population p')
        bdb.execute('drop table t')

def test_nig_normal_latent_numbering():
    with bayesdb_open(':memory:') as bdb:
        bayesdb_register_metamodel(bdb, NIGNormalMetamodel())
        bdb.sql_execute('create table t(id integer primary key, x, y)')
        for x in xrange(100):
            bdb.sql_execute('insert into t(x, y) values(?, ?)', (x, x*x - 100))
        bdb.execute('''
            create population p for t(id ignore; model x,y as numerical)
        ''')
        assert core.bayesdb_has_population(bdb, 'p')
        pid = core.bayesdb_get_population(bdb, 'p')
        assert core.bayesdb_variable_numbers(bdb, pid, None) == [1, 2]

        bdb.execute('create generator g0 for p using nig_normal')
        bdb.execute('''
            create generator g1 for p using nig_normal(xe deviation(x))
        ''')

        assert core.bayesdb_has_generator(bdb, pid, 'g0')
        g0 = core.bayesdb_get_generator(bdb, pid, 'g0')
        assert core.bayesdb_has_generator(bdb, pid, 'g1')
        g1 = core.bayesdb_get_generator(bdb, pid, 'g1')
        assert core.bayesdb_variable_numbers(bdb, pid, None) == [1, 2]
        assert core.bayesdb_variable_numbers(bdb, pid, g0) == [1, 2]
        assert core.bayesdb_generator_column_numbers(bdb, g0) == [1, 2]
        assert core.bayesdb_variable_numbers(bdb, pid, g1) == [-1, 1, 2]
        assert core.bayesdb_generator_column_numbers(bdb, g1) == [-1, 1, 2]

def test_nig_normal_latent_smoke():
    with bayesdb_open(':memory:') as bdb:
        bayesdb_register_metamodel(bdb, NIGNormalMetamodel())
        bdb.sql_execute('create table t(x)')
        for x in xrange(100):
            bdb.sql_execute('insert into t(x) values(?)', (x,))
        bdb.execute('create population p for t(x numerical)')
        bdb.execute('create generator g0 for p using nig_normal')
        bdb.execute('''
            create generator g1 for p using nig_normal(xe deviation(x))
        ''')
        bdb.execute('initialize 1 model for g0')
        bdb.execute('analyze g0 for 1 iteration wait')
        bdb.execute('initialize 1 model for g1')
        bdb.execute('analyze g1 for 1 iteration wait')

        # PROBABILITY OF x = v
        bdb.execute('estimate probability of x = 50 within p').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('estimate probability of xe = 1 within p').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('''
                estimate probability of xe = 1 within p modelled by g0
            ''').fetchall()
        bdb.execute('''
            estimate probability of xe = 1 within p modelled by g1
        ''').fetchall()

        # PREDICTIVE PROBABILITY OF x
        bdb.execute('estimate predictive probability of x from p').fetchall()
        with pytest.raises(BQLError):
            bdb.execute(
                'estimate predictive probability of xe from p').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('''
                estimate predictive probability of xe from p modelled by g0
            ''').fetchall()
        for r, p_xe in bdb.execute('''
            estimate rowid, predictive probability of xe from p modelled by g1
        '''):
            assert p_xe is None, 'rowid %r p(xe) %r' % (r, p_xe)

        # INFER/PREDICT
        bdb.execute(
            'INFER EXPLICIT PREDICT x CONFIDENCE x_c FROM p').fetchall()
        with pytest.raises(BQLError):
            bdb.execute(
                'INFER EXPLICIT PREDICT xe CONFIDENCE xe_c FROM p').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('''
                INFER EXPLICIT PREDICT xe CONFIDENCE xe_c FROM p
                    MODELLED BY g0
            ''').fetchall()
        bdb.execute('''
            INFER EXPLICIT PREDICT xe CONFIDENCE xe_c FROM p
                MODELLED BY g1
        ''').fetchall()

        # SIMULATE x
        bdb.execute('simulate x from p limit 1').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('simulate x, xe from p limit 1').fetchall()
        with pytest.raises(BQLError):
            bdb.execute(
                'simulate x, xe from p modelled by g0 limit 1').fetchall()
        bdb.execute('simulate x, xe from p modelled by g1 limit 1').fetchall()

        assert 100 == len(bdb.execute('''
            estimate similarity with respect to x from pairwise p limit 100
        ''').fetchall())
        assert 1 == len(bdb.execute('''
            estimate similarity from pairwise p modelled by g0 limit 1
        ''').fetchall())
        assert 1 == len(bdb.execute('''
            estimate similarity with respect to (x)
                from pairwise p modelled by g0 limit 1
        ''').fetchall())
                # No such column xe in g0.
        with pytest.raises(BQLError):
            assert 1 == len(bdb.execute('''
                estimate similarity with respect to (xe)
                    from pairwise p modelled by g0 limit 1
            ''').fetchall())
        # Column xe exists in g1.
        assert 1 == len(bdb.execute('''
            estimate similarity with respect to xe
                from pairwise p modelled by g1 limit 1
        ''').fetchall())
        assert 1 == len(bdb.execute('''
            estimate similarity with respect to (xe)
                from pairwise p modelled by g1 limit 1
        ''').fetchall())
        # Similarity with respect to multiple columns.
        with pytest.raises(BQLError):
            assert 1 == len(bdb.execute('''
                estimate similarity with respect to (x, xe)
                    from pairwise p modelled by g1 limit 1
            ''').fetchall())

        bdb.execute('drop models from g0')
        bdb.execute('drop generator g0')
        bdb.execute('drop models from g1')
        bdb.execute('drop generator g1')
        bdb.execute('drop population p')
        bdb.execute('drop table t')

def test_nig_normal_latent_conditional_smoke():
    with bayesdb_open(':memory:') as bdb:
        bayesdb_register_metamodel(bdb, NIGNormalMetamodel())
        bdb.sql_execute('create table t(x)')
        for x in xrange(100):
            bdb.sql_execute('insert into t(x) values(?)', (x,))
        bdb.execute('create population p for t(x numerical)')
        bdb.execute('create generator g0 for p using nig_normal')
        bdb.execute('''
            create generator g1 for p using nig_normal(xe deviation(x))
        ''')
        bdb.execute('initialize 1 model for g0')
        bdb.execute('analyze g0 for 1 iteration wait')
        bdb.execute('initialize 1 model for g1')
        bdb.execute('analyze g1 for 1 iteration wait')

        # observed given observed
        bdb.execute('''
            estimate probability of x = 50 given (x = 50) within p
        ''').fetchall()
        bdb.execute('''
            estimate probability of x = 50 given (x = 50) within p
                modelled by g0
        ''').fetchall()
        bdb.execute('''
            estimate probability of x = 50 given (x = 50) within p
                modelled by g1
        ''').fetchall()

        # observed given latent
        with pytest.raises(BQLError):
            bdb.execute('''
                estimate probability of x = 50 given (xe = 50) within p
            ''').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('''
                estimate probability of x = 50 given (xe = 50) within p
                    modelled by g0
            ''').fetchall()
        bdb.execute('''
            estimate probability of x = 50 given (xe = 50) within p
                modelled by g1
        ''').fetchall()

        # latent given observed
        with pytest.raises(BQLError):
            bdb.execute('''
                estimate probability of xe = 50 given (x = 50) within p
            ''').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('''
                estimate probability of xe = 50 given (x = 50) within p
                    modelled by g0
            ''').fetchall()
        bdb.execute('''
            estimate probability of xe = 50 given (x = 50) within p
                modelled by g1
        ''').fetchall()

        bdb.execute('drop models from g0')
        bdb.execute('drop generator g0')
        bdb.execute('drop models from g1')
        bdb.execute('drop generator g1')
        bdb.execute('drop population p')
        bdb.execute('drop table t')

def test_nig_normal_latent_2var_smoke():
    with bayesdb_open(':memory:') as bdb:
        bayesdb_register_metamodel(bdb, NIGNormalMetamodel())
        bdb.sql_execute('create table t(x, y)')
        for x in xrange(100):
            bdb.sql_execute('insert into t(x, y) values(?, ?)',
                (x, x*x - 100))
        bdb.execute('create population p for t(x numerical; y numerical)')

        # CORRELATION, CORRELATION PVALUE, without generators.
        assert 4 == len(bdb.execute('''
            estimate correlation, correlation pvalue
                from pairwise variables of p
        ''').fetchall())

        bdb.execute('create generator g0 for p using nig_normal')
        bdb.execute('''
            create generator g1 for p using nig_normal(xe deviation(x))
        ''')
        bdb.execute('initialize 1 model for g0')
        bdb.execute('analyze g0 for 1 iteration wait')
        bdb.execute('initialize 1 model for g1')
        bdb.execute('analyze g1 for 1 iteration wait')

        # CORRELATION, CORRELATION PVALUE, with generators.
        assert 4 == len(bdb.execute('''
            estimate correlation, correlation pvalue
                from pairwise variables of p
        ''').fetchall())
        assert 4 == len(bdb.execute('''
            estimate correlation, correlation pvalue
                from pairwise variables of p modelled by g0
        ''').fetchall())
        with pytest.raises(BQLError):
            assert 4 == len(bdb.execute('''
                estimate correlation, correlation pvalue
                    from pairwise variables of p modelled by g1
            ''').fetchall())

        # DEPENDENCE PROBABILITY, MUTUAL INFORMATION
        assert 4 == len(bdb.execute('''
            estimate dependence probability, mutual information
                from pairwise variables of p
        ''').fetchall())
        assert 4 == len(bdb.execute('''
            estimate dependence probability, mutual information
                from pairwise variables of p modelled by g0
        ''').fetchall())
        assert 9 == len(bdb.execute('''
            estimate dependence probability, mutual information
                from pairwise variables of p modelled by g1
        ''').fetchall())

        # SIMULATE LATENT VARIABLE
        assert 10 == len(bdb.execute('''
            simulate xe from p modeled by g1 limit 10;
        ''').fetchall())
        assert 10 == len(bdb.execute('''
            simulate y, xe from p modeled by g1 limit 10;
        ''').fetchall())
        # Cannot simulate the latent xe from the population p.
        with pytest.raises(BQLError):
            assert 10 == len(bdb.execute('''
                simulate xe from p limit 10;
            ''').fetchall())
        # Cannot simulate the latent xe from the generator g0.
        with pytest.raises(BQLError):
            assert 10 == len(bdb.execute('''
                simulate xe from p modeled by g0 limit 10;
            ''').fetchall())

        bdb.execute('drop models from g0')
        bdb.execute('drop generator g0')
        bdb.execute('drop models from g1')
        bdb.execute('drop generator g1')
        bdb.execute('drop population p')
        bdb.execute('drop table t')

def test_nig_normal_latent_2var_conditional_smoke():
    with bayesdb_open(':memory:') as bdb:
        bayesdb_register_metamodel(bdb, NIGNormalMetamodel())
        bdb.sql_execute('create table t(x, y)')
        for x in xrange(100):
            bdb.sql_execute('insert into t(x, y) values(?, ?)',
                (x, x*x - 100))
        bdb.execute('create population p for t(x numerical; y numerical)')

        # CORRELATION, CORRELATION PVALUE, without generators.
        assert 4 == len(bdb.execute('''
            estimate correlation, correlation pvalue
                from pairwise variables of p
        ''').fetchall())

        bdb.execute('create generator g0 for p using nig_normal')
        bdb.execute('''
            create generator g1 for p using nig_normal(xe deviation(x))
        ''')
        bdb.execute('initialize 1 model for g0')
        bdb.execute('analyze g0 for 1 iteration wait')
        bdb.execute('initialize 1 model for g1')
        bdb.execute('analyze g1 for 1 iteration wait')

        # observed given other observed
        bdb.execute('''
            estimate probability of x = 50 given (y = 49) within p
        ''').fetchall()
        bdb.execute('''
            estimate probability of x = 50 given (y = 49) within p
                modelled by g0
        ''').fetchall()
        bdb.execute('''
            estimate probability of x = 50 given (y = 49) within p
                modelled by g1
        ''').fetchall()
        bdb.execute('simulate x from p given y = 49 limit 1').fetchall()
        bdb.execute('''
            simulate x from p modelled by g0 given y = 49 limit 1
        ''').fetchall()
        bdb.execute('''
            simulate x from p modelled by g1 given y = 49 limit 1
        ''').fetchall()

        # observed given related latent
        with pytest.raises(BQLError):
            bdb.execute('''
                estimate probability of x = 50 given (xe = 1) within p
            ''').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('''
                estimate probability of x = 50 given (xe = 1) within p
                    modelled by g0
            ''').fetchall()
        bdb.execute('''
            estimate probability of x = 50 given (xe = 1) within p
                modelled by g1
        ''').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('simulate x from p given xe = 1 limit 1').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('''
                simulate x from p modelled by g0 given xe = 1 limit 1
            ''').fetchall()
        bdb.execute('''
            simulate x from p modelled by g1 given xe = 1 limit 1
        ''').fetchall()

        # observed given unrelated latent
        with pytest.raises(BQLError):
            bdb.execute('''
                estimate probability of y = 50 given (xe = 1) within p
            ''').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('''
                estimate probability of y = 50 given (xe = 1) within p
                    modelled by g0
            ''').fetchall()
        bdb.execute('''
            estimate probability of y = 50 given (xe = 1) within p
                modelled by g1
        ''').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('simulate y from p given xe = 1 limit 1').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('''
                simulate y from p modelled by g0 given xe = 1 limit 1
            ''').fetchall()
        bdb.execute('''
            simulate y from p modelled by g1 given xe = 1 limit 1
        ''').fetchall()

        # latent given related observed
        with pytest.raises(BQLError):
            bdb.execute('''
                estimate probability of xe = 1 given (x = 50) within p
            ''').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('''
                estimate probability of xe = 1 given (x = 50) within p
                    modelled by g0
            ''').fetchall()
        bdb.execute('''
            estimate probability of xe = 1 given (x = 50) within p
                modelled by g1
        ''').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('simulate xe from p given x = 50 limit 1').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('''
                simulate xe from p modelled by g0 given x = 50 limit 1
            ''').fetchall()
        bdb.execute('''
            simulate xe from p modelled by g1 given x = 50 limit 1
        ''').fetchall()

        # latent given unrelated observed
        with pytest.raises(BQLError):
            bdb.execute('''
                estimate probability of xe = 1 given (y = 50) within p
            ''').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('''
                estimate probability of xe = 1 given (y = 50) within p
                    modelled by g0
            ''').fetchall()
        bdb.execute('''
            estimate probability of xe = 1 given (y = 50) within p
                modelled by g1
        ''').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('simulate xe from p given y = 50 limit 1').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('''
                simulate xe from p modelled by g0 given y = 50 limit 1
            ''').fetchall()
        bdb.execute('''
            simulate xe from p modelled by g1 given y = 50 limit 1
        ''').fetchall()

        bdb.execute('drop models from g0')
        bdb.execute('drop generator g0')
        bdb.execute('drop models from g1')
        bdb.execute('drop generator g1')
        bdb.execute('drop population p')
        bdb.execute('drop table t')

def test_nig_normal_latent_2var2lat_conditional_smoke():
    with bayesdb_open(':memory:') as bdb:
        bayesdb_register_metamodel(bdb, NIGNormalMetamodel())
        bdb.sql_execute('create table t(x, y)')
        for x in xrange(100):
            bdb.sql_execute('insert into t(x, y) values(?, ?)',
                (x, x*x - 100))
        bdb.execute('create population p for t(x numerical; y numerical)')
        bdb.execute('create generator g0 for p using nig_normal')
        bdb.execute('''
            create generator g1 for p using nig_normal(
                xe deviation(x),
                ye deviation(y)
            )
        ''')
        bdb.execute('initialize 1 model for g0')
        bdb.execute('analyze g0 for 1 iteration wait')
        bdb.execute('initialize 1 model for g1')
        bdb.execute('analyze g1 for 1 iteration wait')

        # latent given latent
        with pytest.raises(BQLError):
            bdb.execute('''
                estimate probability of xe = 1 given (ye = -1) within p
            ''').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('''
                estimate probability of xe = 1 given (ye = -1) within p
                     modelled by g0
            ''').fetchall()
        bdb.execute('''
            estimate probability of xe = 1 given (ye = -1) within p
                 modelled by g1
        ''').fetchall()

        with pytest.raises(BQLError):
            bdb.execute('''
                simulate xe from p given ye = -1 limit 1
            ''').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('''
                simulate xe from p modelled by g0 given ye = -1 limit 1
            ''').fetchall()
        bdb.execute('''
            simulate xe from p modelled by g1 given ye = -1 limit 1
        ''').fetchall()

        with pytest.raises(BQLError):
            bdb.execute(
                'estimate dependence probability of xe with ye within p')
        with pytest.raises(BQLError):
            bdb.execute('''
                estimate dependence probability of xe with ye within p
                    modelled by g0
            ''')
        bdb.execute('''
            estimate dependence probability of xe with ye within p
                modelled by g1
        ''')

        with pytest.raises(BQLError):
            bdb.execute(
                'estimate mutual information of xe with ye within p')
        with pytest.raises(BQLError):
            bdb.execute('''
                estimate mutual information of xe with ye within p
                    modelled by g0
            ''')
        bdb.execute('''
            estimate mutual information of xe with ye within p
                modelled by g1
        ''')

        bdb.execute('drop models from g0')
        bdb.execute('drop generator g0')
        bdb.execute('drop models from g1')
        bdb.execute('drop generator g1')
        bdb.execute('drop population p')
        bdb.execute('drop table t')
