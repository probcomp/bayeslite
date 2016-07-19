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
        with pytest.raises(ValueError):
            bdb.execute(
                'estimate predictive probability of xe from p').fetchall()
        with pytest.raises(ValueError):
            bdb.execute('''
                estimate predictive probability of xe from p modelled by g0
            ''').fetchall()
        for r, p_xe in bdb.execute('''
            estimate rowid, predictive probability of xe from p modelled by g1
        '''):
            assert p_xe is None, 'rowid %r p(xe) %r' % (r, p_xe)

        # SIMULATE x
        bdb.execute('simulate x from p limit 1').fetchall()
        with pytest.raises(BQLError):
            bdb.execute('simulate x, xe from p limit 1').fetchall()
        with pytest.raises(BQLError):
            bdb.execute(
                'simulate x, xe from p modelled by g0 limit 1').fetchall()
        bdb.execute('simulate x, xe from p modelled by g1 limit 1').fetchall()

        assert 100*100 == \
            len(bdb.execute('estimate similarity from pairwise p').fetchall())
        assert 1 == len(bdb.execute('''
            estimate similarity from pairwise p modelled by g0 limit 1
        ''').fetchall())
        assert 1 == len(bdb.execute('''
            estimate similarity with respect to (x)
                from pairwise p modelled by g0 limit 1
        ''').fetchall())
        with pytest.raises(BQLError):
            assert 1 == len(bdb.execute('''
                estimate similarity with respect to (xe)
                    from pairwise p modelled by g0 limit 1
            ''').fetchall())
        with pytest.raises(BQLError):
            assert 1 == len(bdb.execute('''
                estimate similarity with respect to (x, xe)
                    from pairwise p modelled by g0 limit 1
            ''').fetchall())
        assert 1 == len(bdb.execute('''
            estimate similarity from pairwise p modelled by g1 limit 1
        ''').fetchall())
        assert 1 == len(bdb.execute('''
            estimate similarity with respect to (xe)
                from pairwise p modelled by g1 limit 1
        ''').fetchall())
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

def test_nig_normal_latent_2var_smoke():
    with bayesdb_open(':memory:') as bdb:
        bayesdb_register_metamodel(bdb, NIGNormalMetamodel())
        bdb.sql_execute('create table t(x, y)')
        for x in xrange(100):
            bdb.sql_execute('insert into t(x, y) values(?, ?)',
                (x, x*x - 100))
        bdb.execute('create population p for t(x numerical, y numerical)')

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
            estimate 1
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
