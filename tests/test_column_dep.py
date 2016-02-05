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

import numpy as np
import pytest

import crosscat.LocalEngine

import bayeslite
from bayeslite.metamodels.crosscat import CrosscatMetamodel

# Synthetic dataset (x,y,z,v,w) for the tests. Fixed seed is not used since
# the tests should pass independently of the generated dataset.

def test_complex_dependencies__ci_slow():
    # Parameterize number of rows in synthetic dataset.
    n_rows = 250

    # Add an id column to ensure generator and cc colnos are different.
    ids = np.arange(n_rows)

    # Create real-valued data, such that DEP(x,y), DEP(y,z), and IND(x,z)
    mean = [4, -2, -11]
    cov = [[3.0, 0.7, 0.0],
           [0.7, 4.0, 0.6],
           [0.0, 0.6, 2.0]]
    numerical_data = np.random.multivariate_normal(mean, cov, size=n_rows)
    x, y, z = numerical_data[:,0], numerical_data[:,1], numerical_data[:,2]

    # Create categorical data v, highly dependent on x.
    bins = [np.percentile(x,p) for p in xrange(0,101,10)]
    v = np.digitize(x, bins)

    # Create categorical data, independent of all other columns.
    w = np.random.choice(range(8), size=n_rows)

    data = np.vstack((ids,x,y,z,w,v)).T

    # Create the database.
    with bayeslite.bayesdb_open(builtin_metamodels=False) as bdb:
        cc = crosscat.LocalEngine.LocalEngine(seed=0)
        ccme = CrosscatMetamodel(cc)
        bayeslite.bayesdb_register_metamodel(bdb, ccme)

        # Read the dataset.
        bdb.sql_execute('CREATE TABLE foo(id,x,y,z,v,w)')
        for row in data:
            bdb.sql_execute('INSERT INTO foo VALUES(?,?,?,?,?,?)', row)

        # Create schema, we will force  IND(x y), IND(x v), and DEP(z v w).
        bql = '''
            CREATE GENERATOR bar FOR foo USING crosscat(
                GUESS(*),
                id IGNORE,
                x NUMERICAL,
                y NUMERICAL,
                z NUMERICAL,
                v CATEGORICAL,
                w CATEGORICAL,
                INDEPENDENT(x, y),
                INDEPENDENT(x, v),
                DEPENDENT(z, v, w)
            );
        '''
        bdb.execute(bql)

        # Prepare the checker function.
        def check_dependencies():
            bql = '''
                ESTIMATE DEPENDENCE PROBABILITY FROM PAIRWISE COLUMNS OF bar
            '''
            for _id, col1, col2, dep in bdb.execute(bql):
                # test IND(x y)
                if (col1, col2) in [('x','y'), ('y','x')]:
                    assert dep == 0
                    continue
                # test IND(x v)
                if (col1, col2) in [('x','v'), ('v','x')]:
                    assert dep == 0
                    continue
                # test DEP(z v)
                if (col1, col2) in [('z','v'), ('v','z')]:
                    assert dep == 1
                    continue
                # test DEP(z w)
                if (col1, col2) in [('z', 'w'), ('w', 'z')]:
                    assert dep == 1
                    continue

        # Test dependency pre-analysis.
        bdb.execute('INITIALIZE 10 MODELS FOR bar')
        check_dependencies()

        # Test dependency post-analysis.
        bdb.execute('ANALYZE bar for 10 ITERATION WAIT')
        check_dependencies()

def test_impossible_duplicate_dependency():
    # Throw exception when two columns X and Y are both dependent and
    # independent.

    data = [(0, 1, 0, 0), (1, 0, 0, 1)]

    # Create the database.
    with bayeslite.bayesdb_open(builtin_metamodels=False) as bdb:
        cc = crosscat.LocalEngine.LocalEngine(seed=0)
        ccme = CrosscatMetamodel(cc)
        bayeslite.bayesdb_register_metamodel(bdb, ccme)

        # Read the dataset.
        bdb.sql_execute('CREATE TABLE foo(id,a,b,c)')
        for row in data:
            bdb.sql_execute('INSERT INTO foo VALUES(?,?,?,?)', row)

        # Create schema, we will force DEP(a c) and IND(a c).
        bql = '''
            CREATE GENERATOR bar FOR foo USING crosscat(
                GUESS(*),
                id IGNORE,
                a CATEGORICAL,
                b CATEGORICAL,
                c CATEGORICAL,
                INDEPENDENT(a,b,c),
                DEPENDENT(a,c),
            );
        '''

        # An error should be thrown about impossible schema.
        with pytest.raises(bayeslite.BQLError):
            bdb.execute(bql)

def test_impossible_nontransitive_dependency():
    # Test impossibility of non-transitive dependencies. While in the
    # general case, dependence is not transitive, crosscat assumes
    # transitive closure under dependency constraints.  The test is
    # valid since we are using a crosscat local engine.  Note that
    # transitivity under independence is not forced by crosscat.
    # Changing the behavior of CrossCat to deal with impossible
    # constraints (such as random dropout) will require updating this
    # test.
    data = [(0, 1, 0, 0), (1, 0, 0, 1)]

    # Create the database.
    with bayeslite.bayesdb_open(builtin_metamodels=False) as bdb:
        cc = crosscat.LocalEngine.LocalEngine(seed=0)
        ccme = CrosscatMetamodel(cc)
        bayeslite.bayesdb_register_metamodel(bdb, ccme)

        # Read the dataset.
        bdb.sql_execute('CREATE TABLE foo(id,a,b,c)')
        for row in data:
            bdb.sql_execute('INSERT INTO foo VALUES(?,?,?,?)', row)

        # Create schema, we will force DEP(a b), DEP(b c), and IND(a c) which
        # is non-transitive.
        bql = '''
            CREATE GENERATOR bar FOR foo USING crosscat(
                GUESS(*),
                id IGNORE,
                a CATEGORICAL,
                b CATEGORICAL,
                c CATEGORICAL,
                DEPENDENT(a,b),
                DEPENDENT(b,c),
                INDEPENDENT(a,c)
            );
        '''

        # Creating the generator should succeed.
        bdb.execute(bql)

        # Error thrown when initializing since no initial state exists.
        # XXX Currently CrossCat throws a RuntimeError, we should fix
        # the CrossCat exception hierarchy.
        with pytest.raises(RuntimeError):
            bdb.execute('INITIALIZE 10 MODELS FOR bar')
