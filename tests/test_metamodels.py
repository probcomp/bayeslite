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
import tempfile

import crosscat.LocalEngine

import bayeslite

import bayeslite.core as core

from bayeslite import bql_quote_name
from bayeslite.metamodels.crosscat import CrosscatMetamodel
from bayeslite.metamodels.iid_gaussian import StdNormalMetamodel

examples = {
    'crosscat': (
        lambda: CrosscatMetamodel(crosscat.LocalEngine.LocalEngine(seed=0)),
        't',
        'CREATE TABLE t(x NUMERIC, y CYCLIC, z CATEGORICAL)',
        'INSERT INTO t (x, y, z) VALUES (?, ?, ?)',
        [
            (0, 1.57, 'foo'),
            (1.83, 3.141, 'bar'),
            (1.82, 3.140, 'bar'),
            (-1, 6.28, 'foo'),
        ],
        't_cc',
        'CREATE GENERATOR t_cc FOR t USING crosscat'
            '(x NUMERICAL, y CYCLIC, z CATEGORICAL)',
        'CREATE GENERATOR t_cc FOR t USING crosscat'
            '(x NUMERICAL, y ZAPATISTICAL, z CATEGORICAL)',
        'CREATE GENERATOR t_cc FOR t USING crosscat'
            '(x NUMERICAL, y CYCLIC, w CATEGORICAL)',
    ),
    'iid_gaussian': (
        lambda: StdNormalMetamodel(seed=0),
        't',
        'CREATE TABLE t(x NUMERIC, y NUMERIC)',
        'INSERT INTO t (x, y) VALUES (?, ?)',
        [(0, 1), (1, float('nan')), (2, -1.2)],
        't_sn',
        'CREATE GENERATOR t_sn FOR t USING std_normal'
            ' (x NUMERICAL, y NUMERICAL)',
        # XXX Should invent something that fails for
        # metamodel-specific reasons here.
        'CREATE GENERATOR t_sn FOR t USING std_normal'
            ' (x NUMERICAL, z NUMERICAL)',
        'CREATE GENERATOR t_sn FOR t USING std_normal'
            ' (x NUMERICAL, z NUMERICAL)',
    ),
}

@pytest.mark.parametrize('persist,exname',
    [(persist, key)
        for persist in (True, False)
        for key in sorted(examples.keys())])
def test_example(persist, exname):
    if persist:
        with tempfile.NamedTemporaryFile(prefix='bayeslite') as f:
            with bayeslite.bayesdb_open(pathname=f.name,
                    builtin_metamodels=False) as bdb:
                _test_example(bdb, exname)
            with bayeslite.bayesdb_open(pathname=f.name,
                    builtin_metamodels=False) as bdb:
                _retest_example(bdb, exname)
    else:
        with bayeslite.bayesdb_open(builtin_metamodels=False) as bdb:
            _test_example(bdb, exname)

def _test_example(bdb, exname):
    mm, t, t_sql, data_sql, data, g, g_bql, g_bqlbad0, g_bqlbad1 = \
        examples[exname]
    qt = bql_quote_name(t)
    qg = bql_quote_name(g)

    bayeslite.bayesdb_register_metamodel(bdb, mm())

    # Create a table.
    assert not core.bayesdb_has_table(bdb, t)
    with bdb.savepoint_rollback():
        bdb.sql_execute(t_sql)
        assert core.bayesdb_has_table(bdb, t)
    assert not core.bayesdb_has_table(bdb, t)
    bdb.sql_execute(t_sql)
    assert core.bayesdb_has_table(bdb, t)

    # Insert data into the table.
    assert bdb.execute('SELECT COUNT(*) FROM %s' % (qt,)).fetchvalue() == 0
    for row in data:
        bdb.sql_execute(data_sql, row)
    n = len(data)
    assert bdb.execute('SELECT COUNT(*) FROM %s' % (qt,)).fetchvalue() == n

    # Create a generator.  Make sure savepoints work for this.
    assert not core.bayesdb_has_generator(bdb, g)
    with pytest.raises(Exception):
        with bdb.savepoint():
            bdb.execute(g_bqlbad0)
    assert not core.bayesdb_has_generator(bdb, g)
    with pytest.raises(Exception):
        with bdb.savepoint():
            bdb.execute(g_bqlbad1)
    assert not core.bayesdb_has_generator(bdb, g)
    with bdb.savepoint_rollback():
        bdb.execute(g_bql)
        assert core.bayesdb_has_generator(bdb, g)
    assert not core.bayesdb_has_generator(bdb, g)
    bdb.execute(g_bql)
    assert core.bayesdb_has_generator(bdb, g)
    with pytest.raises(Exception):
        bdb.execute(g_bql)
    assert core.bayesdb_has_generator(bdb, g)

    gid = core.bayesdb_get_generator(bdb, g)
    assert not core.bayesdb_generator_has_model(bdb, gid, 0)
    assert [] == core.bayesdb_generator_modelnos(bdb, gid)
    with bdb.savepoint_rollback():
        bdb.execute('INITIALIZE 1 MODEL FOR %s' % (qg,))
        assert core.bayesdb_generator_has_model(bdb, gid, 0)
        assert [0] == core.bayesdb_generator_modelnos(bdb, gid)
    with bdb.savepoint_rollback():
        bdb.execute('INITIALIZE 10 MODELS FOR %s' % (qg,))
        for i in range(10):
            assert core.bayesdb_generator_has_model(bdb, gid, i)
            assert range(10) == core.bayesdb_generator_modelnos(bdb, gid)
    bdb.execute('INITIALIZE 2 MODELS FOR %s' % (qg,))

    # Test dropping things.
    with pytest.raises(bayeslite.BQLError):
        bdb.execute('DROP TABLE %s' % (qt,))
    with bdb.savepoint_rollback():
        # Note that sql_execute does not protect us!
        bdb.sql_execute('DROP TABLE %s' % (qt,))
        assert not core.bayesdb_has_table(bdb, t)
    assert core.bayesdb_has_table(bdb, t)
    # XXX Should we reject dropping a generator when there remain
    # models?  Should we not reject dropping a table when there remain
    # generators?  A table can be dropped when there remain indices.
    #
    # with pytest.raises(bayeslite.BQLError):
    #     # Models remain.
    #     bdb.execute('DROP GENERATOR %s' % (qg,))
    with bdb.savepoint_rollback():
        bdb.execute('DROP GENERATOR %s' % (qg,))
        assert not core.bayesdb_has_generator(bdb, g)
    assert core.bayesdb_has_generator(bdb, g)
    with bdb.savepoint_rollback():
        bdb.execute('DROP GENERATOR %s' % (qg,))
        assert not core.bayesdb_has_generator(bdb, g)
        bdb.execute(g_bql)
        assert core.bayesdb_has_generator(bdb, g)
    assert core.bayesdb_has_generator(bdb, g)
    assert gid == core.bayesdb_get_generator(bdb, g)

    # Test dropping models.
    with bdb.savepoint_rollback():
        bdb.execute('DROP MODEL 1 FROM %s' % (qg,))
        assert core.bayesdb_generator_has_model(bdb, gid, 0)
        assert not core.bayesdb_generator_has_model(bdb, gid, 1)
        assert [0] == core.bayesdb_generator_modelnos(bdb, gid)

    # Test analyzing models.
    bdb.execute('ANALYZE %s FOR 1 ITERATION WAIT' % (qg,))
    bdb.execute('ANALYZE %s MODEL 0 FOR 1 ITERATION WAIT' % (qg,))
    bdb.execute('ANALYZE %s MODEL 1 FOR 1 ITERATION WAIT' % (qg,))

def _retest_example(bdb, exname):
    mm, t, t_sql, data_sql, data, g, g_bql, g_bqlbad0, g_bqlbad1 = \
        examples[exname]
    qt = bql_quote_name(t)
    qg = bql_quote_name(g)

    bayeslite.bayesdb_register_metamodel(bdb, mm())

    assert core.bayesdb_has_table(bdb, t)
    assert core.bayesdb_has_generator(bdb, g)
    gid = core.bayesdb_get_generator(bdb, g)
    assert core.bayesdb_generator_has_model(bdb, gid, 0)
    assert core.bayesdb_generator_has_model(bdb, gid, 1)
    bdb.execute('ANALYZE %s FOR 1 ITERATION WAIT' % (qg,))
    bdb.execute('ANALYZE %s MODEL 0 FOR 1 ITERATION WAIT' % (qg,))
    bdb.execute('ANALYZE %s MODEL 1 FOR 1 ITERATION WAIT' % (qg,))
