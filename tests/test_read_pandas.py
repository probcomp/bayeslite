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

import apsw
import pandas
import pytest

from bayeslite import bayesdb_open
from bayeslite import bql_quote_name
from bayeslite.core import bayesdb_has_table
from bayeslite.read_pandas import bayesdb_read_pandas_df

def do_test(bdb, t, df, index=None):
    qt = bql_quote_name(t)
    countem = 'select count(*) from %s' % (qt,)
    assert not bayesdb_has_table(bdb, t)

    with pytest.raises(ValueError):
        bayesdb_read_pandas_df(bdb, t, df, index=index)

    bayesdb_read_pandas_df(bdb, t, df, create=True, ifnotexists=False,
        index=index)
    assert len(df.index) == bdb.execute(countem).fetchvalue()

    with pytest.raises(ValueError):
        bayesdb_read_pandas_df(bdb, t, df, create=True, ifnotexists=False,
            index=index)
    assert 4 == bdb.execute(countem).fetchvalue()

    with pytest.raises(apsw.ConstraintError):
        bayesdb_read_pandas_df(bdb, t, df, create=True, ifnotexists=True,
            index=index)
    assert 4 == bdb.execute(countem).fetchvalue()

def test_integral_noindex():
    with bayesdb_open() as bdb:
        df = pandas.DataFrame([(1,2,'foo'),(4,5,6),(7,8,9),(10,11,12)],
            index=[42, 78, 62, 43])
        do_test(bdb, 't', df)

def test_integral_index():
    with bayesdb_open() as bdb:
        df = pandas.DataFrame([(1,2,'foo'),(4,5,6),(7,8,9),(10,11,12)],
            index=[42, 78, 62, 43])
        do_test(bdb, 't', df, index='quagga')

def test_nonintegral_noindex():
    with bayesdb_open() as bdb:
        df = pandas.DataFrame([(1,2,'foo'),(4,5,6),(7,8,9),(10,11,12)],
            index=[42, 78, 62, 43])
        with pytest.raises(ValueError):
            bayesdb_read_pandas_df(bdb, 't', df)

def test_nonintegral_index():
    with bayesdb_open() as bdb:
        df = pandas.DataFrame([(1,2,'foo'),(4,5,6),(7,8,9),(10,11,12)],
            index=[42, 78, 62, 43])
        do_test(bdb, 't', df, index='eland')
