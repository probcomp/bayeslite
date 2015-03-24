# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2014, MIT Probabilistic Computing Project
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

import StringIO
import contextlib
import pytest

import bayeslite

@contextlib.contextmanager
def bayesdb(*args, **kwargs):
    bdb = bayeslite.BayesDB(*args, **kwargs)
    try:
        yield bdb
    finally:
        bdb.close()

csv_data = '''1,2,3,foo,bar,nan,"",quagga
4,5,6,baz,quux,42.0,"",eland
7,8,6,zot,mumble,87.0,"zoot",caribou
'''

csv_hdr = 'a,b,c,name,nick,age,muppet,animal\n'

csv_hdrdata = csv_hdr + csv_data

def test_read_csv():
    with bayesdb() as bdb:
        f = StringIO.StringIO(csv_data)
        with pytest.raises(ValueError):
            # Table must already exist for create=False.
            bayeslite.bayesdb_read_csv(bdb, 't', f, header=False, create=False,
                ifnotexists=False)
        f = StringIO.StringIO(csv_data)
        with pytest.raises(ValueError):
            # Must pass create=True for ifnotexists=True.
            bayeslite.bayesdb_read_csv(bdb, 't', f, header=False, create=False,
                ifnotexists=True)
        f = StringIO.StringIO(csv_data)
        with pytest.raises(ValueError):
            # Must pass create=False for header=False.
            bayeslite.bayesdb_read_csv(bdb, 't', f, header=False, create=True,
                ifnotexists=False)
        f = StringIO.StringIO(csv_data)
        with pytest.raises(ValueError):
            # Must pass create=False for header=False.
            bayeslite.bayesdb_read_csv(bdb, 't', f, header=False, create=True,
                ifnotexists=True)
        f = StringIO.StringIO(csv_hdrdata)
        with pytest.raises(ValueError):
            # Table must already exist for create=False.
            bayeslite.bayesdb_read_csv(bdb, 't', f, header=True, create=False,
                ifnotexists=False)
        f = StringIO.StringIO(csv_hdrdata)
        with pytest.raises(ValueError):
            # Must pass create=True for ifnotexists=True.
            bayeslite.bayesdb_read_csv(bdb, 't', f, header=True, create=False,
                ifnotexists=True)
        f = StringIO.StringIO(csv_hdrdata)
        with pytest.raises(ValueError):
            with bdb.savepoint():
                # Table must not exist if ifnotexists=False.
                bdb.sql_execute('CREATE TABLE t(x)')
                bayeslite.bayesdb_read_csv(bdb, 't', f, header=True,
                    create=True, ifnotexists=False)
        f = StringIO.StringIO(csv_hdrdata)
        bayeslite.bayesdb_read_csv(bdb, 't', f, header=True, create=True,
            ifnotexists=False)
        data = list(bdb.sql_execute('SELECT * FROM t'))
        assert data == [
            # XXX Would be nice if the NaN could actually be that, or
            # at least None/NULL.
            (1,2,3,'foo','bar',u'nan',u'',u'quagga'),
            (4,5,6,'baz','quux',42.0,u'',u'eland'),
            (7,8,6,'zot','mumble',87.0,u'zoot',u'caribou'),
        ]
        f = StringIO.StringIO(csv_hdr)
        bayeslite.bayesdb_read_csv(bdb, 't', f, header=True, create=True,
            ifnotexists=True)
        assert list(bdb.sql_execute('SELECT * FROM t')) == data
        assert bdb.sql_execute('SELECT sql FROM sqlite_master WHERE name = ?',
                ('t',)).next()[0] == \
            'CREATE TABLE "t"' \
            '("a" NUMERIC,"b" NUMERIC,"c" NUMERIC,"name" NUMERIC,' \
            '"nick" NUMERIC,"age" NUMERIC,"muppet" NUMERIC,"animal" NUMERIC)'
        f = StringIO.StringIO(csv_data)
        bayeslite.bayesdb_read_csv(bdb, 't', f, header=False, create=False,
            ifnotexists=False)
        assert list(bdb.sql_execute('SELECT * FROM t')) == data + data
        f = StringIO.StringIO(csv_hdrdata)
        bayeslite.bayesdb_read_csv(bdb, 't', f, header=True, create=False,
            ifnotexists=False)
        assert list(bdb.sql_execute('SELECT * FROM t')) == data + data + data
