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

import StringIO
import pytest
import tempfile

import bayeslite

from bayeslite.util import cursor_value

csv_hdr = 'a,b,c,name,nick,age,muppet,animal\n'

csv_data = '''1,2,3,foo,bar,nan,"",quagga
4,5,6,baz,quux,42.0,"",eland
7,8,6,zot,mumble,87.0,"zoot",caribou
'''

csv_hdrdata = csv_hdr + csv_data

def test_read_csv():
    with bayeslite.bayesdb_open(builtin_metamodels=False) as bdb:

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
        with pytest.raises(IOError):
            # Table must have no empty values in header.
            csv_hdrdata_prime = csv_hdrdata[1:]
            f = StringIO.StringIO(csv_hdrdata_prime)
            with bdb.savepoint():
                bayeslite.bayesdb_read_csv(bdb, 't', f, header=True,
                    create=True, ifnotexists=False)

        f = StringIO.StringIO(csv_hdrdata)
        bayeslite.bayesdb_read_csv(bdb, 't', f, header=True, create=True,
            ifnotexists=False)
        data = bdb.sql_execute('SELECT * FROM t').fetchall()
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
        assert bdb.sql_execute('SELECT * FROM t').fetchall() == data
        assert cursor_value(bdb.sql_execute('SELECT sql FROM sqlite_master'
                    ' WHERE name = ?', ('t',))) == \
            'CREATE TABLE "t"' \
            '("a" NUMERIC,"b" NUMERIC,"c" NUMERIC,"name" NUMERIC,' \
            '"nick" NUMERIC,"age" NUMERIC,"muppet" NUMERIC,"animal" NUMERIC)'

        f = StringIO.StringIO(csv_data)
        bayeslite.bayesdb_read_csv(bdb, 't', f, header=False, create=False,
            ifnotexists=False)
        assert bdb.sql_execute('SELECT * FROM t').fetchall() == data + data

        f = StringIO.StringIO(csv_hdrdata)
        bayeslite.bayesdb_read_csv(bdb, 't', f, header=True, create=False,
            ifnotexists=False)
        assert bdb.sql_execute('SELECT * FROM t').fetchall() == \
            data + data + data
        with tempfile.NamedTemporaryFile(prefix='bayeslite') as temp:
            with open(temp.name, 'w') as f:
                f.write(csv_hdrdata)
            bayeslite.bayesdb_read_csv_file(bdb, 't', temp.name, header=True,
                create=False, ifnotexists=False)
        assert bdb.sql_execute('SELECT * FROM t').fetchall() == \
            data + data + data + data

        # Test the BQL CREATE TABLE FROM <csv-file> syntax.
        f = StringIO.StringIO(csv_hdrdata)
        with tempfile.NamedTemporaryFile(prefix='bayeslite') as temp:
            with open(temp.name, 'w') as f:
                f.write(csv_hdrdata)
            bdb.execute('CREATE TABLE t2 FROM \'%s\'' % (temp.name,))
            assert bdb.sql_execute('SELECT * FROM t2').fetchall() == data

        # Trying to read a csv with an empty column name should fail.
        csv_header_corrupt = csv_hdr.replace('a,b',',')
        csv_hdrdata_corrupt = csv_header_corrupt + csv_data
        with tempfile.NamedTemporaryFile(prefix='bayeslite') as temp:
            with open(temp.name, 'w') as f:
                f.write(csv_hdrdata_corrupt)
            with pytest.raises(IOError):
                bayeslite.bayesdb_read_csv_file(
                    bdb, 't3', temp.name, header=True, create=True)
