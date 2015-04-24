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

import contextlib
import os
import pexpect
import pytest
import tempfile


TIMEOUT = 2
ROOT = os.path.dirname(os.path.abspath(__file__))
DHA_CSV = os.path.join(ROOT, '..', '..', 'tests', 'dha.csv')
THOOKS_PY = os.path.join(ROOT, 'thooks.py')

READ_DATA = '''
-- do something that fails (should not kick us out)
.csv dha

-- create a table properly
.csv dha {0}

-- single line BQL
SELECT name FROM dha LIMIT 2;

-- mulitline BQL. 2nd line is space indented; 3rd line is tabbed.
SELECT name FROM dha
    ORDER BY name ASC
    LIMIT 5;
'''.format(DHA_CSV)


class spawnjr(pexpect.spawn):
    def expectprompt(self):
        return self.expect('bayeslite> ', timeout=TIMEOUT)


@contextlib.contextmanager
def read_data():
    with tempfile.NamedTemporaryFile(prefix='bayeslite-shell') as temp:
        with open(temp.name, 'w') as f:
            f.write(READ_DATA)
        yield temp.name


@pytest.fixture
def spawnbdb():
    c = spawnjr('bayeslite --no-init-file --debug')
    c.delaybeforesend = 0
    c.expectprompt()
    return c


@pytest.fixture
def spawntable():
    c = spawnjr('bayeslite')
    c.expectprompt()
    assert not an_error_probably_happened(c.before)

    c.sendline('.csv dha %s' % (DHA_CSV,))
    c.expectprompt()
    assert not an_error_probably_happened(c.before)
    return 'dha', c


@pytest.fixture
def spawngen():
    c = spawnjr('bayeslite')
    c.expectprompt()
    assert not an_error_probably_happened(c.before)

    c.sendline('.csv dha %s' % (DHA_CSV,))
    c.expectprompt()
    assert not an_error_probably_happened(c.before)

    c.sendline('.guess dha_cc dha')
    c.expectprompt()
    assert not an_error_probably_happened(c.before)
    return 'dha_cc', c


def an_error_probably_happened(string):
    error_clues = ['error', 'traceback', 'exception']
    stringlower = string.lower()
    return any(x in stringlower for x in error_clues)


# Tests begin
# ````````````````````````````````````````````````````````````````````````````
def test_shell_loads(spawnbdb):
    c = spawnbdb


def test_python_expression(spawnbdb):
    c = spawnbdb
    c.sendline('.python 2 * 3')
    c.expectprompt()

    assert not an_error_probably_happened(c.before)
    assert '6' in c.before


def test_help_returns_list_of_commands(spawnbdb):
    c = spawnbdb
    c.sendline('.help')
    c.expectprompt()

    assert '.codebook' in c.before
    assert '.csv' in c.before
    assert '.describe' in c.before
    assert '.guess' in c.before
    assert '.help' in c.before
    assert '.hook' in c.before
    assert '.python' in c.before
    assert '.read' in c.before
    assert '.sql' in c.before
    assert '.trace' in c.before
    assert '.untrace' in c.before


def test_dot_csv(spawnbdb):
    c = spawnbdb
    cmd = '.csv dha %s' % (DHA_CSV,)
    c.sendline(cmd)
    c.expectprompt()
    assert not an_error_probably_happened(c.before)


def test_describe_columns_without_generator(spawntable):
    table, c = spawntable
    c.sendline('.describe columns %s' % (table,))
    c.expect('No such generator: %s' % (table,), timeout=TIMEOUT)


def test_bql_select(spawntable):
    table, c = spawntable
    c.sendline('SELECT name FROM %s ORDER BY name ASC LIMIT 5;' % (table,))
    checkstr = "             NAME\r\n" +\
               "-----------------\r\n" +\
               "       Abilene TX\r\n" +\
               "         Akron OH\r\n" +\
               "Alameda County CA\r\n" +\
               "        Albany GA\r\n" +\
               "        Albany NY\r\n"
    c.expectprompt()
    assert not an_error_probably_happened(c.before)
    assert checkstr in c.before


def test_guess(spawntable):
    table, c = spawntable
    cmd = '.guess dha_cc dha'
    c.sendline(cmd)
    c.expectprompt()

    assert not an_error_probably_happened(c.before)
    assert len(c.before.strip()) == len(cmd)


def test_sql(spawntable):
    table, c = spawntable
    cmd = '.sql pragma table_info(bayesdb_column)'
    c.sendline(cmd)
    c.expectprompt()

    assert not an_error_probably_happened(c.before)

    splitres = c.before.split('\n')

    # remove the previous command column and the header underline
    del splitres[0]
    del splitres[1]

    splitstr = [s.strip().replace(' ', '').split('|') for s in splitres]
    assert len(splitstr[0]) == 6
    assert splitstr[0] == ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk']


def test_describe_column_with_gnerator(spawngen):
    gen, c = spawngen
    c.sendline('.describe models %s' % (gen,))
    c.expectprompt()

    checkstr = "modelno | iterations\r\n--------+-----------\r\n"
    assert checkstr in c.before

    c.sendline('.describe columns %s' % (gen,))
    c.expectprompt()
    assert not an_error_probably_happened(c.before)

    splitres = c.before.split('\n')
    splitstr = [s.strip().replace(' ', '') for s in splitres[1:]]

    assert splitstr[0] == 'colno|name|stattype|shortname'
    assert len(splitstr) == 66
    for row in splitstr[2:-1]:
        srow = row.split('|')

        assert len(srow) == 4
        assert srow[3] == 'None'
        assert srow[2] == 'numerical'


def test_hook(spawnbdb):
    c = spawnbdb
    c.sendline('.hook %s' % (THOOKS_PY,))
    c.expectprompt()

    assert not an_error_probably_happened(c.before)
    assert 'added command ".myhook"' in c.before

    c.sendline('.help')
    c.expectprompt()

    assert not an_error_probably_happened(c.before)
    assert 'myhook help string' in c.before

    c.sendline('.help myhook')
    c.expectprompt()

    assert not an_error_probably_happened(c.before)
    assert '.myhook <string>' in c.before

    c.sendline('.myhook zoidberg')
    c.expectprompt()

    assert not an_error_probably_happened(c.before)
    assert 'john zoidberg' in c.before


def test_read_nonsequential(spawnbdb):
    c = spawnbdb

    with read_data() as fname:
        c.sendline('.read %s' % (fname,))
        c.expect('--DEBUG: .read complete', timeout=2)
    res = c.before

    assert not an_error_probably_happened(res)

    assert res.count(' .csv') == 1
    assert res.count('SELECT') == 0

    # should print SELECT output
    assert res.count('NAME') == 2


def test_read_nonsequential_verbose(spawnbdb):
    c = spawnbdb

    with read_data() as fname:
        c.sendline('.read %s -v' % (fname,))
        c.expect('--DEBUG: .read complete', timeout=2)
    res = c.before

    assert not an_error_probably_happened(res)

    # should output help on first failure
    assert res.count(' .csv') == 3
    assert res.count('SELECT') == 2

    # should print SELECT output
    assert res.count('NAME') == 2
