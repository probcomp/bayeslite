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

import os
import pytest
import pexpect

TIMEOUT = 2
ROOT = os.path.dirname(os.path.abspath(__file__))
DHA_CSV = os.path.join(ROOT, '..', '..', 'tests', 'dha.csv')
THOOKS_PY = os.path.join(ROOT, 'thooks.py')


@pytest.fixture
def spawnbdb():
    return pexpect.spawn('bayeslite')


@pytest.fixture
def spawntable():
    c = pexpect.spawn('bayeslite')
    c.expect('bayeslite>', timeout=TIMEOUT)
    assert not an_error_probably_happened(c.before)

    c.sendline('.csv dha %s' % (DHA_CSV,))
    c.expect('bayeslite>', timeout=TIMEOUT)
    assert not an_error_probably_happened(c.before)
    return 'dha', c


@pytest.fixture
def spawngen():
    c = pexpect.spawn('bayeslite')
    c.expect('bayeslite>', timeout=TIMEOUT)
    assert not an_error_probably_happened(c.before)

    c.sendline('.csv dha %s' % (DHA_CSV,))
    c.expect('bayeslite>', timeout=TIMEOUT)
    assert not an_error_probably_happened(c.before)

    c.sendline('.guess dha_cc dha')
    c.expect('bayeslite>', timeout=TIMEOUT)
    assert not an_error_probably_happened(c.before)
    return 'dha_cc', c


def an_error_probably_happened(string):
    error_clues = ['Error', 'Traceback']
    return any([x in string for x in error_clues])


# Tests begin
# ````````````````````````````````````````````````````````````````````````````
def test_shell_loads(spawnbdb):
    c = spawnbdb
    c.expect('bayeslite>', timeout=TIMEOUT)


def test_python_expression(spawnbdb):
    c = spawnbdb
    c.expect('bayeslite>', timeout=TIMEOUT)
    c.sendline('.python 2 * 3')
    c.expect('bayeslite>', timeout=TIMEOUT)

    assert not an_error_probably_happened(c.before)
    assert '6' in c.before


def test_help_returns_list_of_commands(spawnbdb):
    c = spawnbdb
    c.expect('bayeslite>', timeout=TIMEOUT)
    c.sendline('.help')
    c.expect('bayeslite>', timeout=TIMEOUT)

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
    c.expect('bayeslite> ', timeout=TIMEOUT)
    cmd = '.csv dha %s' % (DHA_CSV)
    c.sendline(cmd)
    c.expect('bayeslite> ', timeout=TIMEOUT)
    assert not an_error_probably_happened(c.before)
    assert c.before.strip().decode('unicode_escape').replace(' \x08', '') \
        == cmd.strip()


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
    c.expect('bayeslite> ', timeout=TIMEOUT)
    assert not an_error_probably_happened(c.before)
    assert checkstr in c.before


def test_guess(spawntable):
    table, c = spawntable
    cmd = '.guess dha_cc dha'
    c.sendline(cmd)
    c.expect('bayeslite> ', timeout=TIMEOUT)

    # add 3 to length of cmd because \r\n is tacked on the end and a space is
    # tacked onto the front
    assert not an_error_probably_happened(c.before)
    assert len(c.before) == len(cmd) + 3


def test_sql(spawntable):
    table, c = spawntable
    cmd = '.sql pragma table_info(bayesdb_column)'
    c.sendline(cmd)
    c.expect('bayeslite> ', timeout=TIMEOUT)

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
    c.expect('bayeslite> ', timeout=TIMEOUT)

    checkstr = "modelno | iterations\r\n--------+-----------\r\n"
    assert checkstr in c.before

    c.sendline('.describe columns %s' % (gen,))
    c.expect('bayeslite> ', timeout=TIMEOUT)
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
    c.expect('bayeslite>', timeout=TIMEOUT)
    c.sendline('.hook %s' % (THOOKS_PY,))
    c.expect('bayeslite>', timeout=TIMEOUT)

    assert not an_error_probably_happened(c.before)
    assert 'added command ".myhook"' in c.before

    c.sendline('.help')
    c.expect('bayeslite>', timeout=TIMEOUT)

    assert not an_error_probably_happened(c.before)
    assert 'myhook help string' in c.before

    c.sendline('.help myhook')
    c.expect('bayeslite>', timeout=TIMEOUT)

    assert not an_error_probably_happened(c.before)
    assert '.myhook <string>' in c.before

    c.sendline('.myhook zoidberg')
    c.expect('bayeslite>', timeout=TIMEOUT)

    assert not an_error_probably_happened(c.before)
    assert 'john zoidberg' in c.before
