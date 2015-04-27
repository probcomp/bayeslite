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
    def __init__(self, *args, **kwargs):
        if 'timeout' not in kwargs:
            kwargs['timeout'] = TIMEOUT
        super(spawnjr, self).__init__(*args, **kwargs)


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
    c.expect_exact(
        'Welcome to the Bayeslite shell.\r\n'
        "Type `.help' for help.\r\n"
        'bayeslite> '
    )
    assert c.before == ''
    return c


@pytest.fixture
def spawntable():
    c = spawnbdb()
    cmd = '.csv dha %s' % (DHA_CSV,)
    c.sendline(cmd)
    c.expect_exact('bayeslite> ')
    # XXX Kludge to strip control characters introduced by the pty
    # when the line wraps, which vary from system to system (some use
    # backspace; some use carriage return; some insert spaces).
    def remove_control(s):
        return s.translate(None, ''.join(map(chr, range(32 + 1) + [127])))
    assert remove_control(c.before) == remove_control(cmd)
    return 'dha', c


@pytest.fixture
def spawngen(spawntable):
    table, c = spawntable
    c.sendline('.guess dha_cc %s' % (table,))
    c.expect_exact(
        '.guess dha_cc %s\r\n'
        'bayeslite> ' % (table,)
    )
    assert c.before == ''
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
    c.expect_exact(
        '.python 2 * 3\r\n'
        '6\r\n'
        'bayeslite> '
    )
    assert c.before == ''


def test_help_returns_list_of_commands(spawnbdb):
    c = spawnbdb
    c.sendline('.help')
    c.expect_exact(
        '.help\r\n'
        '     .codebook    load codebook for table\r\n'
        '          .csv    create table from CSV file\r\n'
        '     .describe    describe BayesDB entities\r\n'
        '        .guess    guess data generator\r\n'
        '         .help    show help for commands\r\n'
        '         .hook    add custom commands from a python source file\r\n'
        ' .legacymodels    load legacy models\r\n'
        '       .python    evaluate a Python expression\r\n'
        '         .read    read a file of shell commands\r\n'
        '          .sql    execute a SQL query\r\n'
        '        .trace    trace queries\r\n'
        '      .untrace    untrace queries\r\n'
        "Type `.help <cmd>' for help on the command <cmd>.\r\n"
        'bayeslite> '
    )
    assert c.before == ''


def test_dot_csv(spawnbdb):
    c = spawnbdb
    cmd = '.csv dha %s' % (DHA_CSV,)
    c.sendline(cmd)
    c.expect_exact('bayeslite> ')
    def remove_control(s):
        return s.translate(None, ''.join(map(chr, range(32 + 1) + [127])))
    assert remove_control(c.before) == remove_control(cmd)


def test_describe_columns_without_generator(spawntable):
    table, c = spawntable
    c.sendline('.describe columns %s' % (table,))
    c.expect_exact(
        '.describe columns %s\r\n'
        'No such generator: %s\r\n'
        'bayeslite> ' % (table, table)
    )


def test_bql_select(spawntable):
    table, c = spawntable
    query = 'SELECT name FROM %s ORDER BY name ASC LIMIT 5;' % (table,)
    c.sendline(query)
    c.expect_exact(
        '%s\r\n'
        '             NAME\r\n'
        '-----------------\r\n'
        '       Abilene TX\r\n'
        '         Akron OH\r\n'
        'Alameda County CA\r\n'
        '        Albany GA\r\n'
        '        Albany NY\r\n'
        'bayeslite> ' % (query,)
    )
    assert c.before == ''


def test_guess(spawntable):
    table, c = spawntable
    c.sendline('.guess dha_cc %s' % (table,))
    c.expect_exact(
        '.guess dha_cc %s\r\n'
        'bayeslite> ' % (table,)
    )
    assert c.before == ''


def test_sql(spawntable):
    table, c = spawntable
    c.sendline('.sql pragma table_info(bayesdb_column)')
    c.expect_exact(
        '.sql pragma table_info(bayesdb_column)\r\n'
        'cid |        name |    type | notnull | dflt_value | pk\r\n'
        '----+-------------+---------+---------+------------+---\r\n'
        '  0 |     tabname |    TEXT |       1 |       None |  1\r\n'
        '  1 |       colno | INTEGER |       1 |       None |  2\r\n'
        '  2 |        name |    TEXT |       1 |       None |  0\r\n'
        '  3 |   shortname |    TEXT |       0 |       None |  0\r\n'
        '  4 | description |    TEXT |       0 |       None |  0\r\n'
        'bayeslite> '
    )
    assert c.before == ''


def test_describe_column_with_generator(spawngen):
    gen, c = spawngen
    c.sendline('.describe models %s' % (gen,))
    c.expect_exact(
        '.describe models %s\r\n'
        'modelno | iterations\r\n'
        '--------+-----------\r\n'
        'bayeslite> ' % (gen,)
    )
    assert c.before == ''
    c.sendline('.describe columns %s' % (gen,))
    c.expect_exact(
        '.describe columns %s\r\n'
        'colno |                name |  stattype | shortname\r\n'
        '------+---------------------+-----------+----------\r\n'
        '    1 |         N_DEATH_ILL | numerical |      None\r\n'
        '    2 |       TTL_MDCR_SPND | numerical |      None\r\n'
        '    3 |       MDCR_SPND_INP | numerical |      None\r\n'
        '    4 |      MDCR_SPND_OUTP | numerical |      None\r\n'
        '    5 |       MDCR_SPND_LTC | numerical |      None\r\n'
        '    6 |      MDCR_SPND_HOME | numerical |      None\r\n'
        '    7 |      MDCR_SPND_HSPC | numerical |      None\r\n'
        '    8 |    MDCR_SPND_AMBLNC | numerical |      None\r\n'
        '    9 |       MDCR_SPND_EQP | numerical |      None\r\n'
        '   10 |     MDCR_SPND_OTHER | numerical |      None\r\n'
        '   11 |           TTL_PARTB | numerical |      None\r\n'
        '   12 |     PARTB_EVAL_MGMT | numerical |      None\r\n'
        '   13 |         PARTB_PROCS | numerical |      None\r\n'
        '   14 |          PARTB_IMAG | numerical |      None\r\n'
        '   15 |         PARTB_TESTS | numerical |      None\r\n'
        '   16 |         PARTB_OTHER | numerical |      None\r\n'
        '   17 |    HOSP_REIMB_P_DCD | numerical |      None\r\n'
        '   18 |     HOSP_DAYS_P_DCD | numerical |      None\r\n'
        '   19 |    REIMB_P_PTNT_DAY | numerical |      None\r\n'
        '   20 |    HOSP_REIMB_RATIO | numerical |      None\r\n'
        '   21 |      HOSP_DAY_RATIO | numerical |      None\r\n'
        '   22 |   REIMB_P_DAY_RATIO | numerical |      None\r\n'
        '   23 |       MD_PYMT_P_DCD | numerical |      None\r\n'
        '   24 |      MD_VISIT_P_DCD | numerical |      None\r\n'
        '   25 |     PYMT_P_MD_VISIT | numerical |      None\r\n'
        '   26 | MD_VISIT_PYMT_RATIO | numerical |      None\r\n'
        '   27 |      MD_VISIT_RATIO | numerical |      None\r\n'
        '   28 |  PYMT_P_VISIT_RATIO | numerical |      None\r\n'
        '   29 |           HOSP_BEDS | numerical |      None\r\n'
        '   30 |         TTL_IC_BEDS | numerical |      None\r\n'
        '   31 |          HI_IC_BEDS | numerical |      None\r\n'
        '   32 |         INT_IC_BEDS | numerical |      None\r\n'
        '   33 |       MED_SURG_BEDS | numerical |      None\r\n'
        '   34 |            SNF_BEDS | numerical |      None\r\n'
        '   35 |           TOTAL_FTE | numerical |      None\r\n'
        '   36 |              MS_FTE | numerical |      None\r\n'
        '   37 |              PC_FTE | numerical |      None\r\n'
        '   38 |         MS_PC_RATIO | numerical |      None\r\n'
        '   39 |             RNS_REQ | numerical |      None\r\n'
        '   40 |    HOSP_DAYS_P_DCD2 | numerical |      None\r\n'
        '   41 |   TTL_IC_DAYS_P_DCD | numerical |      None\r\n'
        '   42 |    HI_IC_DAYS_P_DCD | numerical |      None\r\n'
        '   43 |   INT_IC_DAYS_P_DCD | numerical |      None\r\n'
        '   44 | MED_SURG_DAYS_P_DCD | numerical |      None\r\n'
        '   45 |      SNF_DAYS_P_DCD | numerical |      None\r\n'
        '   46 |  TTL_MD_VISIT_P_DCD | numerical |      None\r\n'
        '   47 |      MS_VISIT_P_DCD | numerical |      None\r\n'
        '   48 |      PC_VISIT_P_DCD | numerical |      None\r\n'
        '   49 |   MS_PC_RATIO_P_DCD | numerical |      None\r\n'
        '   50 |     HHA_VISIT_P_DCD | numerical |      None\r\n'
        '   51 |       PCT_DTHS_HOSP | numerical |      None\r\n'
        '   52 |      PCT_DTHS_W_ICU | numerical |      None\r\n'
        '   53 |       PCT_DTHS_HSPC | numerical |      None\r\n'
        '   54 |     HSPC_DAYS_P_DCD | numerical |      None\r\n'
        '   55 |      PCT_PTNT_10_MD | numerical |      None\r\n'
        '   56 |          N_MD_P_DCD | numerical |      None\r\n'
        '   57 |     TTL_COPAY_P_DCD | numerical |      None\r\n'
        '   58 |      MD_COPAY_P_DCD | numerical |      None\r\n'
        '   59 |     EQP_COPAY_P_DCD | numerical |      None\r\n'
        '   60 |          QUAL_SCORE | numerical |      None\r\n'
        '   61 |           AMI_SCORE | numerical |      None\r\n'
        '   62 |           CHF_SCORE | numerical |      None\r\n'
        '   63 |         PNEUM_SCORE | numerical |      None\r\n'
        'bayeslite> ' % (gen,)
    )
    assert c.before == ''


def test_hook(spawnbdb):
    c = spawnbdb
    c.sendline('.hook %s' % (THOOKS_PY,))
    c.expect_exact(
        '.hook %s\r\n'
        'added command ".myhook"\r\n'
        'bayeslite> ' % (THOOKS_PY,)
    )
    assert c.before == ''
    c.sendline('.help')
    c.expect_exact(
        '.help\r\n'
        '     .codebook    load codebook for table\r\n'
        '          .csv    create table from CSV file\r\n'
        '     .describe    describe BayesDB entities\r\n'
        '        .guess    guess data generator\r\n'
        '         .help    show help for commands\r\n'
        '         .hook    add custom commands from a python source file\r\n'
        ' .legacymodels    load legacy models\r\n'
        '       .myhook    myhook help string\r\n'
        '       .python    evaluate a Python expression\r\n'
        '         .read    read a file of shell commands\r\n'
        '          .sql    execute a SQL query\r\n'
        '        .trace    trace queries\r\n'
        '      .untrace    untrace queries\r\n'
        "Type `.help <cmd>' for help on the command <cmd>.\r\n"
        'bayeslite> '
    )
    assert c.before == ''
    c.sendline('.help myhook')
    c.expect_exact(
        '.help myhook\r\n'
        '.myhook <string>\r\n'
        'bayeslite> '
    )
    c.sendline('.myhook zoidberg')
    c.expect_exact(
        '.myhook zoidberg\r\n'
        'john zoidberg\r\n'
        'bayeslite> '
    )


def test_read_nonsequential(spawnbdb):
    c = spawnbdb
    with read_data() as fname:
        c.sendline('.read %s' % (fname,))
        c.expect_exact(
            '.read %s\r\n'
            'Usage: .csv <table> </path/to/data.csv>\r\n'
            '      NAME\r\n'
            '----------\r\n'
            'Abilene TX\r\n'
            '  Akron OH\r\n'
            '             NAME\r\n'
            '-----------------\r\n'
            '       Abilene TX\r\n'
            '         Akron OH\r\n'
            'Alameda County CA\r\n'
            '        Albany GA\r\n'
            '        Albany NY\r\n'
            '--DEBUG: .read complete\r\n'
            'bayeslite> ' % (fname,)
        )
        assert c.before == ''


def test_read_nonsequential_verbose(spawnbdb):
    c = spawnbdb
    with read_data() as fname:
        c.sendline('.read %s' % (fname,))
        c.expect_exact(
            '.read %s\r\n'
            'Usage: .csv <table> </path/to/data.csv>\r\n'
            '      NAME\r\n'
            '----------\r\n'
            'Abilene TX\r\n'
            '  Akron OH\r\n'
            '             NAME\r\n'
            '-----------------\r\n'
            '       Abilene TX\r\n'
            '         Akron OH\r\n'
            'Alameda County CA\r\n'
            '        Albany GA\r\n'
            '        Albany NY\r\n'
            '--DEBUG: .read complete\r\n'
            'bayeslite> ' % (fname,)
        )
