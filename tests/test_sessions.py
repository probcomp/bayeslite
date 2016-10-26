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
import json
import pytest

from collections import namedtuple

import bayeslite
from bayeslite.loggers import CaptureLogger
import bayeslite.metamodels.troll_rng as troll
import bayeslite.sessions as sescap
from bayeslite.version import __version__

from bayeslite.util import cursor_value

import test_core

def make_bdb():
    crosscat = test_core.local_crosscat()
    metamodel = test_core.CrosscatMetamodel(crosscat)
    bdb = bayeslite.bayesdb_open(builtin_metamodels=False)
    bayeslite.bayesdb_register_metamodel(bdb, metamodel)
    return bdb

def make_bdb_with_sessions(*args, **kwargs):
    bdb = make_bdb()
    tr = sescap.SessionOrchestrator(bdb, *args, **kwargs)
    return (bdb, tr)

def get_num_sessions(executor):
    return cursor_value(executor('''SELECT COUNT(*) FROM bayesdb_session'''))

def get_session_versions(executor):
    return list(executor('''SELECT version from bayesdb_session'''))

def get_num_entries(executor):
    return cursor_value(executor('''
        SELECT COUNT(*) FROM bayesdb_session_entries
    '''))

def get_entries(executor):
    entries = executor('''
        SELECT * FROM bayesdb_session_entries ORDER BY id
    ''')
    fields = [
        'id',
        'session_id',
        'type',
        'data',
        'start_time',
        'end_time',
        'error',
    ]
    assert fields == [d[0] for d in entries.description]
    SessionEntry = namedtuple('SessionEntry', fields)
    return [SessionEntry(*e) for e in entries]

def _basic_test_trace(executor):

    # a new session is automatically initialized with id 1
    assert get_num_sessions(executor) == 1

    # the above select query counts should become one or more entries
    num_entries = cursor_value(executor('''
        SELECT COUNT(*) FROM bayesdb_session_entries
    '''))
    assert num_entries > 0

    # entries are ordered starting from 1
    for id, entry in enumerate(get_entries(executor)):
        assert entry.session_id == 1
        assert entry.id == id + 1

def test_sessions_basic_bql():
    (bdb, tr) = make_bdb_with_sessions()
    _basic_test_trace(bdb.execute)

def test_sessions_basic_sql():
    (bdb, tr) = make_bdb_with_sessions()
    _basic_test_trace(bdb.sql_execute)

def _simple_bql_query(bdb):
    bdb.execute('''SELECT COUNT(*) FROM bayesdb_session''').fetchall()

def test_sessions_session_id_and_clear_sessions():
    (bdb, tr) = make_bdb_with_sessions()
    _simple_bql_query(bdb)

    # create two more sessions
    tr._start_new_session()
    tr._start_new_session()
    assert tr.current_session_id() == 3
    assert get_num_sessions(bdb.execute) == 3
    versions = get_session_versions(bdb.execute)
    assert 3 == len(versions)
    for version_row in versions:
        assert 1 == len(version_row)
        assert version_row[0] == __version__

    # there should now be one session (the current session)
    tr.clear_all_sessions()
    assert tr.current_session_id() == 1
    assert get_num_sessions(bdb.execute) == 1

    # the entry ids in the current session should start from 1
    assert min(entry.id for entry in get_entries(bdb.execute)) == 1

def test_sessions_start_stop():
    bdb = make_bdb()

    # the session table exists but there should be no entries before we
    # register the session tracer
    assert get_num_sessions(bdb.execute) == 0
    _simple_bql_query(bdb)
    assert get_num_entries(bdb.execute) == 0

    # registering the tracer starts recording of sessions
    tr = sescap.SessionOrchestrator(bdb)
    _simple_bql_query(bdb)
    num = get_num_entries(bdb.execute)
    assert num > 0

    # stopping the tracer
    tr.stop_saving_sessions()
    _simple_bql_query(bdb)
    assert get_num_entries(bdb.execute) == num

    # restarting the tracer
    tr.start_saving_sessions()
    _simple_bql_query(bdb)
    assert get_num_entries(bdb.execute) > num

def test_sessions_json_dump():
    (bdb, tr) = make_bdb_with_sessions()
    _simple_bql_query(bdb)
    tr.stop_saving_sessions()
    json_str = tr.dump_current_session_as_json()
    session = json.loads(json_str)
    assert isinstance(session, dict)
    assert 'version' in session
    assert session['version'] == __version__
    assert 'fields' in session
    assert isinstance(session['fields'], list)
    nfields = len(session['fields'])
    assert 'entries' in session
    assert isinstance(session['entries'], list)
    entries = session['entries']
    assert len(entries) == get_num_entries(bdb.execute)
    for entry in entries:
        assert len(entry) == nfields

def _nonexistent_table_helper(executor, tr):
    query = 'SELECT * FROM nonexistent_table'
    try:
        executor(query)
        assert False
    except apsw.SQLError:
        #tr._start_new_session()
        assert tr._check_error_entries(tr.session_id) > 0

def test_sessions_error_entry_bql():
    (bdb, tr) = make_bdb_with_sessions()
    _nonexistent_table_helper(bdb.execute, tr)

def test_sessions_error_entry_sql():
    (bdb, tr) = make_bdb_with_sessions()
    _nonexistent_table_helper(bdb.sql_execute, tr)

def test_sessions_no_errors():
    with test_core.analyzed_bayesdb_population(test_core.t1(),
            10, None, max_seconds=1) as (bdb, population_id, generator_id):
        tr = sescap.SessionOrchestrator(bdb)
        # simple query
        cursor = bdb.execute('''
            SELECT age, weight FROM t1
                WHERE label = 'frotz'
                ORDER BY weight
        ''')
        cursor.fetchall()
        # add a metamodel and do a query
        cursor = bdb.execute('''
            ESTIMATE PREDICTIVE PROBABILITY OF age FROM p1
        ''')
        cursor.fetchall()
        # there should be no error entries in the previous session
        #tr._start_new_session()
        assert tr._check_error_entries(tr.session_id) == 0

class Boom(Exception): pass
class ErroneousMetamodel(troll.TrollMetamodel):
    def __init__(self):
        self.call_ct = 0
    def name(self): return 'erroneous'
    def logpdf_joint(self, *_args, **_kwargs):
        if self.call_ct > 10: # Wait to avoid raising during sqlite's prefetch
            raise Boom()
        self.call_ct += 1
        return 0

def test_sessions_error_metamodel():
    with test_core.t1() as (bdb, _population_id, _generator_id):
        bayeslite.bayesdb_register_metamodel(bdb, ErroneousMetamodel())
        bdb.execute('DROP GENERATOR p1_cc')
        bdb.execute('''
            CREATE GENERATOR p1_err FOR p1
                USING erroneous(age NUMERICAL)
        ''')
        tr = sescap.SessionOrchestrator(bdb)
        cursor = bdb.execute('''
            ESTIMATE PREDICTIVE PROBABILITY OF age FROM p1
        ''')
        with pytest.raises(Boom):
            cursor.fetchall()
        #tr._start_new_session()
        assert tr._check_error_entries(tr.session_id) > 0

def test_sessions_send_data():
    lgr = CaptureLogger()
    (bdb, tr) = make_bdb_with_sessions(session_logger=lgr)
    _simple_bql_query(bdb)
    tr.send_session_data()
    assert 1 == len(lgr.calls)
    assert "_send" == lgr.calls[0][0]
    assert "SELECT COUNT(*) FROM bayesdb_session" in str(lgr.calls[0][1])

def test_sessions_send_data__ci_network():
    (bdb, tr) = make_bdb_with_sessions()
    _simple_bql_query(bdb)
    tr.send_session_data()

def test_error():
    (bdb, tr) = make_bdb_with_sessions()
    with pytest.raises(apsw.SQLError):
        bdb.execute('select x from nonexistent_table')
    assert 1 == bdb.sql_execute('''
            SELECT COUNT(*) FROM bayesdb_session_entries
                WHERE type = 'bql' AND error LIKE '%no such table%'
        ''').fetchvalue()
    assert 1 == bdb.sql_execute('''
            SELECT COUNT(*) FROM bayesdb_session_entries
                WHERE type = 'sql' AND error LIKE '%no such table%'
        ''').fetchvalue()
    assert 0 == bdb.sql_execute('''
            SELECT COUNT(*) FROM bayesdb_session_entries
                WHERE type = 'bql' AND error IS NULL
        ''').fetchvalue()
    assert 0 == bdb.sql_execute('''
            SELECT COUNT(*) FROM bayesdb_session_entries
                WHERE type = 'bql' AND end_time IS NULL
        ''').fetchvalue()
