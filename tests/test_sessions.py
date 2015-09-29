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

import pytest
import bayeslite
import bayeslite.sessions as ss
import test_core
import json
import sqlite3

def make_bdb():
    crosscat = test_core.local_crosscat()
    metamodel = test_core.CrosscatMetamodel(crosscat)
    bdb = bayeslite.bayesdb_open(builtin_metamodels=False)
    bayeslite.bayesdb_register_metamodel(bdb, metamodel)
    return bdb

def make_bdb_with_sessions():
    bdb = make_bdb()
    t = ss.SessionTracer(bdb)
    return (bdb, t)

def get_id(entry):
    return entry[0]

def get_session(entry):
    return entry[1]

def get_time(entry):
    return entry[2]

def get_type(entry):
    return entry[3]

def get_data(entry):
    return entry[4]

def get_num_sessions(executor):
    return int(executor('''
        SELECT COUNT(*) FROM bayesdb_session;
    ''').next()[0])

def get_num_entries(executor):
    return int(executor('''
        SELECT COUNT(*) FROM bayesdb_session_entries;
    ''').next()[0])

def get_entries(executor):
    return list(executor('''
        SELECT * FROM bayesdb_session_entries ORDER BY id;
    '''))

def _basic_test_trace(executor):

    # a new session is automatically initialized with id 1
    assert get_num_sessions(executor) == 1

    # the above select query counts should become one or more entries
    num_entries = int(executor('''
        SELECT COUNT(*) FROM bayesdb_session_entries;
    ''').next()[0])
    assert num_entries > 0

    # entries are ordered starting from 1
    for id, entry in enumerate(get_entries(executor)):
        assert get_session(entry) == 1
        assert get_id(entry) == id + 1

def test_sessions_basic_bql():
    bdb, t = make_bdb_with_sessions()
    _basic_test_trace(bdb.execute)

def test_sessions_basic_sql():
    bdb, t = make_bdb_with_sessions()
    _basic_test_trace(bdb.sql_execute)

def _simple_bql_query(bdb):
    bdb.execute('''
        SELECT COUNT(*) FROM bayesdb_session;
    ''')

def test_sessions_session_id_and_clear_sessions():
    bdb, t = make_bdb_with_sessions()
    _simple_bql_query(bdb)

    # create two more sessions
    t._start_new_session()
    t._start_new_session()
    assert t.current_session_id() == 3
    assert get_num_sessions(bdb.execute) == 3

    # there should now be one session (the current session)
    t.clear_all_sessions()
    assert t.current_session_id() == 1
    assert get_num_sessions(bdb.execute) == 1

    # the entry ids in the current session should start from 1
    assert min(map(get_id, get_entries(bdb.execute))) == 1

def test_sessions_start_stop():
    bdb = make_bdb()

    # the session table exists but there should be no entries before we
    # register the session tracer
    assert get_num_sessions(bdb.execute) == 0
    _simple_bql_query(bdb)
    assert get_num_entries(bdb.execute) == 0

    # registering the tracer starts recording of sessions
    t = ss.SessionTracer(bdb)
    _simple_bql_query(bdb)
    num = get_num_entries(bdb.execute)
    assert num > 0

    # stopping the tracer
    t.stop_saving_sessions()
    _simple_bql_query(bdb)
    assert get_num_entries(bdb.execute) == num

    # restarting the tracer
    t.start_saving_sessions()
    _simple_bql_query(bdb)
    assert get_num_entries(bdb.execute) > num

def test_sessions_json_dump():
    bdb, t = make_bdb_with_sessions()
    _simple_bql_query(bdb)
    t.stop_saving_sessions()
    json_str = t.dump_current_session_as_json()
    entries = json.loads(json_str)
    assert len(entries) == get_num_entries(bdb.execute)

def test_sessions_unfinished_entry():
    bdb, t = make_bdb_with_sessions()
    bql = 'SELECT * FROM nonexistent_table'
    try:
        bdb.execute(bql)
        assert False
    except sqlite3.OperationalError:
        t._start_new_session()
        # check for unfinished queries in the previous session
        assert t._check_unfinished_entries() > 0

@pytest.mark.skipif(True, reason="Not test network session upload")
def test_sessions_send_data():
    bdb, t = make_bdb_with_sessions()
    _simple_bql_query(bdb)
    t.send_session_data()
