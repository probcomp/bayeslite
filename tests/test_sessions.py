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
import bayeslite.sessions as sescap
import test_core
import json
import sqlite3
from collections import namedtuple

def make_bdb():
    crosscat = test_core.local_crosscat()
    metamodel = test_core.CrosscatMetamodel(crosscat)
    bdb = bayeslite.bayesdb_open(builtin_metamodels=False)
    bayeslite.bayesdb_register_metamodel(bdb, metamodel)
    return bdb

def make_bdb_with_sessions():
    bdb = make_bdb()
    tr = sescap.SessionTracer(bdb)
    return (bdb, tr)

def query_scalar_int(executor, query):
    return int(executor(query).next()[0])

def get_num_sessions(executor):
    return query_scalar_int(executor, '''
        SELECT COUNT(*) FROM bayesdb_session;
    ''')

def get_num_entries(executor):
    return query_scalar_int(executor, '''
        SELECT COUNT(*) FROM bayesdb_session_entries;
    ''')

def get_entries(executor):
    entries = list(executor('''
        SELECT * FROM bayesdb_session_entries ORDER BY id;
    '''))
    SessionEntry = namedtuple('SessionEntry', ['id', 'session', 'time', 'type', 'data'])
    return [SessionEntry(e[0], e[1], e[2], e[3], e[4]) for e in entries]

def _basic_test_trace(executor):

    # a new session is automatically initialized with id 1
    assert get_num_sessions(executor) == 1

    # the above select query counts should become one or more entries
    num_entries = query_scalar_int(executor, '''
        SELECT COUNT(*) FROM bayesdb_session_entries;
    ''')
    assert num_entries > 0

    # entries are ordered starting from 1
    for id, entry in enumerate(get_entries(executor)):
        assert entry.session == 1
        assert entry.id == id + 1

def test_sessions_basic_bql():
    (bdb, tr) = make_bdb_with_sessions()
    _basic_test_trace(bdb.execute)

def test_sessions_basic_sql():
    (bdb, tr) = make_bdb_with_sessions()
    _basic_test_trace(bdb.sql_execute)

def _simple_bql_query(bdb):
    bdb.execute('''
        SELECT COUNT(*) FROM bayesdb_session;
    ''')

def test_sessions_session_id_and_clear_sessions():
    (bdb, tr) = make_bdb_with_sessions()
    _simple_bql_query(bdb)

    # create two more sessions
    tr._start_new_session()
    tr._start_new_session()
    assert tr.current_session_id() == 3
    assert get_num_sessions(bdb.execute) == 3

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
    tr = sescap.SessionTracer(bdb)
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
    entries = json.loads(json_str)
    assert len(entries) == get_num_entries(bdb.execute)

def test_sessions_unfinished_entry():
    (bdb, tr) = make_bdb_with_sessions()
    bql = 'SELECT * FROM nonexistent_table'
    try:
        bdb.execute(bql)
        assert False
    except sqlite3.OperationalError:
        tr._start_new_session()
        # check for unfinished queries in the previous session
        assert tr._check_unfinished_entries() > 0

def test_sessions_send_data():
    (bdb, tr) = make_bdb_with_sessions()
    _simple_bql_query(bdb)
    tr.send_session_data()

test_sessions_basic_bql()
test_sessions_basic_sql()
test_sessions_session_id_and_clear_sessions()
test_sessions_start_stop()
test_sessions_json_dump()
test_sessions_unfinished_entry()
test_sessions_send_data()
