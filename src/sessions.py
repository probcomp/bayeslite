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

import json
import requests
import sys
import time
import traceback

import bayeslite

from bayeslite import IBayesDBTracer
from bayeslite.util import cursor_value

_error_previous_session_msg = 'WARNING: Current or previous session contains queries that resulted in errors or exceptions. Consider uploading the session with send_session_data().'

class SessionOrchestrator(object):

    def __init__(self, bdb, logger=None, post=None):
        if post is None:
            post = requests.post
        self.bdb = bdb
        self._qid_to_entry_id = {}
        self._sql_tracer = _SessionTracer("sql", self)
        self._bql_tracer = _SessionTracer("bql", self)
        self._post = post
        self.start_saving_sessions()
        self._suggested_send = False
        self._logger = logger
        self._start_new_session()

    def _info(self, msg):
        if self._logger:
            self._logger.info(msg)
        else:
            print msg

    def _warn(self, msg):
        if self._logger:
            self._logger.warn(msg)
        else:
            print msg

    def _error(self, msg):
        if self._logger:
            self._logger.error(msg)
        else:
            print msg

    def _sql(self, query, bindings=None):
        # Go through bdb.sqlite3.execute instead of bdb.sql_execute to
        # avoid hitting the tracer.
        if bindings == None:
            bindings = ()
        return self.bdb.sqlite3.execute(query, bindings)

    def _add_entry(self, qid, type, query, bindings):
        '''Save a session entry into the database. The entry is initially in
        the not-completed state. Return the new entry's id so that it can be
        set to completed when appropriate.'''
        # check for errors on this session and suggest if we haven't already
        if not self._suggested_send:
            self._check_error_entries(self.session_id)
        t = time.time()
        data = query + json.dumps(bindings)
        self._sql('''
            INSERT INTO bayesdb_session_entries
                (session_id, time, type, data)
                VALUES (?,?,?,?)
        ''', (self.session_id, t, type, data))
        entry_id = cursor_value(self._sql('SELECT last_insert_rowid()'))
        self._qid_to_entry_id[qid] = entry_id

    def _mark_entry_completed(self, qid):
        entry_id = self._qid_to_entry_id[qid]
        self._sql('''
            UPDATE bayesdb_session_entries SET completed = 1 WHERE id = ?
        ''', (entry_id,))

    def _mark_entry_error(self, qid):
        entry_id = self._qid_to_entry_id[qid]
        self._sql('''
            UPDATE bayesdb_session_entries SET error = ? WHERE id = ?
        ''', (traceback.format_exc(), entry_id))

    def _start_new_session(self):
        self._sql('INSERT INTO bayesdb_session DEFAULT VALUES')
        self.session_id = cursor_value(self._sql('SELECT last_insert_rowid()'))
        # check for errors on the previous session
        self._check_error_entries(self.session_id - 1)

    def _check_error_entries(self, session_id):
        '''Check if the previous session contains queries that resulted in
        errors and suggest sending the session'''
        error_entries = cursor_value(self._sql('''
            SELECT COUNT(*) FROM bayesdb_session_entries
                WHERE error IS NOT NULL AND session_id = ?
        ''', (session_id,)))
        # suggest sending sessions but don't suggest more than once
        if error_entries > 0 and not self._suggested_send:
            self._warn(_error_previous_session_msg)
            self._suggested_send = True
        return error_entries

    def clear_all_sessions(self):
        self._sql('DELETE FROM bayesdb_session_entries')
        self._sql('DELETE FROM bayesdb_session')
        self._sql('''
            DELETE FROM sqlite_sequence
                WHERE name = 'bayesdb_session'
                OR name = 'bayesdb_session_entries'
        ''')
        self._start_new_session()

    def list_sessions(self):
        """Lists all saved sessions with the number of entries in each, and
        whether they were sent or not."""
        return self._sql('SELECT * FROM bayesdb_session')

    def current_session_id(self):
        """Returns the current integer session id."""
        return self.session_id

    def dump_session_as_json(self, session_id):
        """Returns a JSON string representing the list of SQL or BQL entries
        (e.g.  queries) executed within session `session_id`."""
        if session_id > self.session_id or session_id < 1:
            raise ValueError('No such session (%d)' % session_id)
        entries = self._sql('''
            SELECT * FROM bayesdb_session_entries
                WHERE session_id = ?
                ORDER BY time DESC
        ''', (session_id,))
        return json.dumps(list(entries))

    def dump_current_session_as_json(self):
        """Returns a JSON string representing the current sesion (see
        `dump_session_as_json`)"""
        return self.dump_session_as_json(self.session_id)

    def send_session_data(self):
        """Send all saved session history. The session history will be used for
        research purposes."""
        probcomp_url = 'http://probcomp.csail.mit.edu/bayesdb/save_sessions.cgi'
        for id in range(1, self.session_id+1):
            self._info('Sending session %d to %s ...' % (id, probcomp_url))
            json_string = self.dump_session_as_json(id)
            self._info(json_string)
            r = self._post(probcomp_url,
                    data={'session_json' : json_string})
            self._info('Response: %s' % (r.text,))

    def start_saving_sessions(self):
        self.bdb.trace(self._bql_tracer)
        self.bdb.sql_trace(self._sql_tracer)

    def stop_saving_sessions(self):
        self.bdb.untrace(self._bql_tracer)
        self.bdb.sql_untrace(self._sql_tracer)

class _SessionTracer(IBayesDBTracer):

    def __init__(self, type, orchestrator):
        self._type = type
        self._orchestrator = orchestrator

    def start(self, qid, query, bindings):
        self._orchestrator._add_entry(qid, self._type, query, bindings)

    def finished(self, qid):
        # TODO: currently appears unreliable, error is being used instead
        self._orchestrator._mark_entry_completed(qid)

    def error(self, qid, e):
        self._orchestrator._mark_entry_error(qid)
