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

import bayeslite
import time
import sys
from __future__ import print_function

class SessionTracer(object):

    def __init__(self, bdb):
        self.bdb = bdb
        self.start_saving_sessions()
        self._start_new_session()
        self._check_unfinished_entries()
        self.sql_exec = self.bdb.sqlite3.execute
        self.out = sys.stderr

    def _print(self, content):
        print(content, self.out)

    def _sql(self, query):
        self._sql(query)

    def _start_new_session(self):
        self._sql('INSERT INTO bayesdb_session DEFAULT VALUES;')
        curs = self._sql('SELECT last_insert_rowid();')
        self.session_id = int(curs.next()[0])

    def _finish(self, entry_id):
        self._sql('''
            UPDATE bayesdb_session_entries
                SET completed=1 WHERE id=?;
        ''', (entry_id,))

    def _trace(self, type, query, bindings):
         '''Save a session entry into the database.'''
        if bindings:
            data += json.dumps(bindings)
        t = time.time()
        self._sql('''
            INSERT INTO bayesdb_session_entries
                (session_id, time, type, data)
                VALUES (?,?,?,?);
        ''', (self.session_id, t, type, data))
        # the entry is initially in the not-completed state. return the new
        # entry's id so that it can be set to completed when appropriate
        curs = self._sql('SELECT last_insert_rowid();')
        entry_id = int(curs.next()[0])
        return lambda : _finish(entry_id)
        # TODO test with returning None

    def _check_unfinished_entries(self):
        '''Check if the previous session ended with a failed command and
        suggest sending the session'''
        cursor = self._sql('''
            SELECT COUNT(*) FROM bayesdb_session_entries
                WHERE completed=0 AND session_id=?;
        ''', (self.session_id-1,))
        uncompleted_entries = int(cursor.next()[0])
        if uncompleted_entries > 0:
            _print('WARNING: Previous session contains uncompleted entries.' +
                    'This may be due to a bad termination or crash of the ' +
                    'previous session. Consider uploading the session.')

    def bql_trace(query, bindings):
        return self._trace("bql", query, bindings)

    def sql_trace(query, bindings):
        return self._trace("sql", query, bindings)

    def clear_all_sessions(self):
        self._sql('DELETE FROM bayesdb_session_entries;')
        self._sql('DELETE FROM bayesdb_session;')
        self._sql('''
            DELETE FROM sqlite_sequence
                WHERE name="bayesdb_session"
                OR name="bayesdb_session_entries";
        ''')
        self._start_new_session()

    def list_sessions(self):
        """Lists all saved sessions with the number of entries in each, and
        whether they were sent or not."""
        return self._sql('SELECT * FROM bayesdb_session;')
    
    def current_session_id(self):
        """Returns the current integer session id."""
        return self.session_id
    
    def dump_session_as_json(self, session_id):
        """Returns a JSON string representing the list of SQL or BQL entries
        (e.g.  queries) executed within session `session_id`."""
        if session_id > self.session_id or session_id < 1:
            raise ValueError('No such session (%d)' % session_id)
        entries = bdb.sql_execute('''
            SELECT * FROM bayesdb_session_entries
                WHERE session_id == ?
                ORDER BY time DESC''', (session_id,))
        return json.dumps(list(entries))
    
    def dump_current_session_as_json(self):
        """Returns a JSON string representing the current sesion (see
        `dump_session_as_json`)"""
        return dump_session_as_json(bdb, self.session_id)
    
    def send_session_data(self):
        """Send all saved session history."""
        probcomp_url = 'http://probcomp.csail.mit.edu/bayesdb/save_sessions.cgi'
        for id in range(1, self.session_id+1):
            print_('Sending session %d to %s ...' % (id, probcomp_url))
            json_string = self.dump_session_as_json(id)
            print_(json_string)
            r = requests.post(probcomp_url,
                    data={'session_json' : json_string})
            print_('Response: %s' % (r.text,))

    def start_saving_sessions(self):
        self.bdb.trace(self.bql_trace)
        self.bdb.sql_trace(self.sql_trace)

    def stop_saving_sessions(self):
        self.bdb.untrace(self.bql_trace)
        self.bdb.sql_untrace(self.sql_trace)

