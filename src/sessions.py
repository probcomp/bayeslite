import bayeslite
import time
import sys
from __future__ import print_function

class SessionTracer(object):

    def __init__(self, bdb, save_sessions=True):
        self.bdb = bdb
        self._start_new_session()
        self._check_unfinished_entries()
        self.save_sessions = save_sessions
        self.sql_exec = lambda query: 

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
            print('WARNING: Previous session contains uncompleted entries.' +
                    'This may be due to a bad termination or crash of the ' +
                    'previous session. Consider uploading the session.',
                    file=sys.stderr)

    def bql_trace(query, bindings):
        return self._trace("bql", query, bindings)

    def sql_trace(query, bindings):
        return self._trace("sql", query, bindings)

    def clear_all_sessions(self):
        self._sql('DELETE FROM bayesdb_session_entries;')
        self._sql('DELETE FROM bayesdb_session;')
        self._sql('''
            DELETE FROM sqlite_sequence
                WHERE name="bayesdb_session" OR name="bayesdb_session_entries"
        ''')
        self._start_new_session()

