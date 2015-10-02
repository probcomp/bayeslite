import bayeslite
import bayeslite.sessions as ss
import pandas as pd

# Load the satellites snapshot into a local instance of bayeslite

bdb = bayeslite.bayesdb_open(pathname='satellites.bdb')
t = ss.SessionTracer(bdb)

def todf(cursor):
    df = pd.DataFrame.from_records(cursor, coerce_float=True)
    if not df.empty:
        df.columns = [desc[0] for desc in cursor.description]
        for col in df.columns:
            try:
                df[col] = df[col].astype(float)
            except ValueError:
                pass
    return df

# Define a utility procedure to conveniently query satellites_bdb
def q(query_string):
    res = bdb.execute(query_string)
    return todf(res)

#print t.clear_all_sessions()

#print q("SELECT * FROM satellites WHERE Name LIKE 'International Space Station%'").transpose()

#print q("select * from bayesdb_session;")
#print 'session 7'
#print q("select * from bayesdb_session_entries where session_id=7;")
#print 'session 8'
#print q("select * from bayesdb_session_entries where session_id=8;")
#print 'session 9'
#print q("select * from bayesdb_session_entries where session_id=9;")

#print 'current session: ', bql_utils.current_session_id(satellites_bdb)
#print q('SELECT * FROM bayesdb_session_entries ORDER BY time DESC')
#exit()
#q('drop table unlikely_periods')

#q('''CREATE TEMP TABLE unlikely_periods AS ESTIMATE name, class_of_orbit, period_minutes,
#PREDICTIVE PROBABILITY OF period_minutes AS "Relative Probability of Period"
#FROM satellites_cc;
#''')


print 'list sessions'
print todf(t.list_sessions())

print 'current session id'
print t.current_session_id()

print 'dump session as json 1'
print t.dump_session_as_json(1)

print 'dump current session as json'
print t.dump_current_session_as_json()

print 'dump session 2 as json'
print t.dump_session_as_json(2)

print 'select all entries'
print q('select * from bayesdb_session_entries;')

print 'list all sessions'
print todf(t.list_sessions())

print 'list all entries'
print todf(bdb.sql_execute('select id,session_id from bayesdb_session_entries;'))

print 'stop saving session entries'
t.stop_saving_sessions()

print 'doing some stuff'
q('select * from bayesdb_session_entries where completed=0 order by time')
q('select * from bayesdb_session_entries where completed=0 order by time')
q('select * from bayesdb_session_entries where completed=0 order by time')

print 'list all entries (should be no change)'
print todf(bdb.sql_execute('select id,session_id from bayesdb_session_entries;'))

print 'start saving session entries'
t.start_saving_sessions()

print 'doing some stuff'
q('select * from bayesdb_session_entries where completed=0 order by time')
q('select * from bayesdb_session_entries where completed=0 order by time')
q('select * from bayesdb_session_entries where completed=0 order by time')

print 'list all entries (shoud be more stuff)'
print todf(bdb.sql_execute('select id,session_id from bayesdb_session_entries;'))

print 'stop saving session entries'
t.stop_saving_sessions()


print 'send out the sessions to the servser'
t.send_session_data()
