assert __name__ == '__main__'

import bayeslite
import bayeslite.bql as bql
import bayeslite.core as core
import bayeslite.parse as parse
import crosscat.LocalEngine as localengine
import getopt
import sys

def usage():
    print >>sys.stderr, 'Usage: %s [-hv] [-i <iter>] [-m <models>]' % \
        (sys.argv[0])

iterations = None
modelnos = None
try:
    opts, args = getopt.getopt(sys.argv[1:], '?hi:m:', [])
except getopt.GetoptError as e:
    print str(e)
    usage()
if 0 < len(args):
    usage()
for o, a in opts:
    if o in ('-h', '-?'):
        usage()
        sys.exit()
    elif o == '-i':
        iterations = int(a)
    elif o == '-m':
        modelnos = a
    else:
        assert False, 'bad option %s' % (o,)


def print_cursors(cursors):
    for cursor in cursors:
        for cr in cursor:
            print(cr)


bdb = bayeslite.BayesDB(localengine.LocalEngine(seed=0), pathname='satellites.bdb')
bdb.execute("create btable if not exists SATELLITES" +
    " from 'satellites.utf8.csv'")
bdb.execute('initialize 10 models if not exists for SATELLITES')
if iterations is not None:
    modelspec = 'models %s' % (modelnos,) if modelnos is not None else ''
    bdb.execute('analyze SATELLITES %s for %d iterations wait' %
        (modelspec, iterations))


# TODO add schema updates once schema is cleared up
queries = [
    'estimate pairwise dependence probability from satellites',
    'summarize select EXPECTED_LIFETIME from satellites where CLASS_OF_ORBIT = "LEO"',
    'summarize select EXPECTED_LIFETIME from satellites where CLASS_OF_ORBIT = "GEO"',
    'select COUNTRY_OF_OPERATOR, CLASS_OF_ORBIT, EXPECTED_LIFETIME from SATELLITES order by predictive probability of EXPECTED_LIFETIME asc limit 10',
    'select CLASS_OF_ORBIT, PERIOD_MINUTES, predictive probability of PERIOD_MINUTES from SATELLITES where CLASS_OF_ORBIT = "GEO" order by predictive probability of PERIOD_MINUTES asc limit 5',
    'select CLASS_OF_ORBIT, PERIOD_MINUTES, predictive probability of CLASS_OF_ORBIT from SATELLITES order by predictive probability of CLASS_OF_ORBIT asc limit 5',
    'freq select CLASS_OF_ORBIT from SATELLITES where PERIOD_MINUTES < 200',
    'freq simulate USERS from SATELLITES times 100 given EXPECTED_LIFETIME = 20',
    'freq simulate USERS from SATELLITES times 100 given EXPECTED_LIFETIME = 5',
    'freq simulate USERS from SATELLITES times 100 given CLASS_OF_ORBIT = "GEO"',
    'freq simulate USERS from SATELLITES times 100 given CLASS_OF_ORBIT = "LEO"',
    'estimate pairwise row similarity with respect to USERS, PURPOSE from SATELLITES save clusters with threshold 0.75 as user_purpose_cluster',  # ignore save to
    'show row lists for SATELLITES',
    'freq select USERS from SATELLITES where key in user_purpose_cluster_1',
    'freq select PURPOSE from SATELLITES where key in user_purpose_cluster_1',
    'freq select USERS from SATELLITES where key in user_purpose_cluster_0',
    'freq select PURPOSE from SATELLITES where key in user_purpose_cluster_0',
]

for query in queries:
    try:
        c =  bdb.execute(query)
    except Exception as err:
        print('FAIL:{}'.format(query))
        print(err.message)
    else:
        print('SUCCESS:{}'.format(query))
        print_cursors(c)