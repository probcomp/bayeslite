assert __name__ == '__main__'

import bayeslite
import bayeslite.bql as bql
import bayeslite.core as core
import bayeslite.parse as parse
import crosscat.LocalEngine as localengine
import getopt
import sys

# BEGIN: copypasta
def bql_exec(bdb, string):
    import sys
    print >>sys.stderr, '--> %s' % (string.strip(),)
    phrases = parse.parse_bql_string(string)
    phrase = phrases.next()
    done = None
    try:
        phrases.next()
        done = False
    except StopIteration:
        done = True
    if done is not True:
        raise ValueError('>1 phrase: %s' % (string,))
    return bql.execute_phrase(bdb, phrase)

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

# END: copypasta


def print_cursors(cursors):
    for cursor in cursors:
        for cr in cursor:
            print(cr)


bdb = bayeslite.BayesDB(localengine.LocalEngine(seed=0))

bql_exec(bdb, "create btable if not exists dha from 'dha.csv'")
bql_exec(bdb, 'initialize 10 models if not exists for dha')

if iterations is not None:
    modelspec = 'models %s' % (modelnos,) if modelnos is not None else ''
    bql_exec(bdb, 'analyze dha %s for %d iterations wait' %
        (modelspec, iterations))


# ESTIMATE PAIRWISE DEPENDENCE PROBABILITY FROM dha SAVE TO dha_z.png
queries = [
    'estimate pairwise dependence probability from dha',
    'estimate columns from dha order by dependence probability with MDCR_SPND_AMBLNC limit 10',
    'estimate columns from dha order by dependence probability with MDCR_SPND_AMBLNC limit 10',
    'estimate columns from dha order by dependence probability with QUAL_SCORE limit 10',
    'select NAME, predictive probability of MDCR_SPND_AMBLNC from dha order by predictive probability of MDCR_SPND_AMBLNC asc limit 10',
    'select NAME, predictive probability of QUAL_SCORE from dha order by predictive probability of QUAL_SCORE asc limit 10',
    'select NAME, predictive probability of PYMT_P_MD_VISIT from dha order by predictive probability of PYMT_P_MD_VISIT asc limit 10'
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
