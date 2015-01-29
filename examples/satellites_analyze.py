assert __name__ == '__main__'

import bayeslite.bql as bql
import bayeslite.core as core
import bayeslite.parse as parse
import crosscat.LocalEngine as localengine
import getopt
import sys

# XXX This is wrong -- should be part of bayesdb proper.  But it, and
# copypasta of it, will do for now until internals are restructured
# well enough for bdb.execute to work.
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

bdb = core.BayesDB(localengine.LocalEngine(seed=0), pathname='satellites.bdb')
bql_exec(bdb, "create btable if not exists satellites" +
    " from 'satellites.utf8.csv'")
bql_exec(bdb, 'initialize 10 models if not exists for satellites')
if iterations is not None:
    modelspec = 'models %s' % (modelnos,) if modelnos is not None else ''
    bql_exec(bdb, 'analyze satellites %s for %d iterations wait' %
        (modelspec, iterations))
