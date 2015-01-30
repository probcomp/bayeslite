assert __name__ == '__main__'

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

bdb = core.BayesDB(localengine.LocalEngine(seed=0), pathname='satellites.bdb')
bdb.execute("create btable if not exists satellites" +
    " from 'satellites.utf8.csv'")
bdb.execute('initialize 10 models if not exists for satellites')
if iterations is not None:
    modelspec = 'models %s' % (modelnos,) if modelnos is not None else ''
    bdb.execute('analyze satellites %s for %d iterations wait' %
        (modelspec, iterations))
