assert __name__ == '__main__'

import bayeslite
import bayeslite.bql as bql
import bayeslite.core as core
import bayeslite.crosscat
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

bdb = bayeslite.bayesdb_open(pathname='satellites.bdb')
bayeslite.bayesdb_register_metamodel(bdb, bayeslite.crosscat.CrosscatMetamodel(localengine.LocalEngine(seed=0)))
try:
    bayeslite.bayesdb_read_csv_file(bdb, 'satellites', 'satellites.csv', header=True, create=True)
except ValueError: # table exists
    pass
else:
    bdb.execute('create default generator satellites_cc for satellites using crosscat (guess(*))')
    bdb.execute('initialize 10 models for satellites')
if iterations is not None:
    modelspec = 'models %s' % (modelnos,) if modelnos is not None else ''
    bdb.execute('analyze satellites %s for %d iterations wait' %
        (modelspec, iterations))
