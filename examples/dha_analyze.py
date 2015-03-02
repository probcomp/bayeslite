import bayeslite
import bayeslite.bql as bql
import bayeslite.core as core
import bayeslite.parse as parse
import crosscat.LocalEngine as localengine
from prettytable import PrettyTable
import getopt
import sys


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

def try_query(query):
    c =  bdb.execute(query)
    print_cursors(c)
        
def print_cursors(curs):
    col_names = [cn[0] for cn in curs.description]
    rows = curs.fetchall()
    pt = PrettyTable()
    for col_num, col_name in enumerate(col_names):
        pt.add_column(col_name, [row[col_num] for row in rows])
    print(pt)

model_file = 'dha_models.pkl.gz'
table_name = 'dh_test'


# bayeslite
bdb = bayeslite.BayesDB()
bayeslite.bayesdb_register_metamodel(bdb, 'crosscat', localengine.LocalEngine(seed=0))
bayeslite.bayesdb_set_default_metamodel(bdb, 'crosscat')
bayeslite.bayesdb_import_csv_file(bdb, table_name, 'dha.csv')
bayeslite.bayesdb_load_legacy_models(bdb, table_name, model_file)

try_query('estimate columns from {} order by dependence probability with MDCR_SPND_AMBLNC DESC limit 10'.format(table_name))
try_query('estimate columns from {} order by dependence probability with QUAL_SCORE limit 10'.format(table_name))