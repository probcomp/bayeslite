# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2016, MIT Probabilistic Computing Project
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

"""Main bayeslite API.

The focus of the bayeslite API is the *BayesDB*, a handle for a
database.  To obtain a BayesDB handle, use :func:`bayesdb_open`::

    import bayeslite

    bdb = bayeslite.bayesdb_open(pathname='foo.bdb')

When done, close it with the :meth:`~BayesDB.close` method::

    bdb.close()

BayesDB handles also serve as context managers, so you can do::

    with bayeslite.bayesdb_open(pathname='foo.bdb') as bdb:
        bdb.execute('SELECT 42')
        ...

You can query the probable (according to the analyses stored in
the database) implications of the data by passing BQL queries
to the :meth:`~BayesDB.execute` method::

    bql = 'ESTIMATE DEPENDENCE PROBABILITY FROM PAIRWISE COLUMNS OF foo'
    for x in bdb.execute(bql):
       print x

You can also execute normal SQL on a BayesDB handle `bdb` with the
:meth:`~BayesDB.sql_execute` method::

    bdb.sql_execute('CREATE TABLE t(x INT, y TEXT, z REAL)')
    bdb.sql_execute("INSERT INTO t VALUES(1, 'xyz', 42.5)")
    bdb.sql_execute("INSERT INTO t VALUES(1, 'pqr', 83.7)")
    bdb.sql_execute("INSERT INTO t VALUES(2, 'xyz', 1000)")

(BQL does not yet support ``CREATE TABLE`` and ``INSERT`` directly, so
you must use :meth:`~BayesDB.sql_execute` for those.)

When imported, the :mod:`bayeslite` module will notify the MIT
Probabilistic Computing Project over the internet of the software
version you are using, and warn if it is out-of-date.  To disable
this, set the environment variable ``BAYESDB_DISABLE_VERSION_CHECK``
before import, such as with::

    import os
    os.environ['BAYESDB_DISABLE_VERSION_CHECK'] = '1'
    import bayeslite

If you would like to analyze your own data with BayesDB, please
contact bayesdb@mit.edu to participate in our research project.
"""

from bayeslite.bayesdb import BayesDB
from bayeslite.bayesdb import bayesdb_open
from bayeslite.bayesdb import IBayesDBTracer
from bayeslite.codebook import bayesdb_load_codebook_csv_file
from bayeslite.exception import BayesDBException
from bayeslite.exception import BQLError
from bayeslite.legacy_models import bayesdb_load_legacy_models
from bayeslite.metamodel import IBayesDBMetamodel
from bayeslite.metamodel import bayesdb_builtin_metamodel
from bayeslite.metamodel import bayesdb_deregister_metamodel
from bayeslite.metamodel import bayesdb_register_metamodel
from bayeslite.parse import BQLParseError
from bayeslite.read_csv import bayesdb_read_csv
from bayeslite.read_csv import bayesdb_read_csv_file
from bayeslite.schema import bayesdb_upgrade_schema
from bayeslite.sqlite3_util import sqlite3_quote_name
from bayeslite.txn import BayesDBTxnError
from bayeslite.version import __version__

# XXX This is not a good place for me.  Find me a better home, please!
def bql_quote_name(name):
    """Quote `name` as a BQL identifier, e.g. a table or column name.

    Do NOT use this for strings, e.g. inserting data into a table.
    Use query parameters instead.
    """
    return sqlite3_quote_name(name)

__all__ = [
    'BQLError',
    'BQLParseError',
    'BayesDB',
    'BayesDBException',
    'BayesDBTxnError',
    'bayesdb_deregister_metamodel',
    'bayesdb_load_codebook_csv_file',
    'bayesdb_load_legacy_models',
    'bayesdb_open',
    'bayesdb_read_csv',
    'bayesdb_read_csv_file',
    'bayesdb_register_metamodel',
    'bayesdb_upgrade_schema',
    'bql_quote_name',
    'IBayesDBMetamodel',
    'IBayesDBTracer',
]

from bayeslite.metamodels.crosscat import CrosscatMetamodel
from crosscat.LocalEngine import LocalEngine as CrosscatLocalEngine

bayesdb_builtin_metamodel(CrosscatMetamodel(CrosscatLocalEngine(seed=0)))

import bayeslite.remote
import os
if not 'BAYESDB_DISABLE_VERSION_CHECK' in os.environ:
    bayeslite.remote.version_check()

# Notebooks should contain comment lines documenting this behavior and
# offering a solution, like so:
# Please keep BayesDB up to date. To disable remote version checking:
# import os; os.environ['BAYESDB_DISABLE_VERSION_CHECK'] = '1'
