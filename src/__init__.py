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
"""

from bayeslite.bayesdb import BayesDB
from bayeslite.bayesdb import bayesdb_open
from bayeslite.bayesdb import IBayesDBTracer
from bayeslite.codebook import bayesdb_load_codebook_csv_file
from bayeslite.exception import BayesDBException
from bayeslite.exception import BQLError
from bayeslite.metamodel import IBayesDBMetamodel
from bayeslite.metamodel import bayesdb_builtin_metamodel
from bayeslite.metamodel import bayesdb_deregister_metamodel
from bayeslite.metamodel import bayesdb_register_metamodel
from bayeslite.nullify import bayesdb_nullify
from bayeslite.parse import BQLParseError
from bayeslite.quote import bql_quote_name
from bayeslite.read_csv import bayesdb_read_csv
from bayeslite.read_csv import bayesdb_read_csv_file
from bayeslite.schema import bayesdb_upgrade_schema
from bayeslite.txn import BayesDBTxnError
from bayeslite.version import __version__

# XXX This is not a good place for me.  Find me a better home, please!

__all__ = [
    'BQLError',
    'BQLParseError',
    'BayesDB',
    'BayesDBException',
    'BayesDBTxnError',
    'bayesdb_deregister_metamodel',
    'bayesdb_load_codebook_csv_file',
    'bayesdb_nullify',
    'bayesdb_open',
    'bayesdb_read_csv',
    'bayesdb_read_csv_file',
    'bayesdb_register_metamodel',
    'bayesdb_upgrade_schema',
    'bql_quote_name',
    'IBayesDBMetamodel',
    'IBayesDBTracer',
]

# Register cgpm as a builtin metamodel.
from bayeslite.metamodels.cgpm_metamodel import CGPM_Metamodel
bayesdb_builtin_metamodel(CGPM_Metamodel({}, multiprocess=True))
