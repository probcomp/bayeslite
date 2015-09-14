# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2014, MIT Probabilistic Computing Project
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
database in memory or on disk.  To obtain a BayesDB handle, either
creating one in memory or opening or creating one on disk, use
:func:`bayesdb_open`::

   import bayeslite

   memdb = bayeslite.bayesdb_open()
   filedb = bayeslite.bayesdb_open(pathname='foo.bdb')

When done, close it with the :meth:`~BayesDB.close` method::

   filedb.close()
   memdb.close()

The contents of ``memdb`` will be forgotten when it is closed or when
the Python process exists.  The contents of ``filedb`` will be stored
durably on disk in ``foo.bdb``.

You can execute normal SQL on a BayesDB handle `bdb` with the
:meth:`~BayesDB.sql_execute` method::

   bdb.sql_execute('create table t(x int, y text, z real)')
   bdb.sql_execute("insert into t values(1, 'xyz', 42.5)")
   bdb.sql_execute("insert into t values(1, 'pqr', 83.7)")
   bdb.sql_execute("insert into t values(2, 'xyz', 1000)")

(BQL does not yet support CREATE TABLE and INSERT directly, so you
must use :meth:`~BayesDB.sql_execute` for those.)

To model your data and ask probabilistic BQL queries about it, you
must first register a metamodel, such as the Crosscat metamodel::

   import crosscat.LocalEngine
   import bayeslite.crosscat

   cc = crosscat.LocalEngine.LocalEngine(seed=0)
   ccmm = bayeslite.crosscat.CrosscatMetamodel(cc)
   bayeslite.bayesdb_register_metamodel(bdb, ccmm)

Then you can model a table with Crosscat and query the probable
implications of the data in the table::

   bdb.execute('create generator t_cc for t using crosscat(guess(*))')
   bdb.execute('initialize 10 models for t_cc')
   bdb.execute('analyze t_cc for 10 iterations wait')
   for x in bdb.execute('estimate pairwise dependence probablity from t_cc'):
       print x
"""

from bayeslite.bayesdb import BayesDB
from bayeslite.bayesdb import bayesdb_open
from bayeslite.bqlfn import bayesdb_simulate
from bayeslite.codebook import bayesdb_load_codebook_csv_file
from bayeslite.exception import BayesDBException
from bayeslite.exception import BQLError
from bayeslite.legacy_models import bayesdb_load_legacy_models
from bayeslite.metamodel import IBayesDBMetamodel
from bayeslite.metamodel import bayesdb_deregister_metamodel
from bayeslite.metamodel import bayesdb_register_metamodel
from bayeslite.parse import BQLParseError
from bayeslite.read_csv import bayesdb_read_csv
from bayeslite.read_csv import bayesdb_read_csv_file
from bayeslite.txn import BayesDBTxnError
from bayeslite.version import __version__

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
    'bayesdb_simulate',
    'IBayesDBMetamodel',
    '__version__',
]
