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

"""Probabilistic database built on SQLite 3.

Bayeslite is a probabilistic database built on `SQLite 3
<https://www.sqlite.org/>`__.  In addition to SQL queries on
conventional SQL tables, it supports probabilistic BQL queries on
generative models for data in a table.

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
durably on disk in ``foo.bdb``.  The BayesDB object acts as a context
manager that returns itself on entry and closes itself on exit, so you
can write::

   with bayeslite.bayesdb_open(pathname='foo.bdb') as bdb:
       ...

Given a BayesDB handle ``bdb``, execute BQL queries on it with the
:meth:`~BayesDB.execute` method::

   bdb.execute('create table t(x int, y text, z real)')
"""

from bayeslite.bayesdb import BayesDB
from bayeslite.bayesdb import bayesdb_open
from bayeslite.bqlfn import bayesdb_simulate
from bayeslite.codebook import bayesdb_load_codebook_csv_file
from bayeslite.exception import BayesDBException
from bayeslite.legacy_models import bayesdb_load_legacy_models
from bayeslite.metamodel import IBayesDBMetamodel
from bayeslite.metamodel import bayesdb_deregister_metamodel
from bayeslite.metamodel import bayesdb_register_metamodel
from bayeslite.parse import BQLParseError
from bayeslite.read_csv import bayesdb_read_csv
from bayeslite.read_csv import bayesdb_read_csv_file
from bayeslite.txn import BayesDBTxnError
