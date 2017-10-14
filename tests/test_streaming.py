# -*- c_ding: utf-8 -*-

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
import time
import random
import string
import contextlib
import shutil
import tempfile
import os.path

import bayeslite.core as core

from bayeslite import bayesdb_open, bayesdb_register_metamodel
from bayeslite.exception import BQLError
from bayeslite.metamodels.loom_metamodel import LoomMetamodel

PREDICT_RUNS = 100
X_MIN, Y_MIN = 0, 0
X_MAX, Y_MAX = 200, 100

# TODO fix fail when two tests are run with the same prefix
# currently low priority since bql users will use timestamps as prefix


@contextlib.contextmanager
def tempdir(prefix):
    path = tempfile.mkdtemp(prefix=prefix)
    try:
        yield
    finally:
        if os.path.isdir(path):
            shutil.rmtree(path)


def test_basic_stream():
    """Simple test of the LoomMetamodel on a one variable table
    Only checks for errors from the Loom system."""
    from datetime import datetime
    with tempdir('bayeslite-loom') as loom_store_path:
        with bayesdb_open(':memory:') as bdb:
            #bayesdb_register_metamodel(bdb,
            #    LoomMetamodel(loom_store_path=loom_store_path))
            bdb.sql_execute('create table t(x)')
            for x in xrange(10):
                bdb.sql_execute('insert into t(x) values(?)', (x,))
            bdb.execute('create population p for t(x numerical)')
            bdb.execute('create generator g for p')
            bdb.execute('initialize 1 models for g')
            bdb.execute('analyze g for 10 iterations wait')
            bdb.execute('stream estimate probability density of x = ? from p').fetchall()
            #bdb.execute('simulate x from p limit 1').fetchall()
            bdb.execute('drop models from g')
            bdb.execute('drop generator g')
            bdb.execute('drop population p')
            bdb.execute('drop table t')

