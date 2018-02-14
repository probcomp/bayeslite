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

import itertools

import apsw
import pytest

from bayeslite import bayesdb_open
from bayeslite import bqlmath

from bayeslite.math_util import abserr
from bayeslite.util import cursor_value


def get_python_math_call(name, probe):
    func = bqlmath.bqlmath_funcs[name]
    if isinstance(probe, tuple):
        return func(*probe)
    else:
        return func(probe)

def get_sql_math_call(name, probe):
    if isinstance(probe, tuple):
        return 'SELECT %s%s' % (name, str(probe))
    else:
        return 'SELECT %s(%s)' % (name, probe)

PROBES_FLOAT = [-2.5, -1, -0.1, 0, 0.1, 1, 2.5]
PROBES_TUPLE = itertools.combinations(PROBES_FLOAT, 2)
PROBES = itertools.chain(PROBES_FLOAT, PROBES_TUPLE)
FUNCS = bqlmath.bqlmath_funcs.iterkeys()

@pytest.mark.parametrize('name,probe', itertools.product(FUNCS, PROBES))
def test_math_func_one_param(name, probe):
    # Retrieve result from python.
    python_value_error = None
    python_type_error = None
    try:
        result_python = get_python_math_call(name, probe)
    except ValueError:
        python_value_error = True
    except TypeError:
        python_type_error = True

    # Retrieve result from SQL.
    sql_value_error = None
    sql_type_error = None
    try:
        with bayesdb_open(':memory:') as bdb:
            cursor = bdb.execute(get_sql_math_call(name, probe))
            result_sql = cursor_value(cursor)
    except ValueError:
        sql_value_error = True
    except (TypeError, apsw.SQLError):
        sql_type_error = True

    # Domain error on both.
    if python_value_error or sql_value_error:
        assert python_value_error and sql_value_error
    # Arity error on both.
    elif python_type_error or sql_type_error:
        assert python_type_error and sql_type_error
    # Both invocations succeeded, confirm results match.
    else:
        assert abserr(result_python, result_sql) < 1e-4
