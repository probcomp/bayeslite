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

import StringIO

import bayeslite.bql as bql
import bayeslite.parse as parse

import test_smoke

def bql2sql(string):
    with test_smoke.t1() as bdb:
        phrases = parse.parse_bql_string(string)
        out = StringIO.StringIO()
        bql.compile_bql(bdb, phrases, out)
        return out.getvalue()

def test_select_trivial():
    assert bql2sql('select 0;') == 'select 0;'
