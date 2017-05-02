# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2017, MIT Probabilistic Computing Project
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

from .sqlite3_util import sqlite3_quote_name


def bql_quote_name(name):
    """Quote `name` as a BQL identifier, e.g. a table or column name.

    Do NOT use this for strings, e.g. inserting data into a table.
    Use query parameters instead.
    """
    return sqlite3_quote_name(name)
