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

import bayeslite

def test_conditional_probability_pathologies():
    data = [
        ['x', 'a'], ['x', 'a'], ['x', 'a'],
        ['y', 'b'], ['y', 'b'], ['y', 'b'],
    ]
    with bayeslite.bayesdb_open() as bdb:
        bdb.sql_execute('create table t(foo, bar)')
        for row in data:
            bdb.sql_execute('insert into t values (?, ?)', row)
        bdb.execute('''
            create generator t_cc for t using crosscat(
                foo categorical,
                bar categorical
            )
        ''')
        bdb.execute('initialize 1 models for t_cc')
        bdb.execute('analyze t_cc for 1 iterations wait')
        assert bdb.execute('''
            estimate probability of foo = 'x' by t_cc
        ''').fetchvalue() < 1
        assert bdb.execute('''
            estimate probability of foo = 'x' given (foo = 'x') by t_cc
        ''').fetchvalue() == 1
        assert bdb.execute('''
            estimate probability of value 'x' given (foo = 'x')
                from columns of t_cc
                where c.name = 'foo'
        ''').fetchvalue() == 1
        assert bdb.execute('''
            estimate probability of foo = 'x' given (foo = 'y') by t_cc
        ''').fetchvalue() == 0
