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


import pytest

import bayeslite
import bayeslite.ast

from test_parse import parse_bql_string

import bayeslite.metamodels.cgpm_alter.parse as cgpm_alter_parser


# XXX Is there a better way to get the tokens that are supplied to
# cgpm_alter.parse.parse?
def parse_alter_cmds(string):
    phrases = parse_bql_string('''
        ALTER GENERATOR g0 %s
    ''' % (string,))
    altergen = phrases[0]
    assert isinstance(altergen, bayeslite.ast.AlterGen)
    for command in altergen.commands:
        assert isinstance(command, bayeslite.ast.AlterGenGeneric)
    cmds_generic = [cmd.command for cmd in altergen.commands]
    return cgpm_alter_parser.parse(cmds_generic)

def test_empty():
    assert [] == parse_alter_cmds('')
    assert [] == parse_alter_cmds(',')
    assert [] == parse_alter_cmds(';;')
    assert [] == parse_alter_cmds(' ,')
    assert [] == parse_alter_cmds(',; ')
    assert [] == parse_alter_cmds(' ; ')
    assert [] == parse_alter_cmds(' ; ; ')

def test_basic():
    assert parse_alter_cmds('ensure variables (foo, bar, baz) independent') == \
        [cgpm_alter_parser.SetVarCluster(
            ['foo', 'bar', 'baz'],
            cgpm_alter_parser.EnsureIndependent()
        )]
    assert parse_alter_cmds('ensure variables (foo, bar, baz) dependent') == \
        [cgpm_alter_parser.SetVarCluster(
            ['foo', 'bar', 'baz'],
            cgpm_alter_parser.EnsureDependent()
        )]
    assert parse_alter_cmds('ensure variables * dependent') == \
        [cgpm_alter_parser.SetVarCluster(
            cgpm_alter_parser.SqlAll(),
            cgpm_alter_parser.EnsureDependent()
        )]
    with pytest.raises(bayeslite.BQLParseError):
        # Cannot parenthesize *.
        parse_alter_cmds('ensure variables (*) dependent')
    assert parse_alter_cmds('ensure variables a in view of d;') == [
        cgpm_alter_parser.SetVarCluster(['a'], 'd')
    ]
    assert parse_alter_cmds('ensure variables * in view of d;') == [
        cgpm_alter_parser.SetVarCluster(cgpm_alter_parser.SqlAll(), 'd')
    ]
    assert parse_alter_cmds('ensure variables (a, b) in view of d;') == [
        cgpm_alter_parser.SetVarCluster(['a', 'b'], 'd')
    ]
    assert parse_alter_cmds('ensure variable a in singleton view;') == [
        cgpm_alter_parser.SetVarCluster(
            ['a'], cgpm_alter_parser.SingletonCluster())
    ]
    assert parse_alter_cmds('ensure variables ("a", b) in singleton view;') == [
        cgpm_alter_parser.SetVarCluster(
            ['a', 'b'], cgpm_alter_parser.SingletonCluster())
    ]
    assert parse_alter_cmds('ensure variables * in singleton view;') == [
        cgpm_alter_parser.SetVarCluster(
            cgpm_alter_parser.SqlAll(), cgpm_alter_parser.SingletonCluster())
    ]
    assert parse_alter_cmds('set view concentration parameter to 1.12;') == [
        cgpm_alter_parser.SetVarClusterConc(1.12)
    ]
    assert parse_alter_cmds('''
        ensure rows * in cluster of row 1 within view of bar
    ''') == [
        cgpm_alter_parser.SetRowCluster(cgpm_alter_parser.SqlAll(), 1, 'bar')
    ]
    assert parse_alter_cmds('''
        ensure row 3 in cluster of row 1 within view of bar
    ''') == [
        cgpm_alter_parser.SetRowCluster([3], 1, 'bar')
    ]
    assert parse_alter_cmds('''
        ensure rows (1, 2) in cluster of row 1 within view of bar
    ''') == [
        cgpm_alter_parser.SetRowCluster([1, 2], 1, 'bar')
    ]
    assert parse_alter_cmds('''
        ensure rows (1, 2) in singleton cluster within view of quux
    ''') == [
        cgpm_alter_parser.SetRowCluster(
            [1, 2], cgpm_alter_parser.SingletonCluster(), 'quux')
    ]
    assert parse_alter_cmds('''
        ensure rows * in singleton cluster within view of quagga
    ''') == [
        cgpm_alter_parser.SetRowCluster(
            cgpm_alter_parser.SqlAll(),
            cgpm_alter_parser.SingletonCluster(),
            'quagga')
    ]
    assert parse_alter_cmds('''
        set row cluster concentration parameter within view of eland to 12
    ''') == [
        cgpm_alter_parser.SetRowClusterConc('eland', 12)
    ]
