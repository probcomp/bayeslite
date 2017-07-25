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

from test_parse import parse_bql_string

import bayeslite.metamodels.cgpm_analyze.parse as cgpm_analyze_parser

# XXX Is there a better way to get the tokens that are supplied to
# cgpm_analyze.parse.parse?
def parse_analysis_plan(string):
    phrases = parse_bql_string('''
        ANALYZE m FOR 1 ITERATION WAIT (%s)
    ''' % (string,))
    return cgpm_analyze_parser.parse(phrases[0].program)

def test_empty():
    assert [] == parse_analysis_plan('')
    assert [] == parse_analysis_plan(';')
    assert [] == parse_analysis_plan(';;')
    assert [] == parse_analysis_plan(' ;')
    assert [] == parse_analysis_plan('; ')
    assert [] == parse_analysis_plan(' ; ')
    assert [] == parse_analysis_plan(' ; ; ')

def test_miscellaneous():
    assert parse_analysis_plan('VARIABLES A, B,  C; OPTIMIZED') == [
        cgpm_analyze_parser.Variables(['A', 'B', 'C']),
        cgpm_analyze_parser.Optimized('lovecat'),
    ]
    assert parse_analysis_plan('SKIP "foo"; loom; QUIET') == [
        cgpm_analyze_parser.Skip(['foo']),
        cgpm_analyze_parser.Optimized('loom'),
        cgpm_analyze_parser.Quiet(True),
    ]
    assert parse_analysis_plan('SKIP "foo"; loom') == [
        cgpm_analyze_parser.Skip(['foo']),
        cgpm_analyze_parser.Optimized('loom'),
    ]

def test_rows():
    assert parse_analysis_plan('ROWS 1, 2, 3, 19;') == [
        cgpm_analyze_parser.Rows([1, 2, 3, 19]),
    ]

def test_inference_planning_basic():
    assert parse_analysis_plan('SUBPROBLEM variable clustering;') == [
        cgpm_analyze_parser.Subproblem(['variable_clustering']),
    ]
    assert parse_analysis_plan('SUBPROBLEM (variable hyperparameters);') == [
        cgpm_analyze_parser.Subproblem(['variable_hyperparameters']),
    ]
    assert parse_analysis_plan('''
        SUBPROBLEM (
            variable clustering concentration,
            variable clustering
        );
    ''' ) == [
        cgpm_analyze_parser.Subproblem([
            'variable_clustering_concentration',
            'variable_clustering'
        ]),
    ]
    assert parse_analysis_plan('''
        SUBPROBLEM row clustering concentration;
        SUBPROBLEM row clustering;
    ''' ) == [
        cgpm_analyze_parser.Subproblem(['row_clustering_concentration']),
        cgpm_analyze_parser.Subproblem(['row_clustering']),
    ]

def test_inference_planning_bonanza():
    assert parse_analysis_plan('''
        VARIABLES foo, bar, llama, salman;
        ROWS 1, 17, 9;
        SUBPROBLEMS (
            row clustering concentration,
            row clustering,
            variable hyperparameters
        );
        OPTIMIZED;
        QUIET;
    ''' ) == [
        cgpm_analyze_parser.Variables(['foo','bar','llama','salman']),
        cgpm_analyze_parser.Rows([1, 17, 9]),
        cgpm_analyze_parser.Subproblem([
            'row_clustering_concentration',
            'row_clustering',
            'variable_hyperparameters',
        ]),
        cgpm_analyze_parser.Optimized('lovecat'),
        cgpm_analyze_parser.Quiet(True),
    ]
