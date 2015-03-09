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

import bayeslite.metamodel as metamodel

from bayeslite.sqlite3_util import sqlite3_quote_name
from bayeslite.util import casefold

def bayesdb_crosscat_install(bdb, crosscat):
    engine = CrosscatEngine(crosscat)
    metamodel.bayesdb_register_metamodel(bdb, 'crosscat', engine)
    return engine

class CrosscatEngine(metamodel.IMetamodelEngine):
    def __init__(self, crosscat):
        self._crosscat = crosscat

    def register(self, _bdb, _name):
        pass

    def create_metadata(self, bdb, table, column_names, column_types):
        ncols = len(column_names)
        assert ncols == len(column_types)
        # Weird contortions to ignore case distinctions in
        # column_names and the keys of column_types.
        column_positions = dict((casefold(name), i)
            for i, name in enumerate(column_names))
        column_metadata = [None] * ncols
        for name in column_types:
            metadata_creator = self._metadata_creators[column_types[name]]
            metadata = metadata_creator(self, bdb, table, name)
            column_metadata[column_positions[casefold(name)]] = metadata
        assert all(metadata is not None for metadata in column_metadata)
        return {
            'name_to_idx': dict(zip(map(casefold, column_names), range(ncols))),
            'idx_to_name': dict(zip(map(unicode, range(ncols)), column_names)),
            'column_metadata': column_metadata,
        }

    def _create_metadata_numerical(self, _bdb, _table, _column_name):
        return {
            'modeltype': 'normal_inverse_gamma',
            'value_to_code': {},
            'code_to_value': {},
        }

    def _create_metadata_cyclic(self, _bdb, _table, _column_name):
        return {
            'modeltype': 'vonmises',
            'value_to_code': {},
            'code_to_value': {},
        }

    def _create_metadata_ignore(self, bdb, table, column_name):
        metadata = self._create_metadata_categorical(bdb, table, column_name)
        metadata['modeltype'] = 'ignore'
        return metadata

    def _create_metadata_key(self, bdb, table, column_name):
        metadata = self._create_metadata_categorical(bdb, table, column_name)
        metadata['modeltype'] = 'key'
        return metadata

    def _create_metadata_categorical(self, bdb, table, column_name):
        qcn = sqlite3_quote_name(column_name)
        qt = sqlite3_quote_name(table)
        sql = '''
            SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL ORDER BY %s
        ''' % (qcn, qt, qcn, qcn)
        cursor = bdb.sql_execute(sql)
        codes = [row[0] for row in cursor]
        ncodes = len(codes)
        return {
            'modeltype': 'symmetric_dirichlet_discrete',
            'value_to_code': dict(zip(range(ncodes), codes)),
            'code_to_value': dict(zip(codes, range(ncodes))),
        }

    _metadata_creators = {
        'numerical': _create_metadata_numerical,
        'cyclic': _create_metadata_cyclic,
        'ignore': _create_metadata_ignore,      # XXX Why any metadata here?
        'key': _create_metadata_key,            # XXX Why any metadata here?
        'categorical': _create_metadata_categorical,
    }

    def initialize(self, **kwargs):
        return self._crosscat.initialize(**kwargs)
    def analyze(self, **kwargs):
        return self._crosscat.analyze(**kwargs)
    def mutual_information(self, **kwargs):
        return self._crosscat.mutual_information(**kwargs)
    def column_structural_typicality(self, **kwargs):
        return self._crosscat.column_structural_typicality(**kwargs)
    def simple_predictive_probability_multistate(self, **kwargs):
        return self._crosscat.simple_predictive_probability_multistate(**kwargs)
    def similarity(self, **kwargs):
        return self._crosscat.similarity(**kwargs)
    def row_structural_typicality(self, **kwargs):
        return self._crosscat.row_structural_typicality(**kwargs)
    def impute_and_confidence(self, **kwargs):
        return self._crosscat.impute_and_confidence(**kwargs)
    def simple_predictive_sample(self, **kwargs):
        return self._crosscat.simple_predictive_sample(**kwargs)
    def insert(self, **kwargs):
        return self._crosscat.insert(**kwargs)
