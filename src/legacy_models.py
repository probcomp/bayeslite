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

import gzip
import pickle

import bayeslite.core as core

from bayeslite.sqlite3_util import sqlite3_exec_1
from bayeslite.util import casefold

renamed_column_types = {
    'continuous': 'numerical',
    'multinomial': 'categorical',
}

allowed_column_types = {
    'categorical',
    'cyclic',
    'ignore',
    'key',
    'numerical',
}

def bayesdb_load_legacy_models(bdb, table_name, pathname, ifnotexists=False,
        gzipped=None):

    # Load the pickled file -- gzipped, if gzipped is true or if
    # gzipped is not specified and the file ends in .pkl.gz.
    pickled = None
    with open(pathname, 'rb') as f:
        if gzipped or (gzipped is None and pathname.endswith('.pkl.gz')):
            with gzip.GzipFile(fileobj=f) as gzf:
                pickled = pickle.load(gzf)
        else:
            pickled = pickle.load(f)

    # Pick apart the schema and model data.
    #
    # XXX Support even older models formats, from before the schema
    # was included.  Not sure exactly how they were structured.
    if 'schema' not in pickled:
        raise IOError('Invalid legacy model: missing schema')
    if 'models' not in pickled:
        raise IOError('Invalid legacy model: missing models')
    schema = pickled['schema']
    models = pickled['models']

    # Make sure the schema looks sensible.  Map legacy cctypes to
    # modern cctypes.
    if not isinstance(schema, dict):
        raise IOError('Invalid legacy model: schema is not a dict')
    for column_name in schema:
        column_schema = schema[column_name]
        if not isinstance(column_schema, dict):
            raise IOError('Invalid legacy model: column schema is not a dict')
        if not 'cctype' in column_schema:
            raise IOError('Invalid legacy model: column schema missing cctype')
        if column_schema['cctype'] in renamed_column_types:
            column_schema['cctype'] = \
                renamed_column_types[column_schema['cctype']]
        if column_schema['cctype'] not in allowed_column_types:
            raise IOError('Invalid legacy model: unknown column type')

    # XXX Check whether the schema resembles a sane btable schema.
    # XXX Check whether models is a dict mapping integers to thetas.
    # XXX Check whether the thetas look sensible.

    column_types = dict((casefold(column_name),
                         casefold(schema[column_name]['cctype']))
        for column_name in schema)

    # Ready to update the database.  Do it in a savepoint in case
    # anything goes wrong.
    with bdb.savepoint():

        # Ensure the table exists as a btable.
        if core.bayesdb_table_exists(bdb, table_name):
            # Table exists as a btable.  If there are existing models,
            # fail.  If there are no existing models, change the schema.
            table_id = core.bayesdb_table_id(bdb, table_name)
            if column_types != bayesdb_column_types(bdb, table_id):
                if 0 < core.bayesdb_nmodels(bdb, table_id):
                    raise ValueError('legacy models mismatch schema: %s' %
                        (table_name,))
                # XXX Name this operation: DROP BTABLE ...
                bdb.sqlite.execute('''
                    DELETE FROM bayesdb_table_column WHERE table_id = ?
                ''', (table_id,))
                bdb.sqlite.execute('DELETE FROM bayesdb_table WHERE id = ?',
                    (table_id,))
                core.bayesdb_import_sqlite_table(bdb, table_name,
                    column_types=column_types)
        else:
            # Table does not exist as a btable.  Create the btable,
            # assuming that a SQL table exists.  If no SQL table existed
            # by this name, tough: caller should have imported it before.
            core.bayesdb_import_sqlite_table(bdb, table_name,
                column_types=column_types)

        # Determine where to start numbering the new models.
        table_id = core.bayesdb_table_id(bdb, table_name)
        modelno_max_sql = '''
            SELECT MAX(modelno) FROM bayesdb_model WHERE table_id = ?
        '''
        modelno_max = sqlite3_exec_1(bdb.sqlite, modelno_max_sql, (table_id,))
        modelno_start = 0 if modelno_max is None else modelno_max + 1

        # XXX Urk.  Need a serious story about engine identifiers.
        engine_sql = 'SELECT id FROM bayesdb_engine WHERE name = ?'
        engine_id = sqlite3_exec_1(bdb.sqlite, engine_sql, ('crosscat',))

        # Consistently number the models consecutively in order of the
        # external numbering starting at the smallest nonnegative
        # model number not currently used.  Do not vary based on the
        # ordering of Python dict iteration.
        for i, modelno_ext in enumerate(sorted(models.keys())):
            modelno = modelno_start + i
            theta = models[modelno_ext]
            core.bayesdb_init_model(bdb, table_id, modelno, engine_id, theta)

def bayesdb_column_types(bdb, table_id):
    M_c = core.bayesdb_metadata(bdb, table_id)
    metadata = M_c['column_metadata']
    return dict((casefold(core.bayesdb_column_name(bdb, table_id, colno)),
                 casefold(metadata[colno]['modeltype']))
        for colno in core.bayesdb_column_numbers(bdb, table_id))
