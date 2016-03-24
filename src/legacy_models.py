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

"""Support for legacy models from the previous incarnation of BayesDB."""

import gzip
import json
import pickle

import bayeslite.core as core

from bayeslite.sqlite3_util import sqlite3_quote_name
from bayeslite.util import casefold
from bayeslite.util import cursor_value

renamed_column_stattypes = {
    'continuous': 'numerical',
    'multinomial': 'categorical',
}

allowed_column_stattypes = {
    'categorical',
    'cyclic',
    'ignore',
    'key',
    'numerical',
}

def bayesdb_load_legacy_models(bdb, generator, table, metamodel, pathname,
        create=False, ifnotexists=False, gzipped=None):
    """Load legacy BayesDB models from a file.

    Legacy models are from the previous incarnation of BayesDB, before
    bayeslite.  If you did not use the previous incarnation of
    BayesDB, you need not worry about this.

    :param bayeslite.BayesDB bdb: BayesDB instance
    :param str generator: name of generator
    :param str table: name of table
    :param str metamodel: name of metamodel, must be ``crosscat``
    :param str pathname: pathname of legacy models file
    :param bool create: if true and `generator` does not exist, create it
    :param bool ifnotexists: if true and `generator` exists, do it anyway
    :param bool gzipped: if true, or if ``None`` and `pathname`
        ends in ``.pkl.gz``, decompress with gzip first
    """

    if metamodel != 'crosscat':
        raise ValueError('Only crosscat legacy models are supported.')

    if not create:
        if ifnotexists:
            raise ValueError('Not creating generator whether or not exists!')

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

    # Make sure the schema looks sensible.  Map legacy stattypes
    # (`cctypes') to modern stattypes.
    if not isinstance(schema, dict):
        raise IOError('Invalid legacy model: schema is not a dict')
    for column_name in schema:
        column_schema = schema[column_name]
        if not isinstance(column_schema, dict):
            raise IOError('Invalid legacy model: column schema is not a dict')
        if not 'cctype' in column_schema:
            raise IOError('Invalid legacy model: column schema missing cctype')
        if column_schema['cctype'] in renamed_column_stattypes:
            column_schema['cctype'] = \
                renamed_column_stattypes[column_schema['cctype']]
        if column_schema['cctype'] not in allowed_column_stattypes:
            raise IOError('Invalid legacy model: unknown column type')

    # XXX Check whether the schema resembles a sane generator schema.
    # XXX Check whether models is a dict mapping integers to thetas.
    # XXX Check whether the thetas look sensible.
    # XXX Check whether the metamodel makes sense of it!

    column_stattypes = dict((casefold(column_name),
                             casefold(schema[column_name]['cctype']))
        for column_name in schema)

    # Ready to update the database.  Do it in a savepoint in case
    # anything goes wrong.
    with bdb.savepoint():

        # Ensure the table exists.  Can't do anything if we have no
        # data.
        if not core.bayesdb_has_table(bdb, table):
            raise ValueError('No such table: %s' % (repr(table),))

        # Ensure the generator exists.
        if core.bayesdb_has_generator(bdb, generator):
            if create and not ifnotexists:
                raise ValueError('Generator already exists: %s' %
                    (repr(generator),))
            generator_id = core.bayesdb_get_generator(bdb, generator)
            generator_table = core.bayesdb_generator_table(bdb, generator_id)
            if casefold(table) != generator_table:
                raise ValueError(
                    'Generator %r is for table %r, not for table: %r' %
                    (generator, generator_table, table))
            # Generator exists.  If the schema differs and there are
            # existing models, fail.  If the schema differs and there
            # are no existing models, change the schema.
            #
            # XXX Not clear changing the schema is really appropriate.
            generator_id = core.bayesdb_get_generator(bdb, generator)
            old_types = bayesdb_generator_column_stattypes(bdb, generator_id)
            if column_stattypes != old_types:
                sql = '''
                    SELECT COUNT(*) FROM bayesdb_generator_model
                        WHERE generator_id = ?
                '''
                cursor = bdb.sql_execute(sql, (generator_id,))
                if 0 < cursor_value(cursor):
                    raise ValueError('Legacy models mismatch schema: %s' %
                        (repr(generator),))
                qg = sqlite3_quote_name(generator)
                bdb.execute('DROP GENERATOR %s' % (qg,))
                bayesdb_create_legacy_generator(bdb, generator, table,
                    column_stattypes)
        elif create:
            bayesdb_create_legacy_generator(bdb, generator, table,
                column_stattypes)
        else:
            raise ValueError('No such generator: %s' % (repr(generator),))

        # Map the case of the column names in the models.
        #
        # XXX Check more than just the column names.
        for modelno in models:      # dictionary
            theta = models[modelno]
            if 'X_L' not in theta:
                raise IOError('Invalid legacy model: no X_L in theta[%u]' %
                    (modelno,))
            X_L = theta['X_L']
            if 'view_state' not in X_L:
                raise IOError('Invalid legacy model'
                    ': no view_state in X_L[%u]' %
                    (modelno,))
            for viewno, view_state in enumerate(X_L['view_state']):
                if 'column_names' not in view_state:
                    raise IOError('Invalid legacy model: no column names'
                        ' in view state %u of X_L[%u]' % (viewno, modelno))
                view_column_names = view_state['column_names']
                if not isinstance(view_column_names, list):
                    raise IOError('Invalid legacy model'
                        ': non-list for view %u columns in X_L[%u]'
                        % (viewno, modelno))
                for i in range(len(view_column_names)):
                    name = view_column_names[i]
                    if not core.bayesdb_table_has_column(bdb, table, name):
                        raise IOError('No such column in table %s: %s' %
                            (repr(table), repr(name)))
                    # Canonicalize the case.
                    colno = core.bayesdb_table_column_number(bdb, table, name)
                    name = core.bayesdb_table_column_name(bdb, table, colno)
                    view_column_names[i] = name

        # Determine where to start numbering the new models.
        generator_id = core.bayesdb_get_generator(bdb, generator)
        modelno_max_sql = '''
            SELECT MAX(modelno) FROM bayesdb_generator_model
                WHERE generator_id = ?
        '''
        cursor = bdb.sql_execute(modelno_max_sql, (generator_id,))
        modelno_max = cursor_value(cursor)
        modelno_start = 0 if modelno_max is None else modelno_max + 1

        # Consistently number the models consecutively in order of the
        # external numbering starting at the smallest nonnegative
        # model number not currently used.  Do not vary based on the
        # ordering of Python dict iteration.
        insert_model_sql = '''
            INSERT INTO bayesdb_generator_model
                (generator_id, modelno, iterations)
                VALUES (:generator_id, :modelno, :iterations)
        '''
        insert_theta_json_sql = '''
            INSERT INTO bayesdb_crosscat_theta
                (generator_id, modelno, theta_json)
                VALUES (:generator_id, :modelno, :theta_json)
        '''
        for i, modelno_ext in enumerate(sorted(models.keys())):
            modelno = modelno_start + i
            theta = models[modelno_ext]
            iterations = 0
            if 'iterations' in theta and isinstance(theta['iterations'], int):
                iterations = theta['iterations']
            bdb.sql_execute(insert_model_sql, {
                'generator_id': generator_id,
                'modelno': modelno,
                'iterations': iterations,
            })
            bdb.sql_execute(insert_theta_json_sql, {
                'generator_id': generator_id,
                'modelno': modelno,
                'theta_json': json.dumps(theta),
            })

def bayesdb_generator_column_stattypes(bdb, generator_id):
    column_stattypes = {}
    for name in core.bayesdb_generator_column_names(bdb, generator_id):
        stattype = core.bayesdb_generator_column_stattype(bdb, generator_id,
            name)
        column_stattypes[casefold(name)] = casefold(stattype)
    return column_stattypes

def bayesdb_create_legacy_generator(bdb, generator, table, column_stattypes):
    column_names = core.bayesdb_table_column_names(bdb, table)
    qcns = map(sqlite3_quote_name, column_names)
    assert all(column_stattypes[name] in allowed_column_stattypes
        for name in column_stattypes)
    column_name_set = set(casefold(name) for name in column_names)
    for name in column_stattypes:
        if name not in column_name_set:
            raise IOError('No such column in table %s: %s' %
                (repr(table), repr(name)))
    schema = ','.join('%s %s' % (qcn, column_stattypes[casefold(name)])
        for name, qcn in zip(column_names, qcns))
    qg = sqlite3_quote_name(generator)
    qt = sqlite3_quote_name(table)
    qmm = 'crosscat'
    bdb.execute('CREATE GENERATOR %s FOR %s USING %s(%s)' %
        (qg, qt, qmm, schema))
