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

"""Crosscat is a fully Bayesian nonparametric method for analyzing
heterogeneous, high-dimensional data, described at
`<http://probcomp.csail.mit.edu/crosscat/>`__.

This module implements the :class:`bayeslite.IBayesDBMetamodel`
interface for Crosscat.
"""

import apsw
import itertools
import json
import math
import struct
import time

import bayeslite.core as core
import bayeslite.guess as guess
import bayeslite.metamodel as metamodel
import bayeslite.weakprng as weakprng
import crosscat_generator_schema
import crosscat_theta_validator

from bayeslite.exception import BQLError
from bayeslite.sqlite3_util import sqlite3_quote_name
from bayeslite.stats import arithmetic_mean
from bayeslite.util import casefold
from bayeslite.util import cursor_value

crosscat_schema_1 = '''
INSERT INTO bayesdb_metamodel (name, version) VALUES ('crosscat', 1);

CREATE TABLE bayesdb_crosscat_disttype (
    name		TEXT NOT NULL PRIMARY KEY,
    stattype	TEXT NOT NULL REFERENCES bayesdb_stattype(name),
    default_dist	BOOLEAN NOT NULL,
    UNIQUE(stattype, default_dist)
);

INSERT INTO bayesdb_crosscat_disttype (name, stattype, default_dist)
    VALUES
        ('normal_inverse_gamma', 'numerical', 1),
        ('symmetric_dirichlet_discrete', 'categorical', 1),
        ('vonmises', 'cyclic', 1);

CREATE TABLE bayesdb_crosscat_metadata (
    generator_id	INTEGER NOT NULL PRIMARY KEY
                REFERENCES bayesdb_generator(id),
    metadata_json	BLOB NOT NULL
);

CREATE TABLE bayesdb_crosscat_column (
    generator_id	INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    colno		INTEGER NOT NULL CHECK (0 <= colno),
    cc_colno	INTEGER NOT NULL CHECK (0 <= cc_colno),
    disttype	TEXT NOT NULL,
    PRIMARY KEY(generator_id, colno),
    FOREIGN KEY(generator_id, colno)
        REFERENCES bayesdb_generator_column(generator_id, colno),
    UNIQUE(generator_id, cc_colno)
);

CREATE TABLE bayesdb_crosscat_column_codemap (
    generator_id	INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    cc_colno	INTEGER NOT NULL CHECK (0 <= cc_colno),
    code		INTEGER NOT NULL,
    value		TEXT NOT NULL,
    FOREIGN KEY(generator_id, cc_colno)
        REFERENCES bayesdb_crosscat_column(generator_id, cc_colno),
    UNIQUE(generator_id, cc_colno, code),
    UNIQUE(generator_id, cc_colno, value)
);

CREATE TABLE bayesdb_crosscat_theta (
    generator_id	INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    modelno		INTEGER NOT NULL,
    theta_json	BLOB NOT NULL,
    PRIMARY KEY(generator_id, modelno),
    FOREIGN KEY(generator_id, modelno)
        REFERENCES bayesdb_generator_model(generator_id, modelno)
);
'''

crosscat_schema_1to2 = '''
UPDATE bayesdb_metamodel SET version = 2 WHERE name = 'crosscat';

CREATE TABLE bayesdb_crosscat_diagnostics (
    generator_id	INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    modelno		INTEGER NOT NULL,
    checkpoint	INTEGER NOT NULL,
    logscore	REAL NOT NULL CHECK (logscore <= 0),
    num_views	INTEGER NOT NULL CHECK (0 < num_views),
    column_crp_alpha
            REAL NOT NULL,
    iterations	INTEGER,	-- Not historically recorded.
    PRIMARY KEY(generator_id, modelno, checkpoint),
    FOREIGN KEY(generator_id, modelno)
        REFERENCES bayesdb_generator_model(generator_id, modelno)
);
'''

crosscat_schema_2to3 = '''
UPDATE bayesdb_metamodel SET version = 3 WHERE name = 'crosscat';

CREATE TABLE bayesdb_crosscat_subsampled (
    generator_id	INTEGER NOT NULL PRIMARY KEY
                REFERENCES bayesdb_crosscat_metadata
);

-- Generator-wide subsample, not per-model.
CREATE TABLE bayesdb_crosscat_subsample (
    generator_id    INTEGER NOT NULL
                REFERENCES bayesdb_crosscat_subsampled,
    sql_rowid	INTEGER NOT NULL,
    cc_row_id	INTEGER NOT NULL,
    PRIMARY KEY(generator_id, sql_rowid ASC),
    UNIQUE(generator_id, cc_row_id ASC)
    -- Can't express the desired foreign key constraint,
    --	FOREIGN KEY(sql_rowid) REFERENCES <table of generator>(rowid),
    -- for two reasons:
    --   1. No way for constraint to have data-dependent table.
    --   2. Can't refer to implicit rowid in sqlite3 constraints.
    -- So we'll just hope nobody botches it.
);
'''

crosscat_schema_3to4 = '''
UPDATE bayesdb_metamodel SET version = 4 WHERE name = 'crosscat';

CREATE TABLE bayesdb_crosscat_subsample_temp (
    generator_id    INTEGER NOT NULL
                REFERENCES bayesdb_crosscat_metadata,
    sql_rowid	INTEGER NOT NULL,
    cc_row_id	INTEGER NOT NULL,
    PRIMARY KEY(generator_id, sql_rowid ASC),
    UNIQUE(generator_id, cc_row_id ASC)
    -- Can't express the desired foreign key constraint,
    --	FOREIGN KEY(sql_rowid) REFERENCES <table of generator>(rowid),
    -- for two reasons:
    --   1. No way for constraint to have data-dependent table.
    --   2. Can't refer to implicit rowid in sqlite3 constraints.
    -- So we'll just hope nobody botches it.
);
INSERT INTO bayesdb_crosscat_subsample_temp
    SELECT * FROM bayesdb_crosscat_subsample;
DROP TABLE bayesdb_crosscat_subsample;
ALTER TABLE bayesdb_crosscat_subsample_temp
    RENAME TO bayesdb_crosscat_subsample;

DROP TABLE bayesdb_crosscat_subsampled;
'''

crosscat_schema_4to5 = '''
UPDATE bayesdb_metamodel SET version = 5 WHERE name = 'crosscat';

-- Remove the constraint that logscore be nonpositive, since evidently
-- it is the log of a density rather than the log of a normalized
-- probability.
ALTER TABLE bayesdb_crosscat_diagnostics
    RENAME TO bayesdb_crosscat_diagnostics_temp;
CREATE TABLE bayesdb_crosscat_diagnostics (
    generator_id	INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    modelno		INTEGER NOT NULL,
    checkpoint	INTEGER NOT NULL,
    logscore	REAL NOT NULL,
    num_views	INTEGER NOT NULL CHECK (0 < num_views),
    column_crp_alpha
            REAL NOT NULL,
    iterations	INTEGER,	-- Not historically recorded.
    PRIMARY KEY(generator_id, modelno, checkpoint),
    FOREIGN KEY(generator_id, modelno)
        REFERENCES bayesdb_generator_model(generator_id, modelno)
);
INSERT INTO bayesdb_crosscat_diagnostics
    SELECT * FROM bayesdb_crosscat_diagnostics_temp;
DROP TABLE bayesdb_crosscat_diagnostics_temp;
'''

crosscat_schema_5to6 = '''
UPDATE bayesdb_metamodel SET version = 6 WHERE name = 'crosscat';

CREATE TABLE bayesdb_crosscat_column_dependency (
    generator_id    INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    colno0      INTEGER NOT NULL,
    colno1      INTEGER NOT NULL,
    dependent   BOOLEAN NOT NULL,
    PRIMARY KEY(generator_id, colno0, colno1),
    FOREIGN KEY(generator_id, colno0)
        REFERENCES bayesdb_generator_column(generator_id, colno),
    FOREIGN KEY(generator_id, colno1)
        REFERENCES bayesdb_generator_column(generator_id, colno),
    CHECK(colno0 < colno1)
);
'''

class CrosscatMetamodel(metamodel.IBayesDBMetamodel):
    """Crosscat metamodel for BayesDB.

    :param crosscat: Crosscat engine.

    The metamodel is named ``crosscat`` in BQL::

        CREATE GENERATOR t_cc FOR t USING crosscat(...)

    Internally, the Crosscat metamodel adds SQL tables to the database
    with names that begin with ``bayesdb_crosscat_``.
    """

    def __init__(self, crosscat, subsample=None):
        if subsample is None:
            subsample = False
        self._crosscat = crosscat
        self._subsample = subsample
        self._theta_validator = crosscat_theta_validator.Validator()

    def _crosscat_cache_nocreate(self, bdb):
        if bdb.cache is None:
            return None
        if 'crosscat' not in bdb.cache:
            return None
        return self._crosscat_cache(bdb)

    def _crosscat_cache(self, bdb):
        if bdb.cache is None:
            return None
        if 'crosscat' in bdb.cache:
            return bdb.cache['crosscat']
        else:
            cc_cache = CrosscatCache()
            bdb.cache['crosscat'] = cc_cache
            return cc_cache

    def _crosscat_metadata(self, bdb, generator_id):
        cc_cache = self._crosscat_cache(bdb)
        if cc_cache is not None and generator_id in cc_cache.metadata:
            return cc_cache.metadata[generator_id]
        sql = '''
            SELECT metadata_json FROM bayesdb_crosscat_metadata
                WHERE generator_id = ?
        '''
        cursor = bdb.sql_execute(sql, (generator_id,))
        try:
            row = cursor.next()
        except StopIteration:
            generator = core.bayesdb_generator_name(bdb, generator_id)
            raise BQLError(bdb, 'No crosscat metadata for generator: %s' %
                (generator,))
        else:
            metadata = json.loads(row[0])
            if cc_cache is not None:
                cc_cache.metadata[generator_id] = metadata
            return metadata

    def _crosscat_data(self, bdb, generator_id, M_c):
        table_name = core.bayesdb_generator_table(bdb, generator_id)
        qt = sqlite3_quote_name(table_name)
        columns_sql = '''
            SELECT c.name, c.colno, gc.stattype
                FROM bayesdb_column AS c,
                    bayesdb_generator AS g,
                    bayesdb_generator_column AS gc
                WHERE g.id = ?
                    AND c.tabname = g.tabname
                    AND c.colno = gc.colno
                    AND gc.generator_id = g.id
                ORDER BY c.colno ASC
        '''
        columns = bdb.sql_execute(columns_sql, (generator_id,)).fetchall()
        qexpressions = ','.join('CAST(t.%s AS %s)' %
                (sqlite3_quote_name(name),
                    sqlite3_quote_name(core.bayesdb_stattype_affinity(bdb,
                            stattype)))
            for name, _colno, stattype in columns)
        cursor = bdb.sql_execute('''
            SELECT %s FROM %s AS t, bayesdb_crosscat_subsample AS s
                WHERE s.generator_id = ?
                    AND s.sql_rowid = t._rowid_
        ''' % (qexpressions, qt), (generator_id,))
        return [[crosscat_value_to_code(bdb, generator_id, M_c, colno, value)
                for value, (_name, colno, _stattype) in zip(row, columns)]
            for row in cursor]

    def _crosscat_thetas(self, bdb, generator_id, modelno):
        if modelno is not None:
            return {modelno: self._crosscat_theta(bdb, generator_id, modelno)}
        sql = '''
            SELECT modelno FROM bayesdb_crosscat_theta
                WHERE generator_id = ?
        '''
        modelnos = (row[0] for row in bdb.sql_execute(sql, (generator_id,)))
        return dict((modelno, self._crosscat_theta(bdb, generator_id, modelno))
            for modelno in modelnos)

    def _crosscat_theta(self, bdb, generator_id, modelno):
        cc_cache = self._crosscat_cache(bdb)
        if cc_cache is not None and \
           generator_id in cc_cache.thetas and \
           modelno in cc_cache.thetas[generator_id]:
            return cc_cache.thetas[generator_id][modelno]
        sql = '''
            SELECT theta_json FROM bayesdb_crosscat_theta
                WHERE generator_id = ? AND modelno = ?
        '''
        cursor = bdb.sql_execute(sql, (generator_id, modelno))
        try:
            row = cursor.next()
        except StopIteration:
            generator = core.bayesdb_generator_name(bdb, generator_id)
            raise BQLError(bdb, 'No such crosscat model for generator %s: %d' %
                (repr(generator), modelno))
        else:
            theta = json.loads(row[0])
            if cc_cache is not None:
                if generator_id in cc_cache.thetas:
                    assert modelno not in cc_cache.thetas[generator_id]
                    cc_cache.thetas[generator_id][modelno] = theta
                else:
                    cc_cache.thetas[generator_id] = {modelno: theta}
            return theta

    def _crosscat_latent_stata(self, bdb, generator_id, modelno):
        thetas = self._crosscat_thetas(bdb, generator_id, modelno)
        return ((thetas[modelno]['X_L'], thetas[modelno]['X_D'])
            for modelno in sorted(thetas.iterkeys()))

    def _crosscat_latent_state(self, bdb, generator_id, modelno):
        return [statum[0] for statum
            in self._crosscat_latent_stata(bdb, generator_id, modelno)]

    def _crosscat_latent_data(self, bdb, generator_id, modelno):
        return [statum[1] for statum
            in self._crosscat_latent_stata(bdb, generator_id, modelno)]

    def _crosscat_get_row(self, bdb, generator_id, rowid, X_L_list, X_D_list):
        [row_id], X_L_list, X_D_list = \
            self._crosscat_get_rows(bdb, generator_id, [rowid], X_L_list,
                X_D_list)
        return row_id, X_L_list, X_D_list

    def _crosscat_get_rows(self, bdb, generator_id, rowids, X_L_list,
            X_D_list):
        row_ids = [None] * len(rowids)
        index = {}
        for i, rowid in enumerate(rowids):
            if rowid in index:
                index[rowid].add(i)
            else:
                index[rowid] = set([i])
        cursor = bdb.sql_execute('''
            SELECT sql_rowid, cc_row_id FROM bayesdb_crosscat_subsample
                WHERE generator_id = ?
                    AND sql_rowid IN (%s)
        ''' % (','.join('%d' % (rowid,) for rowid in sorted(set(rowids)))),
            (generator_id,))
        for rowid, row_id in cursor:
            for i in index[rowid]:
                row_ids[i] = row_id
            del index[rowid]
        if 0 < len(index):
            rowids = sorted(set(index.keys()))
            table_name = core.bayesdb_generator_table(bdb, generator_id)
            qt = sqlite3_quote_name(table_name)
            columns = bdb.sql_execute('''
                SELECT c.name, c.colno, gc.stattype
                    FROM bayesdb_column AS c,
                        bayesdb_generator AS g,
                        bayesdb_generator_column AS gc
                    WHERE g.id = ?
                        AND c.tabname = g.tabname
                        AND c.colno = gc.colno
                        AND gc.generator_id = g.id
                    ORDER BY c.colno ASC
            ''', (generator_id,)).fetchall()
            qexpressions = ','.join('CAST(%s AS %s)' %
                    (sqlite3_quote_name(name),
                        sqlite3_quote_name(core.bayesdb_stattype_affinity(bdb,
                                stattype)))
                for name, _colno, stattype in columns)
            qrowids = ','.join('%d' % (rowid,) for rowid in rowids)
            M_c = self._crosscat_metadata(bdb, generator_id)
            cursor = bdb.sql_execute('''
                SELECT %s FROM %s WHERE _rowid_ IN (%s) ORDER BY _rowid_ ASC
            ''' % (qexpressions, qt, qrowids))
            rows = [[crosscat_value_to_code(bdb, generator_id, M_c, colno, x)
                     for x, (_name, colno, _stattype) in zip(row, columns)]
                for row in cursor]
            if len(rows) > 0:
                # Need to put more stuff into the subsample temporarily
                T = self._crosscat_data(bdb, generator_id, M_c)
                X_L_list, X_D_list, T = self._crosscat.insert(
                    M_c=M_c,
                    T=T,
                    X_L_list=X_L_list,
                    X_D_list=X_D_list,
                    new_rows=rows,
                )
                for r0, r1 in \
                    zip(T, self._crosscat_data(bdb, generator_id, M_c) + rows):
                    assert all(x0 == x1 or (math.isnan(x0) and math.isnan(x1))
                               for x0, x1 in zip(r0, r1))
            cursor = bdb.sql_execute('''
                SELECT MAX(cc_row_id) + 1 FROM bayesdb_crosscat_subsample
                    WHERE generator_id = ?
            ''', (generator_id,))
            next_row_id = cursor_value(cursor)
            for n, rowid in enumerate(rowids):
                for i in index[rowid]:
                    row_ids[i] = next_row_id + n
        assert all(row_id is not None for row_id in row_ids)
        return row_ids, X_L_list, X_D_list

    def _crosscat_remap_mixed(self, bdb, generator_id, X_L_list,
            X_D_list, items):
        # XXX Why special-case empty items?
        if items is None:
            return None, X_L_list, X_D_list
        M_c = self._crosscat_metadata(bdb, generator_id)
        rowids = [item[0] for item in items]
        row_ids, X_L_list, X_D_list = self._crosscat_get_rows(
            bdb, generator_id, rowids, X_L_list, X_D_list)
        def remap_tuple(row_id, item):
            # XXX See the comment on _crosscat_remap_two below for an
            # explanation of this horrible type dispatch.
            if len(item) == 2:
                (_, colno) = item
                return (row_id, crosscat_cc_colno(bdb, generator_id, colno))
            if len(item) == 3:
                (_, colno, value) = item
                new_colno = crosscat_cc_colno(bdb, generator_id, colno)
                new_value = crosscat_value_to_code(
                    bdb, generator_id, M_c, colno, value)
                return (row_id, new_colno, new_value)
        res = [remap_tuple(row_id, item)
               for row_id, item in zip(row_ids, items)]
        return res, X_L_list, X_D_list

    def _crosscat_remap_two(self, bdb, generator_id, X_L_list, X_D_list,
            first, second):
        # XXX This kludgerosity (together with the tuple size dispatch
        # in _crosscat_remap_mixed) is trying to apply a consistent
        # row id mapping to both the targets and the constraints.  In
        # retrospect, it may have been better to do that by making a
        # routine that explicitly constructs a row id mapping table
        # and separately applying it to the targets and the
        # constraints.  As it is, said mapping table is more or less
        # implicit in the behavior of _crosscat_get_rows (and
        # intertwined with possibly extending the input crosscat
        # states in the case that the requested rows are added to the
        # effective subsample).
        if first is None:
            new_second, X_L_list, X_D_list = self._crosscat_remap_mixed(
                bdb, generator_id, X_L_list, X_D_list, second)
            return None, new_second, X_L_list, X_D_list
        if second is None:
            new_first, X_L_list, X_D_list = self._crosscat_remap_mixed(
                bdb, generator_id, X_L_list, X_D_list, first)
            return new_first, None, X_L_list, X_D_list
        new, X_L_list, X_D_list = self._crosscat_remap_mixed(
            bdb, generator_id, X_L_list, X_D_list, first + second)
        return new[:len(first)], new[len(first):], X_L_list, X_D_list

    def name(self):
        return 'crosscat'

    def register(self, bdb):
        with bdb.savepoint():
            schema_sql = 'SELECT version FROM bayesdb_metamodel WHERE name = ?'
            cursor = bdb.sql_execute(schema_sql, (self.name(),))
            version = None
            try:
                row = cursor.next()
            except StopIteration:
                version = 0
            else:
                version = row[0]
            assert version is not None
            if version == 0:
                # XXX WHATTAKLUDGE!
                for stmt in crosscat_schema_1.split(';'):
                    bdb.sql_execute(stmt)
                version = 1
            if version == 1:
                # XXX WHATTAKLUDGE!
                for stmt in crosscat_schema_1to2.split(';'):
                    bdb.sql_execute(stmt)
                # We never recorded diagnostics in the past, so we
                # can't fill the table in with historical data.  But
                # we did create stub entries in the theta dicts which
                # serve no purpose now, so nuke them.
                sql = '''
                    SELECT generator_id, modelno, theta_json
                        FROM bayesdb_crosscat_theta
                '''
                update_sql = '''
                    UPDATE bayesdb_crosscat_theta SET theta_json = :theta_json
                        WHERE generator_id = :generator_id
                            AND modelno = :modelno
                '''
                for generator_id, modelno, theta_json in bdb.sql_execute(sql):
                    theta = json.loads(theta_json)
                    if len(theta['logscore']) != 0 or \
                       len(theta['num_views']) != 0 or \
                       len(theta['column_crp_alpha']) != 0:
                        raise IOError('Non-stub diagnostics!')
                    del theta['logscore']
                    del theta['num_views']
                    del theta['column_crp_alpha']
                    self._theta_validator.validate(theta)
                    theta_json = json.dumps(theta)
                    bdb.sql_execute(update_sql, {
                        'generator_id': generator_id,
                        'modelno': modelno,
                        'theta_json': theta_json,
                    })
                version = 2
            if version == 2:
                for stmt in crosscat_schema_2to3.split(';'):
                    bdb.sql_execute(stmt)
                version = 3
            if version == 3:
                cursor = bdb.sql_execute('''
                    SELECT generator_id FROM bayesdb_crosscat_metadata
                        WHERE NOT EXISTS
                                (SELECT * FROM bayesdb_crosscat_subsampled AS s
                                    WHERE s.generator_id = generator_id)
                ''')
                for (generator_id,) in cursor:
                    bdb.sql_execute('''
                        INSERT INTO bayesdb_crosscat_subsampled (generator_id)
                            VALUES (?)
                    ''', (generator_id,))
                    table_name = core.bayesdb_generator_table(bdb,
                        generator_id)
                    qt = sqlite3_quote_name(table_name)
                    bdb.sql_execute('''
                        INSERT INTO bayesdb_crosscat_subsample
                            (generator_id, sql_rowid, cc_row_id)
                            SELECT ?, _rowid_, _rowid_ - 1 FROM %s
                    ''' % (qt,), (generator_id,))
                for stmt in crosscat_schema_3to4.split(';'):
                    bdb.sql_execute(stmt)
                version = 4
            if version == 4:
                for stmt in crosscat_schema_4to5.split(';'):
                    bdb.sql_execute(stmt)
                version = 5
            if version == 5:
                for stmt in crosscat_schema_5to6.split(';'):
                    bdb.sql_execute(stmt)
                version = 6
            if version != 6:
                raise BQLError(bdb, 'Crosscat already installed'
                    ' with unknown schema version: %d' % (version,))

    def create_generator(self, bdb, table, schema, instantiate):
        parsed_schema = crosscat_generator_schema.parse(
            schema, subsample_default=self._subsample)

        with bdb.savepoint():
            # If necessary, guess the column statistical types.
            #
            # XXX Allow passing count/ratio cutoffs, and other
            # parameters.
            if parsed_schema.guess:
                column_names = core.bayesdb_table_column_names(bdb, table)
                qt = sqlite3_quote_name(table)
                rows = bdb.sql_execute('SELECT * FROM %s' % (qt,)).fetchall()
                stattypes = guess.bayesdb_guess_stattypes(column_names, rows,
                    overrides=parsed_schema.columns)
                columns = zip(column_names, stattypes)
                columns = [(name, stattype) for name, stattype in columns
                    if stattype not in ('key', 'ignore')]
            else:
                columns = parsed_schema.columns

            # Create the metamodel-independent records and assign a
            # generator id.
            generator_id, column_list = instantiate(columns)

            # Install the metadata json blob.
            M_c = create_metadata(bdb, generator_id, column_list)
            insert_metadata_sql = '''
                INSERT INTO bayesdb_crosscat_metadata
                    (generator_id, metadata_json)
                    VALUES (?, ?)
            '''
            metadata_json = json.dumps(M_c)
            bdb.sql_execute(insert_metadata_sql, (generator_id, metadata_json))

            # Cache the metadata json blob -- we'll probably use it
            # soon.
            cc_cache = self._crosscat_cache(bdb)
            if cc_cache is not None:
                assert generator_id not in cc_cache.metadata
                cc_cache.metadata[generator_id] = M_c

            # Expose the same information relationally.
            insert_column_sql = '''
                INSERT INTO bayesdb_crosscat_column
                    (generator_id, colno, cc_colno, disttype)
                    VALUES (:generator_id, :colno, :cc_colno, :disttype)
            '''
            insert_codemap_sql = '''
                INSERT INTO bayesdb_crosscat_column_codemap
                    (generator_id, cc_colno, code, value)
                    VALUES (:generator_id, :cc_colno, :code, :value)
            '''
            for cc_colno, (colno, name, _stattype) in enumerate(column_list):
                column_metadata = M_c['column_metadata'][cc_colno]
                disttype = column_metadata['modeltype']
                bdb.sql_execute(insert_column_sql, {
                    'generator_id': generator_id,
                    'colno': colno,
                    'cc_colno': cc_colno,
                    'disttype': disttype,
                })
                codemap = column_metadata['value_to_code']
                for code in codemap:
                    bdb.sql_execute(insert_codemap_sql, {
                        'generator_id': generator_id,
                        'cc_colno': cc_colno,
                        'code': code,
                        'value': codemap[code],
                    })

            # Choose a subsample (possibly the whole thing).
            qt = sqlite3_quote_name(table)
            cursor = None
            if parsed_schema.subsample:
                # Sample k of the n rowids without replacement,
                # choosing from all the k-of-n combinations uniformly
                # at random.
                #
                # XXX Let the user pass in a seed.
                k = parsed_schema.subsample
                sql = 'SELECT COUNT(*) FROM %s' % (qt,)
                n = cursor_value(bdb.sql_execute(sql))
                sql = 'SELECT _rowid_ FROM %s ORDER BY _rowid_ ASC' % (qt,)
                cursor = bdb.sql_execute(sql)
                seed = struct.pack('<QQQQ', 0, 0, k, n)
                uniform = weakprng.weakprng(seed).weakrandom_uniform
                # https://en.wikipedia.org/wiki/Reservoir_sampling
                samples = []
                for i, row in enumerate(cursor):
                    if i < k:
                        samples.append(row)
                    else:
                        r = uniform(i + 1)
                        if r < k:
                            samples[r] = row
                cursor = samples
            else:
                cursor = bdb.sql_execute('''
                     SELECT _rowid_ FROM %s ORDER BY _rowid_ ASC
                ''' % (qt,))
            insert_subsample_sql = '''
                INSERT INTO bayesdb_crosscat_subsample
                    (generator_id, sql_rowid, cc_row_id)
                    VALUES (?, ?, ?)
            '''
            for i, row in enumerate(cursor):
                sql_rowid = row[0]
                cc_row_id = i
                bdb.sql_execute(insert_subsample_sql,
                    (generator_id, sql_rowid, cc_row_id))

            # Store dependence constraints, if necessary.
            insert_dep_constraint_sql = '''
                INSERT INTO bayesdb_crosscat_column_dependency
                    (generator_id, colno0, colno1, dependent)
                    VALUES (?, ?, ?, ?)
            '''
            for columns, dependent in parsed_schema.dep_constraints:
                for col1, col2 in itertools.combinations(columns, 2):
                    col1_id = core.bayesdb_generator_column_number(bdb,
                        generator_id, col1)
                    col2_id = core.bayesdb_generator_column_number(bdb,
                        generator_id, col2)
                    min_col_id = min(col1_id, col2_id)
                    max_col_id = max(col1_id, col2_id)
                    try:
                        bdb.sql_execute(insert_dep_constraint_sql,
                            (generator_id, min_col_id, max_col_id, dependent))
                    except apsw.ConstraintError:
                        # XXX This is a cop-out -- we should validate
                        # the relation ourselves (and show a more
                        # helpful error message).
                        raise BQLError(bdb, 'Invalid dependency constraints!')

    def drop_generator(self, bdb, generator_id):
        with bdb.savepoint():
            # Remove the metadata from the cache.
            cc_cache = self._crosscat_cache_nocreate(bdb)
            if cc_cache is not None:
                if generator_id in cc_cache.metadata:
                    del cc_cache.metadata[generator_id]
                if generator_id in cc_cache.thetas:
                    del cc_cache.thetas[generator_id]

            # Delete all the things referring to the generator:
            # - diagnostics
            # - column depedencies
            # - models
            # - subsample
            # - codemap
            # - columns
            # - metadata
            delete_diagnostics_sql = '''
                DELETE FROM bayesdb_crosscat_diagnostics
                    WHERE generator_id = ?
            '''
            bdb.sql_execute(delete_diagnostics_sql, (generator_id,))
            delete_column_dependency_sql = '''
                DELETE FROM bayesdb_crosscat_column_dependency
                    WHERE generator_id = ?
            '''
            bdb.sql_execute(delete_column_dependency_sql, (generator_id,))
            delete_models_sql = '''
                DELETE FROM bayesdb_crosscat_theta
                    WHERE generator_id = ?
            '''
            bdb.sql_execute(delete_models_sql, (generator_id,))
            delete_subsample_sql = '''
                DELETE FROM bayesdb_crosscat_subsample
                    WHERE generator_id = ?
            '''
            bdb.sql_execute(delete_subsample_sql, (generator_id,))
            delete_codemap_sql = '''
                DELETE FROM bayesdb_crosscat_column_codemap
                    WHERE generator_id = ?
            '''
            bdb.sql_execute(delete_codemap_sql, (generator_id,))
            delete_column_sql = '''
                DELETE FROM bayesdb_crosscat_column
                    WHERE generator_id = ?
            '''
            bdb.sql_execute(delete_column_sql, (generator_id,))
            delete_metadata_sql = '''
                DELETE FROM bayesdb_crosscat_metadata
                    WHERE generator_id = ?
            '''
            bdb.sql_execute(delete_metadata_sql, (generator_id,))

    def rename_column(self, bdb, generator_id, oldname, newname):
        assert oldname != newname
        M_c = self._crosscat_metadata(bdb, generator_id)
        assert oldname in M_c['name_to_idx']
        assert newname not in M_c['name_to_idx']
        idx = M_c['name_to_idx'][oldname]
        assert M_c['idx_to_name'][unicode(idx)] == oldname
        del M_c['name_to_idx'][oldname]
        M_c['name_to_idx'][newname] = idx
        M_c['idx_to_name'][unicode(idx)] = newname
        sql = '''
            UPDATE bayesdb_crosscat_metadata SET metadata_json = :metadata_json
                WHERE generator_id = :generator_id
        '''
        metadata_json = json.dumps(M_c)
        total_changes = bdb._sqlite3.totalchanges()
        bdb.sql_execute(sql, {
            'generator_id': generator_id,
            'metadata_json': metadata_json,
        })
        assert bdb._sqlite3.totalchanges() - total_changes == 1
        cc_cache = self._crosscat_cache_nocreate(bdb)
        if cc_cache is not None:
            cc_cache.metadata[generator_id] = M_c

    def initialize_models(self, bdb, generator_id, modelnos, model_config):
        cc_cache = self._crosscat_cache(bdb)
        if cc_cache is not None and generator_id in cc_cache.thetas:
            assert not any(modelno in cc_cache.thetas[generator_id]
                for modelno in modelnos)
        if model_config is None:
            model_config = {
                'kernel_list': [],
                'initialization': 'from_the_prior',
                'row_initialization': 'from_the_prior',
            }
        M_c = self._crosscat_metadata(bdb, generator_id)
        X_L_list, X_D_list = self._crosscat.initialize(
            seed=crosscat_seed(bdb),
            M_c=M_c,
            M_r=None,           # XXX
            T=self._crosscat_data(bdb, generator_id, M_c),
            n_chains=len(modelnos),
            initialization=model_config['initialization'],
            row_initialization=model_config['row_initialization'],
        )
        if len(modelnos) == 1:  # XXX Ugh.  Fix crosscat so it doesn't do this.
            X_L_list = [X_L_list]
            X_D_list = [X_D_list]
        # Ensure dependent columns if necessary.
        dep_constraints = [(crosscat_cc_colno(bdb, generator_id, colno1),
                crosscat_cc_colno(bdb, generator_id, colno2), dep)
            for colno1, colno2, dep in
                crosscat_gen_column_dependencies(bdb, generator_id)]
        if 0 < len(dep_constraints):
            X_L_list, X_D_list = self._crosscat.ensure_col_dep_constraints(
                seed=crosscat_seed(bdb),
                M_c=M_c,
                M_r=None,
                T=self._crosscat_data(bdb, generator_id, M_c),
                X_L=X_L_list,
                X_D=X_D_list,
                dep_constraints=dep_constraints,
            )
        insert_theta_sql = '''
            INSERT INTO bayesdb_crosscat_theta
                (generator_id, modelno, theta_json)
                VALUES (:generator_id, :modelno, :theta_json)
        '''
        for modelno, (X_L, X_D) in zip(modelnos, zip(X_L_list, X_D_list)):
            theta = {
                'X_L': X_L,
                'X_D': X_D,
                'iterations': 0,
                'model_config': model_config,
            }
            self._theta_validator.validate(theta)
            bdb.sql_execute(insert_theta_sql, {
                'generator_id': generator_id,
                'modelno': modelno,
                'theta_json': json.dumps(theta),
            })
            if cc_cache is not None:
                if generator_id in cc_cache.thetas:
                    assert modelno not in cc_cache.thetas[generator_id]
                    cc_cache.thetas[generator_id][modelno] = theta
                else:
                    cc_cache.thetas[generator_id] = {modelno: theta}

    def drop_models(self, bdb, generator_id, modelnos=None):
        cc_cache = self._crosscat_cache_nocreate(bdb)
        if modelnos is None:
            if cc_cache is not None:
                if generator_id in cc_cache.thetas:
                    del cc_cache.thetas[generator_id]
            delete_theta_sql = '''
                DELETE FROM bayesdb_crosscat_theta WHERE generator_id = ?
            '''
            delete_diag_sql = '''
                DELETE FROM bayesdb_crosscat_diagnostics WHERE generator_id = ?
            '''
            bdb.sql_execute(delete_theta_sql, (generator_id,))
            bdb.sql_execute(delete_diag_sql, (generator_id,))
        else:
            delete_theta_sql = '''
                DELETE FROM bayesdb_crosscat_theta
                    WHERE generator_id = ? AND modelno = ?
            '''
            delete_diag_sql = '''
                DELETE FROM bayesdb_crosscat_diagnostics
                    WHERE generator_id = ? AND modelno = ?
            '''
            for modelno in modelnos:
                bdb.sql_execute(delete_theta_sql, (generator_id, modelno))
                bdb.sql_execute(delete_diag_sql, (generator_id, modelno))
            if cc_cache is not None and generator_id in cc_cache.thetas:
                for modelno in modelnos:
                    if modelno in cc_cache.thetas[generator_id]:
                        del cc_cache.thetas[generator_id][modelno]
                if len(cc_cache.thetas[generator_id]) == 0:
                    del cc_cache.thetas[generator_id]

    def analyze_models(self, bdb, generator_id, modelnos=None, iterations=1,
            max_seconds=None, ckpt_iterations=None, ckpt_seconds=None):
        # XXX What about a schema change or insert in the middle of
        # analysis?
        M_c = self._crosscat_metadata(bdb, generator_id)
        T = self._crosscat_data(bdb, generator_id, M_c)
        update_iterations_sql = '''
            UPDATE bayesdb_generator_model
                SET iterations = iterations + :iterations
                WHERE generator_id = :generator_id AND modelno = :modelno
        '''
        update_theta_json_sql = '''
            UPDATE bayesdb_crosscat_theta SET theta_json = :theta_json
                WHERE generator_id = :generator_id AND modelno = :modelno
        '''
        insert_diagnostics_sql = '''
            INSERT INTO bayesdb_crosscat_diagnostics
                (generator_id, modelno, checkpoint,
                    logscore, num_views, column_crp_alpha, iterations)
                VALUES (:generator_id, :modelno, :checkpoint,
                    :logscore, :num_views, :column_crp_alpha, :iterations)
        '''
        if max_seconds is not None:
            deadline = time.time() + max_seconds
        if ckpt_seconds is not None:
            ckpt_deadline = time.time() + ckpt_seconds
            if max_seconds is not None:
                ckpt_deadline = min(ckpt_deadline, deadline)
        if ckpt_iterations is not None and iterations is not None:
            ckpt_iterations = min(ckpt_iterations, iterations)
        while (iterations is None or 0 < iterations) and \
              (max_seconds is None or time.time() < deadline):
            n_steps = 1
            if ckpt_seconds is not None:
                n_steps = 1
            elif ckpt_iterations is not None:
                assert 0 < ckpt_iterations
                n_steps = ckpt_iterations
                if iterations is not None:
                    n_steps = min(n_steps, iterations)
            elif iterations is not None and max_seconds is None:
                n_steps = iterations
            with bdb.savepoint():
                if modelnos is None:
                    numbered_thetas = self._crosscat_thetas(bdb, generator_id,
                        None)
                    update_modelnos = sorted(numbered_thetas.iterkeys())
                    thetas = [numbered_thetas[modelno] for modelno in
                        update_modelnos]
                else:
                    update_modelnos = modelnos
                    thetas = [self._crosscat_theta(bdb, generator_id, modelno)
                        for modelno in update_modelnos]
                if len(thetas) == 0:
                    raise BQLError(bdb, 'No models to analyze'
                        ' for generator: %s' %
                        (core.bayesdb_generator_name(bdb, generator_id),))
                X_L_list = [theta['X_L'] for theta in thetas]
                X_D_list = [theta['X_D'] for theta in thetas]
                # XXX It would be nice to take advantage of Crosscat's
                # internal timer to avoid transferring states between
                # Python and C++ more often than is necessary, but it
                # doesn't report back to us the number of iterations
                # actually performed.
                iterations_in_ckpt = 0
                while True:
                    X_L_list_0 = X_L_list
                    X_L_list, X_D_list, diagnostics = self._crosscat.analyze(
                        seed=crosscat_seed(bdb),
                        M_c=M_c,
                        T=T,
                        do_diagnostics=True,
                        # XXX Require the models share a common kernel_list.
                        kernel_list=thetas[0]['model_config']['kernel_list'],
                        X_L=X_L_list,
                        X_D=X_D_list,
                        n_steps=n_steps,
                    )
                    iterations_in_ckpt += n_steps
                    if iterations is not None:
                        assert n_steps <= iterations
                        iterations -= n_steps
                        if iterations == 0:
                            break
                    if ckpt_iterations is not None:
                        if ckpt_iterations <= iterations_in_ckpt:
                            break
                    elif ckpt_seconds is not None:
                        if ckpt_deadline < time.time():
                            break
                    else:
                        break
                cc_cache = self._crosscat_cache(bdb)
                for i, (modelno, theta, X_L, X_D) \
                        in enumerate(
                            zip(update_modelnos, thetas, X_L_list, X_D_list)):
                    theta['iterations'] += iterations_in_ckpt
                    theta['X_L'] = X_L
                    theta['X_D'] = X_D
                    total_changes = bdb._sqlite3.totalchanges()
                    bdb.sql_execute(update_iterations_sql, {
                        'generator_id': generator_id,
                        'modelno': modelno,
                        'iterations': iterations_in_ckpt,
                    })
                    assert bdb._sqlite3.totalchanges() - total_changes == 1
                    total_changes = bdb._sqlite3.totalchanges()
                    self._theta_validator.validate(theta)
                    bdb.sql_execute(update_theta_json_sql, {
                        'generator_id': generator_id,
                        'modelno': modelno,
                        'theta_json': json.dumps(theta),
                    })
                    assert bdb._sqlite3.totalchanges() - total_changes == 1
                    checkpoint_sql = '''
                        SELECT 1 + MAX(checkpoint)
                            FROM bayesdb_crosscat_diagnostics
                            WHERE generator_id = :generator_id
                                AND modelno = :modelno
                    '''
                    cursor = bdb.sql_execute(checkpoint_sql, {
                        'generator_id': generator_id,
                        'modelno': modelno,
                    })
                    checkpoint = cursor_value(cursor)
                    if checkpoint is None:
                        checkpoint = 0
                    assert isinstance(checkpoint, int)
                    assert 0 < len(diagnostics['logscore'])
                    assert i < len(diagnostics['logscore'][-1])
                    assert diagnostics['logscore'][-1][i] is not None
                    assert not math.isnan(diagnostics['logscore'][-1][i])
                    assert 0 < len(diagnostics['num_views'])
                    assert 0 < len(diagnostics['column_crp_alpha'])
                    bdb.sql_execute(insert_diagnostics_sql, {
                        'generator_id': generator_id,
                        'modelno': modelno,
                        'checkpoint': checkpoint,
                        'logscore': diagnostics['logscore'][-1][i],
                        'num_views': diagnostics['num_views'][-1][i],
                        'column_crp_alpha':
                            diagnostics['column_crp_alpha'][-1][i],
                        'iterations': theta['iterations'],
                    })
                    if cc_cache is not None:
                        if generator_id in cc_cache.thetas:
                            cc_cache.thetas[generator_id][modelno] = theta
                        else:
                            cc_cache.thetas[generator_id] = {modelno: theta}
                if ckpt_seconds is not None:
                    ckpt_deadline = time.time() + ckpt_seconds

    def column_dependence_probability(self, bdb, generator_id, modelno,
            colno0, colno1):
        if colno0 == colno1:
            return 1
        cc_colno0 = crosscat_cc_colno(bdb, generator_id, colno0)
        cc_colno1 = crosscat_cc_colno(bdb, generator_id, colno1)
        count = 0
        nmodels = 0
        for X_L, X_D in self._crosscat_latent_stata(bdb, generator_id,
                modelno):
            nmodels += 1
            assignments = X_L['column_partition']['assignments']
            if assignments[cc_colno0] != assignments[cc_colno1]:
                continue
            count += 1
        return float('NaN') if nmodels == 0 else (float(count)/float(nmodels))

    def column_mutual_information(self, bdb, generator_id, modelno, colno0,
            colno1, numsamples=None):
        if numsamples is None:
            numsamples = 100
        X_L_list = self._crosscat_latent_state(bdb, generator_id, modelno)
        X_D_list = self._crosscat_latent_data(bdb, generator_id, modelno)
        cc_colno0 = crosscat_cc_colno(bdb, generator_id, colno0)
        cc_colno1 = crosscat_cc_colno(bdb, generator_id, colno1)
        r = self._crosscat.mutual_information(
            seed=crosscat_seed(bdb),
            M_c=self._crosscat_metadata(bdb, generator_id),
            X_L_list=X_L_list,
            X_D_list=X_D_list,
            Q=[(cc_colno0, cc_colno1)],
            n_samples=int(math.ceil(float(numsamples) / len(X_L_list)))
        )
        # r has one answer per element of Q, so take the first one.
        r0 = r[0]
        # r0 is (mi, linfoot), and we want mi.
        mi = r0[0]
        # mi is [result for model 0, result for model 1, ...], and we want
        # the mean.
        return arithmetic_mean(mi)

    def row_similarity(self, bdb, generator_id, modelno, rowid, target_rowid,
            colnos):
        X_L_list = self._crosscat_latent_state(bdb, generator_id, modelno)
        X_D_list = self._crosscat_latent_data(bdb, generator_id, modelno)
        [given_row_id, target_row_id], X_L_list, X_D_list = \
            self._crosscat_get_rows(bdb, generator_id, [rowid, target_rowid],
                X_L_list, X_D_list)
        return self._crosscat.similarity(
            M_c=self._crosscat_metadata(bdb, generator_id),
            X_L_list=X_L_list,
            X_D_list=X_D_list,
            given_row_id=given_row_id,
            target_row_id=target_row_id,
            target_columns=[crosscat_cc_colno(bdb, generator_id, colno)
                for colno in colnos],
        )

    def predict_confidence(self, bdb, generator_id, modelno, colno, rowid,
            numsamples=None):
        if numsamples is None:
            numsamples = 100    # XXXWARGHWTF
        M_c = self._crosscat_metadata(bdb, generator_id)
        row = core.bayesdb_generator_row_values(bdb, generator_id, rowid)
        X_L_list = self._crosscat_latent_state(bdb, generator_id, modelno)
        X_D_list = self._crosscat_latent_data(bdb, generator_id, modelno)
        row_id, X_L_list, X_D_list = \
            self._crosscat_get_row(bdb, generator_id, rowid, X_L_list,
                X_D_list)
        cc_colno = crosscat_cc_colno(bdb, generator_id, colno)
        code, confidence = self._crosscat.impute_and_confidence(
            seed=crosscat_seed(bdb),
            M_c=M_c,
            X_L=X_L_list,
            X_D=X_D_list,
            Y=[(row_id,
                cc_colno_,
                crosscat_value_to_code(bdb, generator_id, M_c,
                    crosscat_gen_colno(bdb, generator_id, cc_colno_), value))
               for cc_colno_, value in enumerate(row)
               if value is not None
               if cc_colno_ != cc_colno],
            Q=[(row_id, cc_colno)],
            n=numsamples,
        )
        value = crosscat_code_to_value(bdb, generator_id, M_c, colno, code)
        return value, confidence

    def simulate_joint(self, bdb, generator_id, targets, constraints,
            modelno, num_predictions=1):
        M_c = self._crosscat_metadata(bdb, generator_id)
        X_L_list = self._crosscat_latent_state(bdb, generator_id, modelno)
        X_D_list = self._crosscat_latent_data(bdb, generator_id, modelno)
        Q, Y, X_L_list, X_D_list = self._crosscat_remap_two(
            bdb, generator_id, X_L_list, X_D_list, targets, constraints)
        raw_outputs = self._crosscat.simple_predictive_sample(
            seed=crosscat_seed(bdb),
            M_c=M_c,
            X_L=X_L_list,
            X_D=X_D_list,
            Y=Y,
            Q=Q,
            n=num_predictions
        )
        return [[crosscat_code_to_value(bdb, generator_id, M_c, colno, code)
                for ((_, colno), code) in zip(targets, raw_output)]
            for raw_output in raw_outputs]

    def logpdf_joint(self, bdb, generator_id, targets, constraints,
            modelno=None):
        M_c = self._crosscat_metadata(bdb, generator_id)
        try:
            for _, colno, value in constraints:
                crosscat_value_to_code(bdb, generator_id, M_c, colno, value)
        except KeyError:
            # Probability with constraint that has no code
            return float('nan')
        try:
            for _, colno, value in targets:
                crosscat_value_to_code(bdb, generator_id, M_c, colno, value)
        except KeyError:
            # Probability of value that has no code
            return float('-inf')
        X_L_list = self._crosscat_latent_state(bdb, generator_id, modelno)
        X_D_list = self._crosscat_latent_data(bdb, generator_id, modelno)
        Q, Y, X_L_list, X_D_list = self._crosscat_remap_two(
            bdb, generator_id, X_L_list, X_D_list, targets, constraints)
        r = self._crosscat.predictive_probability_multistate(
            M_c=M_c,
            X_L_list=X_L_list,
            X_D_list=X_D_list,
            Y=Y,
            Q=Q,
        )
        return r

    def insertmany(self, bdb, generator_id, rows):
        with bdb.savepoint():
            # Insert the data into the table.
            table_name = core.bayesdb_generator_table(bdb, generator_id)
            qt = sqlite3_quote_name(table_name)
            sql_column_names = core.bayesdb_table_column_names(bdb, table_name)
            qcns = map(sqlite3_quote_name, sql_column_names)
            sql = '''
                INSERT INTO %s (%s) VALUES (%s)
            ''' % (qt, ', '.join(qcns), ', '.join('?' for _qcn in qcns))
            for row in rows:
                if len(row) != len(sql_column_names):
                    raise BQLError(bdb, 'Wrong row length'
                        ': expected %d, got %d' %
                        (len(sql_column_names), len(row)))
                bdb.sql_execute(sql, row)

            # Find the indices of the modelled columns.
            # XXX Simplify this -- we have the correspondence between
            # colno and modelled_colno in the database.
            modelled_column_names = \
                core.bayesdb_generator_column_names(bdb, generator_id)
            remap = []
            for i, name in enumerate(sql_column_names):
                colno = len(remap)
                if len(modelled_column_names) <= colno:
                    break
                if casefold(name) == casefold(modelled_column_names[colno]):
                    remap.append(i)
            assert len(remap) == len(modelled_column_names)
            M_c = self._crosscat_metadata(bdb, generator_id)
            modelled_rows = [[crosscat_value_to_code(bdb, generator_id, M_c,
                        colno, row[i])
                    for colno, i in enumerate(remap)]
                for row in rows]

            # Update the models.
            T = self._crosscat_data(bdb, generator_id, M_c)
            models_sql = '''
                SELECT m.modelno, ct.theta_json
                    FROM bayesdb_generator_model AS m,
                        bayesdb_crosscat_theta AS ct
                    WHERE m.generator_id = ?
                        AND m.generator_id = ct.generator_id
                        AND m.modelno = ct.modelno
                    ORDER BY m.modelno
            '''
            models = bdb.sql_execute(models_sql, (generator_id,)).fetchall()
            modelnos = [modelno for modelno, _theta_json in models]
            thetas = [json.loads(theta_json)
                for _modelno, theta_json in models]
            X_L_list, X_D_list, T = self._crosscat.insert(
                M_c=M_c,
                T=T,
                X_L_list=[theta['X_L'] for theta in thetas],
                X_D_list=[theta['X_D'] for theta in thetas],
                new_rows=modelled_rows,
            )
            assert T == self._crosscat_data(bdb, generator_id, M_c) \
                + modelled_rows
            update_theta_sql = '''
                UPDATE bayesdb_crosscat_theta SET theta_json = :theta_json
                    WHERE generator_id = :generator_id AND modelno = :modelno
            '''
            for modelno, theta, X_L, X_D \
                    in zip(modelnos, thetas, X_L_list, X_D_list):
                theta['X_L'] = X_L
                theta['X_D'] = X_D
                total_changes = bdb._sqlite3.totalchanges()
                self._theta_validator.validate(theta)
                bdb.sql_execute(update_theta_sql, {
                    'generator_id': generator_id,
                    'modelno': modelno,
                    'theta_json': json.dumps(theta),
                })
                assert bdb._sqlite3.totalchanges() - total_changes == 1

class CrosscatCache(object):
    def __init__(self):
        self.metadata = {}
        self.thetas = {}

def create_metadata(bdb, generator_id, column_list):
    ncols = len(column_list)
    column_names = [name for _colno, name, _stattype in column_list]
    column_metadata = [metadata_creators[casefold(stattype)](bdb, generator_id,
            colno)
        for colno, _name, stattype in column_list]
    return {
        'name_to_idx': dict(zip(column_names, range(ncols))),
        'idx_to_name': dict(zip(map(unicode, range(ncols)), column_names)),
        'column_metadata': column_metadata,
    }

def create_metadata_numerical(_bdb, _generator_id, _colno):
    return {
        'modeltype': 'normal_inverse_gamma',
        'value_to_code': {},
        'code_to_value': {},
    }

def create_metadata_cyclic(_bdb, _generator_id, _colno):
    return {
        'modeltype': 'vonmises',
        'value_to_code': {},
        'code_to_value': {},
    }

def create_metadata_categorical(bdb, generator_id, colno):
    table = core.bayesdb_generator_table(bdb, generator_id)
    column_name = core.bayesdb_table_column_name(bdb, table, colno)
    qt = sqlite3_quote_name(table)
    qcn = sqlite3_quote_name(column_name)
    sql = '''
        SELECT DISTINCT CAST(%s AS TEXT)
            FROM %s WHERE %s IS NOT NULL ORDER BY %s
    ''' % (qcn, qt, qcn, qcn)
    cursor = bdb.sql_execute(sql)
    codes = [row[0] for row in cursor]
    assert all(isinstance(value, unicode) for value in codes)
    ncodes = len(codes)
    return {
        'modeltype': 'symmetric_dirichlet_discrete',
        'value_to_code': dict(zip(range(ncodes), codes)),
        'code_to_value': dict(zip(codes, range(ncodes))),
    }

metadata_creators = {
    'categorical': create_metadata_categorical,
    'cyclic': create_metadata_cyclic,
    'numerical': create_metadata_numerical,
}

def crosscat_value_to_code(bdb, generator_id, M_c, colno, value):
    stattype = core.bayesdb_generator_column_stattype(bdb, generator_id, colno)
    if stattype == 'categorical':
        # For hysterical raisins, code_to_value and value_to_code are
        # backwards.
        #
        # XXX Fix this.
        if value is None:
            return float('NaN')         # XXX !?!??!
        cc_colno = crosscat_cc_colno(bdb, generator_id, colno)
        key = unicode(value)
        code = M_c['column_metadata'][cc_colno]['code_to_value'][key]
        # XXX Crosscat expects floating-point codes.
        return float(code)
    elif stattype in ('cyclic', 'numerical'):
        # Data may be stored in the SQL table as strings, if imported
        # from wacky sources like CSV files, in which case both NULL
        # and non-numerical data -- including the string `nan' which
        # makes sense, and anything else which doesn't -- will be
        # represented by NaN.
        try:
            return float(value)
        except (ValueError, TypeError):
            return float('NaN')
    else:
        raise KeyError

def crosscat_code_to_value(bdb, generator_id, M_c, colno, code):
    stattype = core.bayesdb_generator_column_stattype(bdb, generator_id, colno)
    if stattype == 'categorical':
        if math.isnan(code):
            return None
        cc_colno = crosscat_cc_colno(bdb, generator_id, colno)
        # XXX Whattakludge.
        key = unicode(int(code))
        return M_c['column_metadata'][cc_colno]['value_to_code'][key]
    elif stattype in ('cyclic', 'numerical'):
        if math.isnan(code):
            return None
        return code
    else:
        raise KeyError

def crosscat_cc_colno(bdb, generator_id, colno):
    sql = '''
        SELECT cc_colno FROM bayesdb_crosscat_column
            WHERE generator_id = ? AND colno = ?
    '''
    cursor = bdb.sql_execute(sql, (generator_id, colno))
    try:
        row = cursor.next()
    except StopIteration:
        generator = core.bayesdb_generator_name(bdb, generator_id)
        colname = core.bayesdb_generator_column_name(bdb, generator_id, colno)
        raise BQLError(bdb, 'Column not modelled in generator %s: %s' %
            (repr(generator), repr(colname)))
    else:
        assert len(row) == 1
        assert isinstance(row[0], int)
        return row[0]

def crosscat_gen_colno(bdb, generator_id, cc_colno):
    sql = '''
        SELECT colno FROM bayesdb_crosscat_column
            WHERE generator_id = ? AND cc_colno = ?
    '''
    cursor = bdb.sql_execute(sql, (generator_id, cc_colno))
    try:
        row = cursor.next()
    except StopIteration:
        generator = core.bayesdb_generator_name(bdb, generator_id)
        colname = core.bayesdb_generator_column_name(bdb, generator_id,
            cc_colno)
        raise BQLError(bdb, 'Column not Crosscat-modelled'
            ' in generator %s: %s' % (repr(generator), repr(colname)))
    else:
        assert len(row) == 1
        assert isinstance(row[0], int)
        return row[0]

def crosscat_gen_column_dependencies(bdb, generator_id):
    sql = '''
        SELECT colno0, colno1, dependent
            FROM bayesdb_crosscat_column_dependency
            WHERE generator_id = ?
    '''
    return bdb.sql_execute(sql, (generator_id,)).fetchall()

def crosscat_seed(bdb):
    # XXX Pass a 32-byte seed from weakprng once Crosscat supports
    # that.  Crosscat Github issue #93:
    # https://github.com/probcomp/crosscat/issues/93
    return bdb.py_prng.randrange(2**32)
