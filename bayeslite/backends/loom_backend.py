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

"""Implementation of bayeslite.BayesDB_Backend interface using Loom.

The Loom backend serves as an interface between BayesDB and the Loom
implementation of CrossCat: https://github.com/posterior/loom
"""

import csv
import gzip
import itertools
import json
import os
import tempfile

from StringIO import StringIO
from collections import Counter
from collections import OrderedDict
from datetime import datetime

import loom.tasks

from distributions.io.stream import open_compressed
from loom.cFormat import assignment_stream_load

from bayeslite.core import bayesdb_generator_name
from bayeslite.core import bayesdb_generator_population
from bayeslite.core import bayesdb_population_fresh_row_id
from bayeslite.core import bayesdb_population_row_values
from bayeslite.core import bayesdb_population_table
from bayeslite.core import bayesdb_table_column_number
from bayeslite.core import bayesdb_variable_name
from bayeslite.core import bayesdb_variable_numbers
from bayeslite.core import bayesdb_variable_stattype

from bayeslite.backend import BayesDB_Backend
from bayeslite.backend import bayesdb_backend_version

from bayeslite.exception import BQLError
from bayeslite.sqlite3_util import sqlite3_quote_name

from bayeslite.util import casefold
from bayeslite.util import cursor_row
from bayeslite.util import cursor_value


LOOM_SCHEMA_1 = '''
INSERT INTO bayesdb_backend (name, version)
    VALUES (?, 1);

CREATE TABLE bayesdb_loom_generator (
    generator_id    INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    name            VARCHAR(64) NOT NULL,
    loom_store_path    VARCHAR(64) NOT NULL,
    PRIMARY KEY(generator_id)
);

CREATE TABLE bayesdb_loom_generator_model_info (
    generator_id    INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    num_models      INTEGER NOT NULL,
    PRIMARY KEY(generator_id)
);

CREATE TABLE bayesdb_loom_string_encoding (
    generator_id    INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    colno           INTEGER NOT NULL,
    string_form     VARCHAR(64) NOT NULL,
    integer_form    INTEGER NOT NULL,
    PRIMARY KEY(generator_id, colno, integer_form)
);

CREATE TABLE bayesdb_loom_column_ordering (
    generator_id    INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    colno           INTEGER NOT NULL,
    rank            INTEGER NOT NULL,
    PRIMARY KEY(generator_id, colno)
);

CREATE TABLE bayesdb_loom_column_kind_partition (
    generator_id    INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    modelno         INTEGER NOT NULL,
    colno           INTEGER NOT NULL,
    kind_id         INTEGER NOT NULL,
    PRIMARY KEY(generator_id, modelno, colno)
);

CREATE TABLE bayesdb_loom_row_kind_partition (
    generator_id    INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    modelno         INTEGER NOT NULL,
    rowid           INTEGER NOT NULL,
    kind_id         INTEGER NOT NULL,
    partition_id    INTEGER NOT NULL,
    PRIMARY KEY(generator_id, modelno, rowid, kind_id)
);
'''

CSV_DELIMITER = ','

STATTYPE_TO_LOOMTYPE = {
    'unbounded_nominal'    : 'dpd',
    'counts'               : 'gp',
    'boolean'              : 'bb',
    'nominal'              : 'dd',
    'cyclic'               : 'nich',
    'numerical'            : 'nich',
}


class LoomBackend(BayesDB_Backend):
    """Loom backend for BayesDB.

    The backend is named ``loom`` in BQL ::

        CREATE GENERATOR t_nig FOR t USING loom

    Internally, the Loom backend add SQL tables to the database with names that
    begin with ``bayesdb_loom``.
    """

    def __init__(self, loom_store_path):
        """Initialize the Loom backend.

        `loom_store_path` is the absolute path at which loom stores its
        auxiliary data files.
        """
        if not os.path.isabs(loom_store_path):
            raise ValueError('Loom store path must be an absolute path.')
        self.loom_store_path = loom_store_path
        os.environ['LOOM_STORE'] = self.loom_store_path
        if not os.path.isdir(self.loom_store_path):
            os.makedirs(self.loom_store_path)
        # The cache is a dictionary whose keys are bayeslite.BayesDB objects,
        # and whose values are dictionaries (one cache per bdb). We need
        # self._cache to have separate caches for each bdb because the same
        # instance of LoomBackend may be used across multiple bdb instances.
        self._cache = dict()


    def name(self):
        return 'loom'

    def register(self, bdb):
        with bdb.savepoint():
            version = bayesdb_backend_version(bdb, self.name())
            if version is None:
                bdb.sql_execute(LOOM_SCHEMA_1, (self.name(),))
                version = 1

    def create_generator(self, bdb, generator_id, schema, **kwargs):
        population_id = bayesdb_generator_population(bdb, generator_id)
        table = bayesdb_population_table(bdb, population_id)

        # Store generator info in bdb.
        name = self._generate_name(bdb, generator_id)
        bdb.sql_execute('''
            INSERT INTO bayesdb_loom_generator
            (generator_id, name, loom_store_path)
            VALUES (?, ?, ?)
        ''', (generator_id, name, self.loom_store_path))

        headers = []
        data = []
        data_by_column = {}
        for colno in bayesdb_variable_numbers(bdb, population_id, None):
            column_name = bayesdb_variable_name(bdb, population_id, None, colno)
            headers.append(column_name)
            qt = sqlite3_quote_name(table)
            qcn = sqlite3_quote_name(column_name)
            cursor = bdb.sql_execute('SELECT %s FROM %s' % (qcn, qt))
            col_data = [item for (item,) in cursor.fetchall()]
            data.append(col_data)
            data_by_column[column_name] = col_data
        data = [list(i) for i in zip(*data)]

        # Ingest data into loom.
        schema_file = self._data_to_schema(bdb, population_id, data_by_column)
        csv_file = self._data_to_csv(bdb, headers, data)
        loom.tasks.ingest(
            self._get_loom_project_path(bdb, generator_id),
            rows_csv=csv_file.name, schema=schema_file.name)

        # Store encoding info in bdb.
        self._store_encoding_info(bdb, generator_id)

    def _store_encoding_info(self, bdb, generator_id):
        encoding_path = os.path.join(
            self._get_loom_project_path(bdb, generator_id),
            'ingest', 'encoding.json.gz'
        )
        with gzip.open(encoding_path) as encoding_file:
            encoding = json.loads(encoding_file.read().decode('ascii'))

        population_id = bayesdb_generator_population(bdb, generator_id)
        table = bayesdb_population_table(bdb, population_id)

        # Store string encoding.
        insert_string_encoding = '''
            INSERT INTO bayesdb_loom_string_encoding
            (generator_id, colno, string_form, integer_form)
            VALUES (:generator_id, :colno, :string_form, :integer_form)
        '''
        for col in encoding:
            if 'symbols' in col:
                colno = bayesdb_table_column_number(bdb, table, str(col['name']))
                for string_form, integer_form in col['symbols'].iteritems():
                    bdb.sql_execute(insert_string_encoding, {
                        'generator_id': generator_id,
                        'colno': colno,
                        'string_form': string_form,
                        'integer_form': integer_form
                    })

        # Store ordering of columns.
        insert_order_sql = '''
            INSERT INTO bayesdb_loom_column_ordering
            (generator_id, colno, rank)
            VALUES (:generator_id, :colno, :rank)
        '''
        for col_index in xrange(len(encoding)):
            colno = bayesdb_table_column_number(
                bdb, table, str(encoding[col_index]['name']))
            bdb.sql_execute(insert_order_sql, {
                'generator_id': generator_id,
                'colno': colno,
                'rank': col_index
            })

    def _check_loom_initialized(self, bdb, generator_id):
        # Not invoked on a per-query basis due to high overhead.
        cursor = bdb.sql_execute('''
            SELECT COUNT(*)
            FROM bayesdb_loom_row_kind_partition
            WHERE generator_id = ?
        ''', (generator_id,))
        count_row = cursor.fetchall()
        cursor = bdb.sql_execute('''
            SELECT COUNT(*)
            FROM bayesdb_loom_row_kind_partition
            WHERE generator_id = ?
        ''',(generator_id,))
        count_col = cursor.fetchall()
        if count_row[0][0] == 0 or count_col[0][0] == 0:
            raise BQLError(bdb, 'Analyze must be run before any BQL'\
                ' queries when using loom.')

    def _data_to_csv(self, bdb, headers, data):
        # TODO: Fix the use of delete=False so loom doesn't litter
        #   the filesystem with the files used to communicate with loom.
        with tempfile.NamedTemporaryFile(delete=False) as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=CSV_DELIMITER)
            csv_writer.writerow(headers)
            for row in data:
                processed_row = []
                for elem in row:
                    if elem is None:
                        processed_row.append('')
                    elif isinstance(elem, unicode):
                        processed_row.append(elem.encode('ascii', 'ignore'))
                    else:
                        processed_row.append(elem)
                csv_writer.writerow(processed_row)
        return csv_file

    def _data_to_schema(self, bdb, population_id, data_by_column):
        json_dict = {}
        for colno in bayesdb_variable_numbers(bdb, population_id, None):
            column_name = bayesdb_variable_name(bdb, population_id, None, colno)
            stattype = bayesdb_variable_stattype(bdb, population_id, None, colno)
            if stattype == 'nominal' \
                    and len(set(data_by_column[column_name])) > 256:
                stattype = 'unbounded_nominal'
            json_dict[column_name] = STATTYPE_TO_LOOMTYPE[stattype]
        with tempfile.NamedTemporaryFile(delete=False) as schema_file:
            schema_file.write(json.dumps(json_dict))
        return schema_file

    def _generate_name(self, bdb, generator_id):
        generator_name = bayesdb_generator_name(bdb, generator_id)
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        return '%s_%s' % (timestamp, generator_name)

    def _get_name(self, bdb, generator_id):
        return cursor_value(bdb.sql_execute('''
            SELECT name FROM bayesdb_loom_generator
            WHERE generator_id = ?
        ''', (generator_id,)))

    def _get_loom_project_path(self, bdb, generator_id):
        cursor = bdb.sql_execute('''
            SELECT name, loom_store_path
            FROM bayesdb_loom_generator
            WHERE generator_id = ?
        ''', (generator_id,))
        name, loom_store_path = cursor_row(cursor)
        return os.path.join(loom_store_path, name)

    def initialize_models(self, bdb, generator_id, modelnos):
        cursor = bdb.sql_execute('''
            SELECT num_models
            FROM bayesdb_loom_generator_model_info
            WHERE generator_id = ?
        ''', (generator_id,))
        num_existing = cursor.fetchall()
        if num_existing is None or len(num_existing) == 0:
            num_existing = 0
        else:
            num_existing = num_existing[0][0]
        bdb.sql_execute('''
            INSERT OR REPLACE INTO bayesdb_loom_generator_model_info
            (generator_id, num_models)
            VALUES (?, ?)
        ''', (generator_id, len(modelnos) + num_existing))
        self.analyze_models(bdb, generator_id, iterations=1)

    def _get_num_models(self, bdb, generator_id):
        cursor = bdb.sql_execute('''
            SELECT num_models
            FROM bayesdb_loom_generator_model_info
            WHERE generator_id = ?
        ''', (generator_id,))
        return cursor_value(cursor)

    def drop_generator(self, bdb, generator_id):
        self._del_cache_entry(bdb, generator_id, None)
        with bdb.savepoint():
            self.drop_models(bdb, generator_id)
            bdb.sql_execute('''
                DELETE FROM bayesdb_loom_generator
                WHERE generator_id = ?
            ''', (generator_id,))
            bdb.sql_execute('''
                DELETE FROM bayesdb_loom_generator_model_info
                WHERE generator_id = ?
            ''', (generator_id,))
            bdb.sql_execute('''
                DELETE FROM bayesdb_loom_string_encoding
                WHERE generator_id = ?
            ''', (generator_id,))
            bdb.sql_execute('''
                DELETE FROM bayesdb_loom_column_ordering
                WHERE generator_id = ?
            ''', (generator_id,))

    def drop_models(self, bdb, generator_id, modelnos=None):
        with bdb.savepoint():
            if modelnos is not None:
                raise BQLError(bdb, 'Loom cannot drop specific model numbers.')
            bdb.sql_execute('''
                DELETE FROM bayesdb_loom_column_kind_partition
                WHERE generator_id = ?
            ''', (generator_id,))
            bdb.sql_execute('''
                DELETE FROM bayesdb_loom_row_kind_partition
                WHERE generator_id = ?
            ''', (generator_id,))
            q_server = self._get_cache_entry(bdb, generator_id, 'q_server')
            if q_server is not None:
                q_server.close()
            preql_server = self._get_cache_entry(
                bdb, generator_id, 'preql_server')
            if preql_server is not None:
                preql_server.close()
            self._del_cache_entry(bdb, generator_id, 'q_server')
            self._del_cache_entry(bdb, generator_id, 'preql_server')
            bdb.sql_execute('''
                UPDATE bayesdb_loom_generator_model_info
                SET num_models = 0
                WHERE generator_id = ?
            ''', (generator_id,))
            project_path = self._get_loom_project_path(bdb, generator_id)
            paths = loom.store.get_paths(project_path)
            if 'root' in paths:
                folder_with_models = os.path.join(paths['root'], 'samples')
                # XXX Change to subprocess.check_call
                os.system('rm -rf {}'.format(folder_with_models))

    def analyze_models(self, bdb, generator_id, modelnos=None, iterations=1,
            max_seconds=None, ckpt_iterations=None, ckpt_seconds=None,
            program=None):
        if max_seconds is not None:
            raise BQLError(bdb,
                'Loom analyze does not support number of seconds.')
        if ckpt_iterations is not None or ckpt_seconds is not None:
            raise BQLError(bdb, 'Loom analyze does not support checkpoint.')
        if program is not None:
            raise BQLError(bdb, 'Loom analyze does not support programs.')
        if modelnos is not None:
            raise BQLError(bdb, 'Loom cannot analyze specific model numbers.')

        num_models = (self._get_num_models(bdb, generator_id))
        iterations = max(int(iterations), 1)
        config = {'schedule': {'extra_passes': iterations}}
        project_path = self._get_loom_project_path(bdb, generator_id)

        loom.tasks.infer(project_path, sample_count=num_models, config=config)

        self._store_kind_partition(bdb, generator_id, modelnos)
        self._set_cache_entry(bdb, generator_id, 'q_server',
            loom.query.get_server(
                self._get_loom_project_path(bdb, generator_id)))
        preqlServer = loom.tasks.query(
                self._get_loom_project_path(bdb, generator_id))
        self._set_cache_entry(bdb, generator_id, 'preql_server', preqlServer)

    def _store_kind_partition(self, bdb, generator_id, modelnos):
        population_id = bayesdb_generator_population(bdb, generator_id)
        if modelnos is None:
            modelnos = range(self._get_num_models(bdb, generator_id))
        with bdb.savepoint():
            for modelno in modelnos:
                column_partition = self._retrieve_column_partition(
                    bdb, generator_id, modelno)

                # Bulk insertion of mapping from colno to kind_id.
                colnos = bayesdb_variable_numbers(bdb, population_id, None)
                ranks = [self._get_loom_rank(bdb, generator_id, colno)
                    for colno in colnos]
                insertions = ','.join(
                    str((generator_id, modelno, colno, column_partition[rank]))
                    for colno, rank in zip(colnos, ranks)
                )
                bdb.sql_execute('''
                    INSERT OR REPLACE INTO bayesdb_loom_column_kind_partition
                    (generator_id, modelno, colno, kind_id)
                    VALUES %s
                ''' % (insertions,))

                # Bulk insertion of mapping from (kind_id, rowid) to cluster_id.
                row_partition = self._retrieve_row_partition(
                    bdb, generator_id, modelno)
                insertions = ','.join(
                    str((generator_id, modelno, rowid+1, kind_id, partition_id))
                    for kind_id in row_partition
                    for rowid, partition_id in enumerate(row_partition[kind_id]))
                bdb.sql_execute('''
                    INSERT OR REPLACE INTO
                        bayesdb_loom_row_kind_partition
                    (generator_id, modelno, rowid, kind_id, partition_id)
                    VALUES %s
                ''' % (insertions,))

    def _retrieve_column_partition(self, bdb, generator_id, modelno):
        """Return column partition from a CrossCat model.

        The returned structure is of the form `cgpm.crosscat.state.State.Zv`.
        """
        cross_cat = self._get_cross_cat(bdb, generator_id, modelno)
        return dict(itertools.chain.from_iterable([
            [(loom_rank, k) for loom_rank in kind.featureids]
            for k, kind in enumerate(cross_cat.kinds)
        ]))

    def _retrieve_row_partition(self, bdb, generator_id, modelno):
        """Return row partition from a CrossCat model.

        The returned structure is of the form `cgpm.crosscat.state.State.Zv`.
        """
        cross_cat = self._get_cross_cat(bdb, generator_id, modelno)
        num_kinds = len(cross_cat.kinds)
        assign_in = os.path.join(
            self._get_loom_project_path(bdb, generator_id),
            'samples', 'sample.%d' % (modelno,), 'assign.pbs.gz')
        assignments = {
            a.rowid: [a.groupids(k) for k in xrange(num_kinds)]
            for a in assignment_stream_load(assign_in)
        }
        rowids = sorted(assignments)
        return {
            k: [assignments[rowid][k] for rowid in rowids]
            for k in xrange(num_kinds)
        }

    def _get_cross_cat(self, bdb, generator_id, modelno):
        """Return the loom CrossCat structure whose id is `modelno`."""
        model_in = os.path.join(
            self._get_loom_project_path(bdb, generator_id),
            'samples', 'sample.%d' % (modelno,), 'model.pb.gz')
        cross_cat = loom.schema_pb2.CrossCat()
        with open_compressed(model_in, 'rb') as f:
            cross_cat.ParseFromString(f.read())
        return cross_cat

    def column_dependence_probability(self,
            bdb, generator_id, modelnos, colno0, colno1):
        if modelnos is None:
            modelnos = range(self._get_num_models(bdb, generator_id))
        if colno0 == colno1:
            return [1.]
        cursor = bdb.sql_execute('''
            SELECT
                CAST(t1.kind_id = t2.kind_id AS INT)
            FROM bayesdb_loom_column_kind_partition as t1
                JOIN bayesdb_loom_column_kind_partition as t2 USING
                    (generator_id, modelno)
            WHERE t1.generator_id = ?
                AND t1.modelno in (%s)
                AND t1.colno = ?
                AND t2.colno = ?
        ''' % (','.join(map(str, modelnos)),), (generator_id, colno0, colno1))
        return [c for (c,) in cursor]

    def _get_kind_id(self, bdb, generator_id, modelno, colno):
        cursor = bdb.sql_execute('''
            SELECT kind_id
            FROM bayesdb_loom_column_kind_partition
            WHERE generator_id = ?
                AND modelno = ?
                AND colno = ?
        ''', (generator_id, modelno, colno,))
        return cursor_value(cursor)

    def _get_partition_id(self, bdb, generator_id, modelno, kind_id, rowid):
        cursor = bdb.sql_execute('''
            SELECT partition_id
            FROM bayesdb_loom_row_kind_partition
            WHERE generator_id = ?
                AND modelno = ?
                AND kind_id = ?
                AND rowid = ?
        ''', (generator_id, modelno, kind_id, rowid))
        return cursor_value(cursor)

    def column_mutual_information(self, bdb, generator_id, modelnos, colnos0,
            colnos1, constraints, numsamples):
        # XXX Why are the constraints being ignored? If Loom does not support
        # conditioning, then implement constraints using the simple Monte Carlo
        # estimator.
        population_id = bayesdb_generator_population(bdb, generator_id)
        colnames0 = [
            str(bayesdb_variable_name(bdb, population_id, None, colno))
            for colno in colnos0
        ]
        colnames1 = [
            str(bayesdb_variable_name(bdb, population_id, None, colno))
            for colno in colnos1
        ]
        server = self._get_cache_entry(bdb, generator_id, 'preql_server')
        target_set = server._cols_to_mask(server.encode_set(colnames0))
        query_set = server._cols_to_mask(server.encode_set(colnames1))
        mi = server._query_server.mutual_information(
            target_set,
            query_set,
            entropys=None,
            sample_count=loom.preql.SAMPLE_COUNT
        )
        return mi

    def row_similarity(self, bdb, generator_id, modelnos, rowid, target_rowid,
            colnos):
        if modelnos is None:
            modelnos = range(self._get_num_models(bdb, generator_id))
        assert len(colnos) == 1
        if rowid == target_rowid:
            return [1.] * len(modelnos)
        cursor = bdb.sql_execute('''
            SELECT
                CAST(t1.partition_id = t2.partition_id AS INT)
            FROM bayesdb_loom_row_kind_partition as t1
                JOIN bayesdb_loom_row_kind_partition as t2
                USING (generator_id, modelno, kind_id)
            WHERE t1.generator_id = ?
                AND t1.modelno in (%s)
                AND t1.kind_id = (
                    SELECT kind_id
                    FROM bayesdb_loom_column_kind_partition
                    WHERE generator_id = t1.generator_id
                        AND modelno = t1.modelno
                        AND colno = ?
                )
                AND t1.rowid = ?
                AND t2.rowid = ?
        ''' % (','.join(map(str, modelnos)),), (
            generator_id, colnos[0], rowid, target_rowid))
        return [c for (c,) in cursor]

    def _reorder_row(self, bdb, generator_id, row, dense=True):
        """Reorder a row of columns according to loom's column order.

        Row should be a list of (colno, value) tuples

        Returns a list of (colno, value) tuples in the proper order.
        """
        ordered_column_labels = self._get_ordered_column_labels(
            bdb, generator_id)
        ordererd_column_dict = OrderedDict(
            [(a, None) for a in ordered_column_labels])

        population_id = bayesdb_generator_population(bdb, generator_id)
        for colno, value in zip(range(1, len(row) + 1), row):
            column_name = bayesdb_variable_name(bdb, population_id, None, colno)
            ordererd_column_dict[column_name] = str(value)
        if dense is False:
            return [
                (colno, value)
                for (colno, value) in ordererd_column_dict.iteritems()
                if value is not None
            ]
        return ordererd_column_dict.iteritems()

    def predictive_relevance(self, bdb, generator_id, modelnos, rowid_target,
            rowid_queries, hypotheticals, colno):
        if len(hypotheticals) > 0:
            raise BQLError(bdb, 'Loom cannot handle hypothetical rows' \
                ' because it is unable to insert rows into CrossCat')
        if modelnos is None:
            modelnos = range(self._get_num_models(bdb, generator_id))
        relevances = [0] * len(rowid_queries)
        for modelno in modelnos:
            kind_id_context = self._get_kind_id(
                bdb, generator_id, modelno, colno)
            partition_id_target = self._get_partition_id(bdb,
                generator_id, modelno, kind_id_context, rowid_target)
            for idx, rowid in enumerate(rowid_queries):
                partition_id_query = self._get_partition_id(
                    bdb, generator_id, modelno, kind_id_context, rowid)
                if partition_id_target == partition_id_query:
                    relevances[idx] += 1
        # XXX This procedure appears to be computing the wrong thing.
        return [xsum/float(len(modelnos)) for xsum in relevances]

    def predict_confidence(self, bdb, generator_id, modelnos, rowid, colno,
            numsamples=None):
        if not numsamples:
            numsamples = 2
        assert numsamples > 0

        def _impute_categorical(sample):
            counts = Counter(s[0] for s in sample)
            mode_count = max(counts[v] for v in counts)
            pred = iter(v for v in counts if counts[v] == mode_count).next()
            conf = float(mode_count) / numsamples
            return pred, conf

        def _impute_numerical(sample):
            pred = sum(s[0] for s in sample) / float(len(sample))
            conf = 0
            return pred, conf

        # Retrieve the samples. Specifying `rowid` ensures that relevant
        # constraints are retrieved by `simulate`,
        # so provide empty constraints.
        sample = self.simulate_joint(
            bdb, generator_id, modelnos, rowid, [colno], [], numsamples)

        # Determine the imputation strategy (mode or mean).
        population_id = bayesdb_generator_population(bdb, generator_id)
        stattype = bayesdb_variable_stattype(
            bdb, population_id, None, colno)
        if _is_nominal(stattype):
            return _impute_categorical(sample)
        return _impute_numerical(sample)

    def simulate_joint(self, bdb, generator_id, modelnos, rowid, targets,
            constraints, num_samples=1, accuracy=None):
        # Retrieve the population id.
        population_id = bayesdb_generator_population(bdb, generator_id)

        # If rowid exists, retrieve conditioning data from the table.
        if rowid != bayesdb_population_fresh_row_id(bdb, generator_id):
            row_values_raw = bayesdb_population_row_values(bdb,
                population_id, rowid)
            row_values = [str(a) if isinstance(a, unicode) else a
                for a in row_values_raw]
            row = [entry for entry in enumerate(row_values)
                if entry[1] is not None]
            constraints_colnos = [c[0] for c in constraints]
            row_colnos = [r[0] for r in row]
            if any([colno in constraints_colnos for colno in row_colnos]):
                raise BQLError(bdb, 'Overlap between constraints and' \
                    'target row in simulate.')
            constraints.extend(row)

        # Prepare the query row to provide to Loom.
        row = {}
        target_num_to_name = {}
        for colno in targets:
            name = bayesdb_variable_name(bdb, generator_id, None, colno)
            target_num_to_name[colno] = name
            row[name] = ''
        for (colno, value) in constraints:
            name = bayesdb_variable_name(bdb, generator_id, None, colno)
            row[name] = value

        # Fetch the server.
        server = self._get_cache_entry(bdb, generator_id, 'preql_server')

        # Prepare the csv header.
        csv_headers, csv_values = zip(*row.iteritems())
        lower_to_upper = {str(a).lower(): str(a) for a in csv_headers}
        csv_headers = lower_to_upper.keys()
        csv_values = [str(a) for a in csv_values]

        # Retrieve the samples from the server..
        outfile = StringIO()
        writer = loom.preql.CsvWriter(outfile, returns=outfile.getvalue)
        reader = iter([csv_headers]+[csv_values])
        server._predict(reader, num_samples, writer, False)
        output = writer.result()

        # Parse output.
        returned_headers = [lower_to_upper[a] for a in
            output.strip().split('\r\n')[0].split(CSV_DELIMITER)]
        loom_output = [zip(returned_headers, a.split(CSV_DELIMITER))
            for a in output.strip().split('\r\n')[1:]]
        return_list = []
        for row in loom_output:
            # Prepare the row.
            row_values = []
            row_dict = dict(row)
            for colno in targets:
                colname = target_num_to_name[colno]
                value = row_dict[colname]
                stattype = bayesdb_variable_stattype(
                    bdb, population_id, None, colno)
                if not _is_nominal(stattype):
                    value = float(value)
                row_values.append(value)
            # Add this row to the return list.
            return_list.append(row_values)

        return return_list

    def logpdf_joint(self, bdb, generator_id, modelnos, rowid, targets,
            constraints):
        population_id = bayesdb_generator_population(bdb, generator_id)
        ordered_column_labels = self._get_ordered_column_labels(
            bdb, generator_id)

        # Pr[targets|constraints] = Pr[targest, constraints] / Pr[constraints]
        # The numerator is and_case; denominator is conditional_case.
        and_case = OrderedDict(
            [(a, None) for a in ordered_column_labels])
        conditional_case = OrderedDict(
            [(a, None) for a in ordered_column_labels])

        for (colno, value) in targets:
            column_name = bayesdb_variable_name(bdb, population_id, None, colno)
            and_case[column_name] = self._convert_to_proper_stattype(
                bdb, generator_id, colno, value)
            conditional_case[column_name] = None
        for (colno, value) in constraints:
            column_name = bayesdb_variable_name(bdb, population_id, None, colno)
            processed_value = self._convert_to_proper_stattype(
                bdb, generator_id, colno, value)

            and_case[column_name] = processed_value
            conditional_case[column_name] = processed_value

        and_case = and_case.values()
        conditional_case = conditional_case.values()

        q_server = self._get_cache_entry(bdb, generator_id, 'q_server')
        and_score = q_server.score(and_case)
        conditional_score = q_server.score(conditional_case)
        return and_score - conditional_score

    def _convert_to_proper_stattype(self, bdb, generator_id, colno, value):
        """Convert a value returned by the logpdf_joint method parameters into a
        form that Loom can handle. For instance, convert from an integer to
        real or, from a string to an integer.
        """
        if value is None:
            return value
        population_id = bayesdb_generator_population(bdb, generator_id)
        stattype = bayesdb_variable_stattype(
            bdb, population_id, None, colno)
        # If nominal then return the integer code.
        if _is_nominal(stattype):
            return self._get_integer_form(bdb, generator_id, colno, value)
        # Return the value as float.
        return float(value)

    def _get_integer_form(self, bdb, generator_id, colno, string_form):
        cursor = bdb.sql_execute('''
            SELECT integer_form
            FROM bayesdb_loom_string_encoding
            WHERE generator_id = ?
                AND colno = ?
                AND string_form = ?
        ''', (generator_id, colno, string_form,))
        return cursor_value(cursor)

    def _get_ordered_column_labels(self, bdb, generator_id):
        population_id = bayesdb_generator_population(bdb, generator_id)
        return [
            bayesdb_variable_name(bdb, population_id, None, colno)
            for colno in self._get_order(bdb, generator_id)
        ]

    def _get_loom_rank(self, bdb, generator_id, colno):
        cursor = bdb.sql_execute('''
            SELECT rank
            FROM bayesdb_loom_column_ordering
            WHERE generator_id = ?
                AND colno = ?
        ''', (generator_id, colno,))
        return cursor_value(cursor)

    def _get_order(self, bdb, generator_id):
        """Get the ordering of the columns according to loom"""
        cursor = bdb.sql_execute('''
            SELECT colno
            FROM bayesdb_loom_column_ordering
            WHERE generator_id = ?
            ORDER BY rank ASC
        ''', (generator_id,))
        return [colno for (colno,) in cursor]

    def _retrieve_cache(self, bdb,):
        if bdb in self._cache:
            return self._cache[bdb]
        self._cache[bdb] = dict()
        return self._cache[bdb]

    def _set_cache_entry(self, bdb, generator_id, key, value):
        cache = self._retrieve_cache(bdb)
        if generator_id not in cache:
            cache[generator_id] = dict()
        cache[generator_id][key] = value

    def _get_cache_entry(self, bdb, generator_id, key):
        # Returns None if the generator_id or key do not exist.
        cache = self._retrieve_cache(bdb)
        if generator_id not in cache:
            return None
        if key not in cache[generator_id]:
            return None
        return cache[generator_id][key]

    def _del_cache_entry(self, bdb, generator_id, key):
        # If key is None, wipes bdb[generator_id] in its entirety.
        cache = self._retrieve_cache(bdb)
        if generator_id in cache:
            if key is None:
                del cache[generator_id]
            elif key in cache[generator_id]:
                del cache[generator_id][key]

def _is_nominal(stattype):
    return casefold(stattype) in ['nominal', 'unbounded_nominal']
