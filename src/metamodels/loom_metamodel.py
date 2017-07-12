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

"""The Loom Metamodel serves as an interface between BayesDB
and the Loom crosscat backend: `https://github.com/posterior/loom`.

Crosscat is a fully Bayesian nonparametric method for analyzing
heterogeneous, high-dimensional data, described at
`<http://probcomp.csail.mit.edu/crosscat/>`__.

This module implements the :class:`bayeslite.IBayesDBMetamodel`
interface for the Loom Model.
"""
import collections
import csv
import datetime
import gzip
import json
import itertools
import os
import os.path
import tempfile
import time

from StringIO import StringIO
from collections import Counter

import loom.tasks

import bayeslite.core as core
import bayeslite.metamodel as metamodel

from bayeslite.metamodel import bayesdb_metamodel_version
from bayeslite.sqlite3_util import sqlite3_quote_name
from bayeslite.util import casefold
from distributions.io.stream import open_compressed

# TODO should we use "generator" or "metamodel" in the name of
# "bayesdb_loom_generator"

USE_TIMESTAMP = True
LOOM_SCHEMA_1 = '''
INSERT INTO bayesdb_metamodel (name, version)
    VALUES (?, 1);

CREATE TABLE bayesdb_loom_generator (
    generator_id    INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    name            VARCHAR(64) NOT NULL,
    PRIMARY KEY(generator_id)
);

CREATE TABLE bayesdb_loom_string_encoding (
    generator_id    INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    colno           INTEGRER NOT NULL,
    string_form     VARCHAR(64) NOT NULL,
    integer_form    INTEGRER NOT NULL,
    PRIMARY KEY(generator_id, colno, integer_form)
);

CREATE TABLE bayesdb_loom_column_ordering (
    generator_id    INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    colno           INTEGRER NOT NULL,
    rank            INTEGRER NOT NULL,
    PRIMARY KEY(generator_id, colno)
);

CREATE TABLE bayesdb_loom_kind_partition (
    generator_id    INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    modelno         INTEGRER NOT NULL,
    colno           INTEGRER NOT NULL,
    kind_id          INTEGRER NOT NULL,
    PRIMARY KEY(generator_id, modelno, colno)
);
'''

CSV_DELIMITER = ','

# TODO fill out
# TODO optimize number of bdb calls
STATTYPE_TO_LOOMTYPE = {'categorical': 'dd', 'cyclic': 'nich', 'numerical': 'nich'}


class LoomMetamodel(metamodel.IBayesDBMetamodel):
    """Loom metamodel for BayesDB.

    The metamodel is named ``loom`` in BQL::

        CREATE GENERATOR t_nig FOR t USING loom

    Internally, the Loom metamodel add SQL tables to the
    database with names that begin with ``bayesdb_loom``.
    """

    def __init__(self, data_path =
            '/scratch/mntruell/venv/lib/python2.7/site-packages/data/'):
        """Initialize the loom metamodel

        `data_path` is the absolute path at which loom stores its data files
        """
        self.data_path = data_path

    def name(self):
        return 'loom'

    def register(self, bdb):
        with bdb.savepoint():
            version = bayesdb_metamodel_version(bdb, self.name())
            if version is None:
                bdb.sql_execute(LOOM_SCHEMA_1, (self.name(),))
                version = 1

    def create_generator(self, bdb, generator_id, schema, **kwargs):
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        table = core.bayesdb_population_table(bdb, population_id)

        # Store generator info in bdb
        insert_generator_sql = '''
            INSERT INTO bayesdb_loom_generator
                (generator_id, name)
                VALUES (%d, "%s-%s")
        ''' % (generator_id,
                datetime.datetime.fromtimestamp(time.time())
                .strftime('%Y%m%d-%H%M%S.%f') if USE_TIMESTAMP else
                '',
                core.bayesdb_generator_name(bdb, generator_id))
        bdb.sql_execute(insert_generator_sql)

        # Collect data from into list form
        headers = []
        data = []
        for colno in core.bayesdb_variable_numbers(bdb, population_id, None):
            column_name = core.bayesdb_variable_name(bdb, population_id, colno)
            headers.append(column_name)

            qt = sqlite3_quote_name(table)
            qcn = sqlite3_quote_name(column_name)

            gather_data_sql = '''
                SELECT %s FROM %s
            ''' % (qcn, qt)
            cursor = bdb.sql_execute(gather_data_sql)
            data.append([item for (item,) in cursor])
        data = [list(i) for i in zip(*data)]

        # Ingest data into loom
        schema_file = self._data_to_schema(bdb, population_id, data)
        csv_file = self._data_to_csv(bdb, population_id, headers, data)
        loom.tasks.ingest(
            self._get_name(bdb, generator_id),
            rows_csv=csv_file.name, schema=schema_file.name)

        # Store encoding info in bdb
        self._store_encoding_info(bdb, generator_id)

    def _store_encoding_info(self, bdb, generator_id):
        name = self._get_name(bdb, generator_id)
        assert name is not None

        encoding_path = os.path.join(self._get_path(name),
                'ingest/encoding.json.gz')
        assert os.path.isfile(encoding_path)
        with gzip.open(encoding_path) as encoding_file:
            encoding = json.loads(encoding_file.read().decode('ascii'))

        population_id = core.bayesdb_generator_population(bdb, generator_id)
        table = core.bayesdb_population_table(bdb, population_id)

        # Store string encoding
        insert_string_encoding = '''
            INSERT INTO bayesdb_loom_string_encoding
                (generator_id, colno, string_form, integer_form)
                VALUES (:generator_id, :colno, :string_form, :integer_form)
        '''
        for col in encoding:
            if 'symbols' in col:
                colno = core.bayesdb_table_column_number(bdb,
                        table, str(col['name']))
                for (string_form, integer_form) in col['symbols'].iteritems():
                    bdb.sql_execute(insert_string_encoding, {
                        'generator_id': generator_id,
                        'colno': colno,
                        'string_form': string_form,
                        'integer_form': integer_form
                    })

        # Store ordering of columns
        insert_order_sql = '''
            INSERT INTO bayesdb_loom_column_ordering
                (generator_id, colno, rank)
                VALUES (:generator_id, :colno, :rank)
        '''
        for col_index in range(len(encoding)):
            colno = core.bayesdb_table_column_number(bdb,
                    table, str(encoding[col_index]['name']))
            bdb.sql_execute(insert_order_sql, {
                'generator_id': generator_id,
                'colno': colno,
                'rank': col_index
            })

    def _data_to_csv(self, bdb, population_id, headers, data):
        with tempfile.NamedTemporaryFile(delete=False) as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=CSV_DELIMITER)
            csv_writer.writerow(headers)
            for r in data:
                csv_writer.writerow(r)
        return csv_file

    def _data_to_schema(self, bdb, population_id, data):
        json_dict = {}
        for colno in core.bayesdb_variable_numbers(bdb,
                population_id, None):
            column_name = core.bayesdb_variable_name(bdb,
                    population_id, colno)
            stattype = core.bayesdb_variable_stattype(bdb,
                    population_id, colno)
            json_dict[column_name] = STATTYPE_TO_LOOMTYPE[stattype]

        with tempfile.NamedTemporaryFile(delete=False) as schema_file:
            schema_file.write(json.dumps(json_dict))

        return schema_file

    def _get_name(self, bdb, generator_id):
        gather_data_sql = '''
            SELECT name FROM bayesdb_loom_generator WHERE
            generator_id=%s;
        ''' % (generator_id)
        cursor = bdb.sql_execute(gather_data_sql)

        # TODO fix hack
        for (name,) in cursor:
            return name

    def _get_path(self, loom_name):
        return os.path.join(self.data_path, loom_name)

    def initialize_models(self, bdb, generator_id, modelnos):
        self.num_models = len(modelnos)

    def drop_generator(self, bdb, generator_id):
        with bdb.savepoint():
            self.drop_models(bdb, generator_id)
            delete_bayesdb_loom_generator = '''
                DELETE FROM bayesdb_loom_generator
                    WHERE generator_id = ?
            '''
            bdb.sql_execute(delete_bayesdb_loom_generator, (generator_id,))
            delete_bayesdb_loom_string_encoding = '''
                DELETE FROM bayesdb_loom_string_encoding
                    WHERE generator_id = ?
            '''
            bdb.sql_execute(delete_bayesdb_loom_string_encoding, (generator_id,))
            delete_bayesdb_loom_column_ordering = '''
                DELETE FROM bayesdb_loom_column_ordering
                    WHERE generator_id = ?
            '''
            bdb.sql_execute(delete_bayesdb_loom_column_ordering, (generator_id,))

    def drop_models(self, bdb, generator_id, modelnos=None):
        with bdb.savepoint():
            if modelnos is None:
                delete_bayesdb_loom_kind_partition = '''
                    DELETE FROM bayesdb_loom_kind_partition
                        WHERE generator_id = ?;
                '''
                bdb.sql_execute(delete_bayesdb_loom_kind_partition, (generator_id,))
            else:
                delete_bayesdb_loom_kind_partition = '''
                    DELETE FROM bayesdb_loom_kind_partition
                        WHERE generator_id = ? and modelno = ?;
                '''
                for modelno in modelnos:
                    bdb.sql_execute(delete_bayesdb_loom_kind_partition,
                            (generator_id, modelno))

    def analyze_models(self, bdb, generator_id, modelnos=None, iterations=1,
            max_seconds=None, ckpt_iterations=None, ckpt_seconds=None,
            program=None):
        # Make sure we've been initialized or given a modelnos list
        if modelnos is None:
            try:
                self.num_models
            except AttributeError:
                return

        self.drop_models(bdb, generator_id, modelnos=modelnos)

        name = self._get_name(bdb, generator_id)
        num_models = (self.num_models if modelnos is None else len(modelnos))
        loom.tasks.infer(name, sample_count = num_models)
        self._store_kind_partition(bdb, generator_id, modelnos)

    def _store_kind_partition(self, bdb, generator_id, modelnos):
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        for modelno in range(self.num_models) if modelnos is None else modelnos:
            column_partition = self._retrieve_column_partition(bdb,
                    generator_id, modelno)
            for colno in core.bayesdb_variable_numbers(bdb,
                    population_id, None):
                loom_rank = self._get_loom_rank(bdb, generator_id, colno)
                kind_id = column_partition[loom_rank]
                insert_generator_sql = '''
                    INSERT INTO bayesdb_loom_kind_partition
                        (generator_id, modelno, colno, kind_id)
                        VALUES (%d, %d, %d, %d)
                ''' % (generator_id, modelno, colno, kind_id)
                bdb.sql_execute(insert_generator_sql)

    def _retrieve_column_partition(self, bdb, generator_id, modelno):
        """Return column partition from CrossCat `sample`.

        The returned structure is of the form `cgpm.crosscat.state.State.Zv`.
        """
        cross_cat = self._get_cross_cat(bdb, generator_id, modelno)
        return dict(itertools.chain.from_iterable([
            [(loom_rank, k) for loom_rank in kind.featureids]
            for k, kind in enumerate(cross_cat.kinds)
        ]))

    def _get_cross_cat(self, bdb, generator_id, modelno):
        """Return the loom CrossCat structure whose id is `modelno`."""
        model_in = os.path.join(
            self.data_path, self._get_name(bdb, generator_id),
            'samples', 'sample.%d' % (modelno,), 'model.pb.gz')
        cross_cat = loom.schema_pb2.CrossCat()
        with open_compressed(model_in, 'rb') as f:
            cross_cat.ParseFromString(f.read())
        return cross_cat

    def column_dependence_probability(self,
            bdb, generator_id, modelnos, colno0, colno1):
        hit_list = []
        for modelno in range(self.num_models) if modelnos is None else modelnos:
            dependent = (self._get_kind_id(bdb,
                    generator_id, modelno, colno0) == self._get_kind_id(bdb,
                            generator_id, modelno, colno1))
            hit_list.append(1 if dependent else 0)

        return sum(hit_list)/len(hit_list)

    def _get_kind_id(self, bdb, generator_id, modelno, colno):
        gather_data_sql = '''
            SELECT kind_id FROM bayesdb_loom_kind_partition
            WHERE generator_id = %d and
            modelno = %d and
            colno = %d;
        ''' % (generator_id, modelno, colno)

        # TODO fix hack
        cursor = bdb.sql_execute(gather_data_sql)
        for (kind_id,) in cursor:
            return kind_id

    def column_mutual_information(self, bdb, generator_id, modelnos, colnos0,
            colnos1, constraints, numsamples):
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        colnames0 = [str(core.bayesdb_variable_name(bdb, population_id, colno))
                for colno in colnos0]
        colnames1 = [str(core.bayesdb_variable_name(bdb, population_id, colno))
                for colno in colnos1]

        server = loom.tasks.query(self._get_name(bdb, generator_id))
        target_set = server._cols_to_mask(server.encode_set(colnames0))
        query_set = server._cols_to_mask(server.encode_set(colnames1))
        return server._query_server.mutual_information(
                target_set,
                query_set,
                entropys=None,
                sample_count=loom.preql.SAMPLE_COUNT)

    def row_similarity(self, bdb, generator_id, modelnos, rowid, target_rowid,
            colnos):
        # TODO don't ignore the context
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        _, target_row = zip(*self._reorder_row(bdb, generator_id,
                core.bayesdb_population_row_values(bdb,
                    population_id, target_rowid)))
        _, row = zip(*self._reorder_row(bdb, generator_id,
                core.bayesdb_population_row_values(bdb, population_id, rowid)))

        # TODO: cache server
        # Run simlarity query
        server = loom.tasks.query(self._get_name(bdb, generator_id))
        output = server.similar([target_row], rows2=[row])
        return float(output)

    def _reorder_row(self, bdb, generator_id, row, dense=True):
        """Reorder a row of columns according to loom's column order

        Row should be a list of (colno, value) tuples

        Returns a list of (colno, value) tuples in the proper order.
        """
        ordered_column_labels = self._get_ordered_column_labels(bdb,
                generator_id)
        ordererd_column_dict = collections.OrderedDict(
                [(a, None) for a in ordered_column_labels])

        population_id = core.bayesdb_generator_population(bdb, generator_id)
        for (colno, value) in zip(range(len(row)), row):
            column_name = core.bayesdb_variable_name(bdb, population_id, colno)
            ordererd_column_dict[column_name] = str(value)

        if dense is False:
            return [(colno, value)
                    for (colno, value) in ordererd_column_dict.iteritems()
                    if value is not None]

        return ordererd_column_dict.iteritems()

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

        def _is_categorical(stattype):
            return casefold(stattype) in ['categorical', 'nominal']

        # Retrieve the samples. Specifying `rowid` ensures that relevant
        # constraints are retrieved by `simulate`, so provide empty constraints.
        sample = self.simulate_joint(
            bdb, generator_id, modelnos, rowid, [colno], [], numsamples)

        # Determine the imputation strategy (mode or mean).
        stattype = core.bayesdb_variable_stattype(
            bdb, core.bayesdb_generator_population(bdb, generator_id), colno)
        if _is_categorical(stattype):
            return _impute_categorical(sample)
        else:
            return _impute_numerical(sample)

    def simulate_joint(self, bdb, generator_id, modelnos, rowid, targets,
            constraints, num_samples=1, accuracy=None):
        if rowid != core.bayesdb_generator_fresh_row_id(bdb, generator_id):
            row_values = [str(a) if isinstance(a, unicode) else a
                    for a in
                    core.bayesdb_generator_row_values(bdb, generator_id, rowid)]

            row = [entry for entry in
                    zip(range(len(row_values)), row_values)
                    if entry[1] is not None]

            constraints_colnos, _ = ([], []) if len(
                    constraints) == 0 else zip(*constraints)
            row_colnos, _ = zip(*row)
            if any([colno in constraints_colnos for colno in row_colnos]):
                raise ValueError('''Conflict between
                        constraints and target row in simulate''')

            constraints += row

        row = {}
        target_no_to_name = {}
        for colno in targets:
            name = core.bayesdb_generator_column_name(bdb,
                generator_id, colno)
            target_no_to_name[colno] = name

            row[name] = ''
        for (colno, value) in constraints:
            row[core.bayesdb_generator_column_name(bdb,
                generator_id, colno)] = value

        csv_headers, csv_values = zip(*row.iteritems())

        # TODO cache
        server = loom.tasks.query(self._get_name(bdb, generator_id))

        # Perform predict query with some boiler plate
        # to make loom using StringIO() and an iterable instead of disk
        csv_headers = [str(a) for a in csv_headers]
        csv_values = [str(a) for a in csv_values]
        outfile = StringIO()
        writer = loom.preql.CsvWriter(outfile, returns=outfile.getvalue)
        reader = iter([csv_headers]+[csv_values])
        server._predict(reader, num_samples, writer, False)
        output = writer.result()

        # Parse output
        returned_headers = output.strip().split('\r\n')[0].split(CSV_DELIMITER)
        loom_output = [zip(returned_headers, a.split(CSV_DELIMITER))
                for a in output.strip().split('\r\n')[1:]]
        population_id = core.bayesdb_generator_population(bdb,
                generator_id)
        return_list = []
        for row in loom_output:
            return_list.append([])
            row_dict = dict(row)

            for colno in targets:
                colname = target_no_to_name[colno]
                value = row_dict[colname]
                stattype = core.bayesdb_variable_stattype(
                        bdb, population_id, colno)
                # TODO dont use private
                if core._STATTYPE_TO_AFFINITY[stattype] == 'real':
                    return_list[-1].append(float(value))
                else:
                    return_list[-1].append(value)

        return return_list

    def logpdf_joint(self, bdb, generator_id, modelnos, rowid, targets,
            constraints):
        # TODO optimize bdb calls
        ordered_column_labels = self._get_ordered_column_labels(bdb,
                generator_id)

        and_case = collections.OrderedDict([(a, None)
            for a in ordered_column_labels])
        conditional_case = collections.OrderedDict([(a, None)
            for a in ordered_column_labels])

        population_id = core.bayesdb_generator_population(bdb, generator_id)
        for (colno, value) in targets:
            column_name = core.bayesdb_variable_name(bdb, population_id, colno)

            and_case[column_name] = self._convert_to_proper_stattype(bdb,
                    generator_id, colno, value)
            conditional_case[column_name] = None
        for (colno, value) in constraints:
            column_name = core.bayesdb_variable_name(bdb,
                    population_id, colno)
            processed_value = self._convert_to_proper_stattype(bdb,
                    generator_id, colno, value)

            and_case[column_name] = processed_value
            conditional_case[column_name] = processed_value

        and_case = and_case.values()
        conditional_case = conditional_case.values()

        # TODO cache
        qserver = loom.query.get_server(
                self._get_path(self._get_name(bdb, generator_id)))
        return qserver.score(and_case) - qserver.score(conditional_case)

    def _convert_to_proper_stattype(self, bdb, generator_id, colno, value):
        """
        Convert a value from whats given by the logpdf_joint
        method parameters, to what loom can handle.
        Ex. from an integer to real or from a string to an integer
        """
        if value is None:
            return value

        population_id = core.bayesdb_generator_population(bdb,
                generator_id)
        stattype = core.bayesdb_variable_stattype(
                        bdb, population_id, colno)
        if stattype == 'numerical':
            return float(value)

        # Lookup the string encoding
        if stattype == 'categorical':
            return self._get_integer_form(bdb, generator_id, colno, value)

        return value

    def _get_integer_form(self, bdb, generator_id, colno, string_form):
        gather_data_sql = '''
            SELECT integer_form FROM bayesdb_loom_string_encoding
            WHERE generator_id = %d and
            colno = %d and
            string_form = %s;
        ''' % (generator_id, colno, sqlite3_quote_name(string_form))

        # TODO fix hack
        cursor = bdb.sql_execute(gather_data_sql)
        for (integer_form,) in cursor:
            return integer_form

    def _get_ordered_column_labels(self, bdb, generator_id):
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        return [core.bayesdb_variable_name(bdb, population_id, colno)
                for colno in self._get_order(bdb, generator_id)]

    def _get_loom_rank(self, bdb, generator_id, colno):
        gather_data_sql = '''
            SELECT rank FROM bayesdb_loom_column_ordering
            WHERE generator_id = %d and
            colno = %d
        ''' % (generator_id, colno)
        cursor = bdb.sql_execute(gather_data_sql)

        # TODO fix hack
        for (rank,) in cursor:
            return rank

    def _get_order(self, bdb, generator_id):
        """Get the ordering of the columns according to loom"""
        gather_data_sql = '''
            SELECT colno FROM bayesdb_loom_column_ordering
            WHERE generator_id = %d
            ORDER BY rank ASC
        ''' % (generator_id)
        cursor = bdb.sql_execute(gather_data_sql)
        return [colno for (colno,) in cursor]
