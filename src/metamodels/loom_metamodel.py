"""The Loom Model is a work in progress.

This module implements the :class:`bayeslite.IBayesDBMetamodel`
interface for the Loom Model.
"""
import collections
import csv
import datetime
import gzip
import json
import os
import os.path
import tempfile
import time

from StringIO import StringIO

import loom.tasks

import bayeslite.core as core
import bayeslite.metamodel as metamodel

from bayeslite.sqlite3_util import sqlite3_quote_name
from bayeslite.metamodel import bayesdb_metamodel_version

# TODO should we use "generator" or "metamodel" in the name of
# "bayesdb_loom_generator"

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
    integer_form        INTEGRER NOT NULL,
    PRIMARY KEY(generator_id, colno, integer_form)
);

CREATE TABLE bayesdb_loom_column_ordering (
    generator_id    INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    colno           INTEGRER NOT NULL,
    rank            INTEGRER NOT NULL,
    PRIMARY KEY(generator_id, colno)
);
'''

CSV_DELIMITER = ','

# TODO fill out
# TODO optimize number of bdb calls
STATTYPE_TO_LOOMTYPE = {'categorical': 'dd', 'numerical': 'nich'}


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
        """ Insert the model's schema into bdb."""
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
                .strftime('%Y%m%d-%H%M%S'),
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
            if "symbols" in col:
                colno = core.bayesdb_table_column_number(bdb,
                        table, str(col["name"]))
                for (string_form, integer_form) in col["symbols"].iteritems():
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
                    table, str(encoding[col_index]["name"]))
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
        self.modelnos = len(modelnos)

    def analyze_models(self, bdb, generator_id, modelnos=None, iterations=1,
            max_seconds=None, ckpt_iterations=None, ckpt_seconds=None,
            program=None):
        name = self._get_name(bdb, generator_id)
        loom.tasks.infer(name, sample_count = (
            self.modelnos if modelnos is None else len(modelnos)))

    def column_dependence_probability(self,
            bdb, generator_id, modelnos, colno0, colno1):
        # TODO cache
        server = loom.tasks.query(
                self._get_name(bdb, generator_id))
        output = server.relate(
                [core.bayesdb_generator_column_name(bdb, generator_id, colno0),
            core.bayesdb_generator_column_name(bdb, generator_id, colno1)])
        split_array = [a.split(CSV_DELIMITER) for a in output.split('\n')]

        return float(split_array[1][2])

    def column_mutual_information(self, bdb, generator_id, modelnos, colnos0,
            colnos1, constraints, numsamples):
        return [0]

    def row_similarity(self, bdb, generator_id, modelnos, rowid, target_rowid,
            colnos):
        # TODO don't ignore the context
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        target_row = self._reorder_row(bdb, generator_id,
                core.bayesdb_population_row_values(bdb,
                    population_id, target_rowid))
        row = self._reorder_row(bdb, generator_id,
                core.bayesdb_population_row_values(bdb, population_id, rowid))

        # TODO: cache server
        # Run simlarity query
        server = loom.tasks.query(self._get_name(bdb, generator_id))
        output = server.similar([target_row], rows2=[row])
        return float(output)

    def _reorder_row(self, bdb, generator_id, row):
        """Reorder a row of columns according to loom's encoding

        Row should be a list of (colno, value) tuples

        Returns a list of scalar values in the proper order.
        """
        ordered_column_labels = self._get_ordered_column_labels(bdb,
                generator_id)
        ordererd_column_dict = collections.OrderedDict(
                [(a, None) for a in ordered_column_labels])

        population_id = core.bayesdb_generator_population(bdb, generator_id)
        for (colno, value) in zip(range(len(row)), row):
            column_name = core.bayesdb_variable_name(bdb, population_id, colno)
            ordererd_column_dict[column_name] = str(value)

        return [value
                for (_, value) in ordererd_column_dict.iteritems()]

    def predict_confidence(self, bdb, generator_id, modelnos, rowid, colno,
            numsamples=None):
        return (0, 1)

    def simulate_joint(self, bdb, generator_id, modelnos, rowid, targets,
            constraints, num_samples=1, accuracy=None):

        headers = []
        row = []
        for colno in targets:
            headers.append(core.bayesdb_generator_column_name(bdb,
                generator_id, colno))
            row.append('')
        for (colno, value) in constraints:
            headers.append(core.bayesdb_generator_column_name(bdb,
                generator_id, colno))
            row.append(value)

        # TODO cache
        server = loom.tasks.query(self._get_name(bdb, generator_id))

        # Perform predict query with some boiler plate
        # to make loom using StringIO() and an iterable instead of disk
        outfile = StringIO()
        writer = loom.preql.CsvWriter(outfile, returns=outfile.getvalue)
        reader = iter([headers]+[row])
        server._predict(reader, num_samples, writer, False)
        output = writer.result()

        # Parse output
        loom_output = [a.split(CSV_DELIMITER)
                for a in output.strip().split('\r\n')[1:]]

        # Convert output values to proper data types
        population_id = core.bayesdb_generator_population(bdb,
                generator_id)
        for row_index in range(len(loom_output)):
            for col_index in range(len(loom_output[row_index])):
                stattype = core.bayesdb_variable_stattype(
                        bdb, population_id, targets[col_index])
                # TODO dont use private
                if core._STATTYPE_TO_AFFINITY[stattype] is 'real':
                    loom_output[row_index][col_index] = float(
                            loom_output[row_index][col_index])
        return loom_output

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

    def _get_order(self, bdb, generator_id):
        """Get the ordering of the columns according to loom"""
        gather_data_sql = '''
            SELECT colno FROM bayesdb_loom_column_ordering
            WHERE generator_id = %d
            ORDER BY rank ASC
        ''' % (generator_id)
        cursor = bdb.sql_execute(gather_data_sql)
        return [colno for (colno,) in cursor]
