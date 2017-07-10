"""The Loom Model is a work in progress.

This module implements the :class:`bayeslite.IBayesDBMetamodel`
interface for the Loom Model.
"""
import collections
import csv
import gzip
import json
import os
import os.path
import tempfile


import loom.tasks

import bayeslite.core as core
import bayeslite.metamodel as metamodel

from bayeslite.sqlite3_util import sqlite3_quote_name

loom_schema = '''
INSERT INTO bayesdb_metamodel (name, version)
    VALUES (?, 1);
'''

CSV_DELIMITER = ','

# TODO fill out
# TODO optimize number of bdb calls
STATTYPE_TO_LOOMTYPE = {'categorical': 'dd', 'numerical': 'nich'}


class LoomMetamodel(metamodel.IBayesDBMetamodel):
    """Loom metamodel for BayesDB."""

    def __init__(self,data_path=
            '/scratch/mntruell/venv/lib/python2.7/site-packages/data/'):
        self.DATA_PATH = data_path

    def name(self):
        return 'loom'

    def register(self, bdb):
        with bdb.savepoint():
            bdb.sql_execute(loom_schema, (self.name(),))

    def create_generator(self, bdb, generator_id, schema, **kwargs):
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        table = core.bayesdb_population_table(bdb, population_id)

        # Collect data from bdb
        headers = []
        data = []

        for colno in core.bayesdb_variable_numbers(bdb, population_id, None):
            column_name = core.bayesdb_variable_name(bdb, population_id, colno)
            headers.append(column_name)

            qt = sqlite3_quote_name(table)
            qcn = sqlite3_quote_name(column_name)

            with bdb.savepoint():
                gather_data_sql = '''
                    SELECT %s FROM %s
                ''' % (qcn, qt)
                cursor = bdb.sql_execute(gather_data_sql)

                data.append([item for (item,) in cursor])

        # Transpose data list
        data = [list(i) for i in zip(*data)]

        # Write to temp file
        with tempfile.NamedTemporaryFile() as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=CSV_DELIMITER)
            csv_writer.writerow(headers)
            for r in data:
                csv_writer.writerow(r)
            csv_file.flush()

            # Prepare a schema json file
            json_dict = {}
            for colno in core.bayesdb_variable_numbers(bdb,
                    population_id, None):
                column_name = core.bayesdb_variable_name(bdb,
                        population_id, colno)
                stattype = core.bayesdb_variable_stattype(bdb,
                        population_id, colno)
                json_dict[column_name] = STATTYPE_TO_LOOMTYPE[stattype]

            with tempfile.NamedTemporaryFile() as schema_file:
                schema_file.write(json.dumps(json_dict))
                schema_file.flush()

                # Ingest data
                loom.tasks.ingest(
                    core.bayesdb_generator_name(bdb, generator_id),
                    rows_csv=csv_file.name, schema=schema_file.name)

                # TODO store the string-int transformations - use in logpdf

    def _get_path(self, loom_name):
        return os.path.join(self.DATA_PATH, loom_name)

    def initialize_models(self, bdb, generator_id, modelnos):
        self.modelnos = len(modelnos)

    def analyze_models(self, bdb, generator_id, modelnos=None, iterations=1,
            max_seconds=None, ckpt_iterations=None, ckpt_seconds=None,
            program=None):
        name = core.bayesdb_generator_name(bdb, generator_id)
        loom.tasks.infer(name, sample_count = (
            self.modelnos if modelnos is None else len(modelnos)))

    def column_dependence_probability(self,
            bdb, generator_id, modelnos, colno0, colno1):
        # TODO cache
        server = loom.tasks.query(
                core.bayesdb_generator_name(bdb, generator_id))
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
        return 0

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

        # Write to csv
        with tempfile.NamedTemporaryFile() as temp_file:
            csv_writer = csv.writer(temp_file, delimiter=CSV_DELIMITER)
            csv_writer.writerow(headers)
            csv_writer.writerow(row)
            temp_file.flush()

            # Simulate values
            # TODO cache
            server = loom.tasks.query(core.bayesdb_generator_name(bdb, generator_id))
            output = server.predict(temp_file.name, num_samples, id_offset=False)

            # Parse output
            loom_output = [a.split(CSV_DELIMITER)
                    for a in output.strip().split('\r\n')[1:]]

            # Convert output values to proper data types
            population_id = core.bayesdb_generator_population(bdb, generator_id)
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
        def _convert_to_proper_stattype(bdb, population_id, colno, value):
            """
            Convert a value from whats given by the logpdf_joint
            method parameters, to what loom can handle.
            Ex. from an integer to real or from a string to an integer
            """
            if value is None:
                return value

            stattype = core.bayesdb_variable_stattype(
                            bdb, population_id, colno)
            if stattype == "numerical":
                return float(value)

            # Lookup the string encoding
            if stattype == "categorical":
                column_name = core.bayesdb_variable_name(bdb, population_id, colno)
                encoding_list = self._get_encoding(bdb, generator_id)
                column_encoding = next((x for x in encoding_list if x['name'] == column_name), None)
                return column_encoding['symbols'][value]

            return value

        ordered_column_labels = self._get_order(bdb, generator_id)

        and_case = collections.OrderedDict([(a, None) for a in ordered_column_labels])
        conditional_case = collections.OrderedDict([(a, None) for a in ordered_column_labels])

        population_id = core.bayesdb_generator_population(bdb, generator_id)
        for (colno, value) in targets:
            column_name = core.bayesdb_variable_name(bdb, population_id, colno)

            and_case[column_name] = _convert_to_proper_stattype(bdb,
                    population_id, colno, value)
            conditional_case[column_name] = None
        for (colno, value) in constraints:
            column_name = core.bayesdb_variable_name(bdb,
                    population_id, colno)
            processed_value = _convert_to_proper_stattype(bdb,
                    population_id, colno, value)

            and_case[column_name] = processed_value
            conditional_case[column_name] = processed_value

        and_case = [a[1] for a in and_case.iteritems()]
        conditional_case = [a[1] for a in conditional_case.iteritems()]

        # TODO cache
        qserver = loom.query.get_server(
                self._get_path(core.bayesdb_generator_name(bdb, generator_id)))
        return qserver.score(and_case) - qserver.score(conditional_case)

    def _get_order(self, bdb, generator_id):
        """Get the ordering of the columns according to loom"""
        encoding_list = self._get_encoding(bdb, generator_id)
        return [attr["name"] for attr in encoding_list]

    def _get_encoding(self, bdb, generator_id):
        """Get the encoding of our loom columns.
        This includes the ordering of columns and the
        integer encoding of categorical columns."""
        # TODO cache
        name = core.bayesdb_generator_name(bdb, generator_id)
        with gzip.open(
                os.path.join(self._get_path(name),
                'ingest/encoding.json.gz')) as encoding_file:
            return json.loads(encoding_file.read().decode('ascii'))

