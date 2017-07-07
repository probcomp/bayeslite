"""The Truell Model is a work in progress.

This module implements the :class:`bayeslite.IBayesDBMetamodel`
interface for the Truell Model.
"""
import math
import random

import bayeslite.core as core
import bayeslite.metamodel as metamodel

from bayeslite.sqlite3_util import sqlite3_quote_name

HALF_LOG2PI = 0.5 * math.log(2 * math.pi)

truell_schema = '''
INSERT INTO bayesdb_metamodel (name, version)
    VALUES (?, 1);

CREATE TABLE bayesdb_truell_column (
    generator_id    INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    colno       INTEGER NOT NULL,
    count       INTEGER NOT NULL,
    mu         REAL NOT NULL,
    sigma     REAL NOT NULL,
    PRIMARY KEY(generator_id, colno),
    FOREIGN KEY(generator_id, colno)
        REFERENCES bayesdb_generator_column(generator_id, colno)
);
'''

class TruellMetamodel(metamodel.IBayesDBMetamodel):
    """Truell metamodel for BayesDB."""

    def __init__(self, seed=0):
        self.prng = random.Random(seed)

    def name(self): return 'truell'

    def register(self, bdb):
        with bdb.savepoint():
            bdb.sql_execute(truell_schema, (self.name(),))

    def create_generator(self, bdb, generator_id, schema, **kwargs):
        insert_column_sql = '''
            INSERT INTO bayesdb_truell_column
                (generator_id, colno, count, mu, sigma)
                VALUES (:generator_id, :colno, :count, :mu, :sigma)
        '''
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        table = core.bayesdb_population_table(bdb, population_id)
        for colno in core.bayesdb_variable_numbers(bdb, population_id, None):
            column_name = core.bayesdb_variable_name(bdb, population_id, colno)
            stattype = core.bayesdb_variable_stattype(
                bdb, population_id, colno)
            if not stattype == 'numerical':
                raise BQLError(bdb, 'Truell only supports'
                    ' numerical columns, but %s is %s'
                    % (repr(column_name), repr(stattype)))
            (count, mu, sigma) = self._compute_data(bdb, table, column_name)
            bdb.sql_execute(insert_column_sql, {
                'generator_id': generator_id,
                'colno': colno,
                'count': count,
                'mu': mu,
                'sigma': sigma,
            })

    def _compute_data(self, bdb, table, column_name):
        qt = sqlite3_quote_name(table)
        qcn = sqlite3_quote_name(column_name)

        gather_data_sql = '''
            SELECT %s FROM %s
        ''' % (qcn, qt)
        cursor = bdb.sql_execute(gather_data_sql)

        count = 0
        xsum = 0
        for (item,) in cursor:
            count += 1
            xsum += item

        mu = xsum/count

        # TODO FIX?
        cursor = bdb.sql_execute(gather_data_sql)
        mse = 0
        for (item,) in cursor:
            mse += pow(item-mu, 2)

        sigma = mse/count
        return (count, mu, sigma)


    def initialize_models(self, *args, **kwargs):
        pass

    def analyze_models(self, bdb, generator_id, modelnos=None, iterations=1,
            max_seconds=None, ckpt_iterations=None, ckpt_seconds=None,
            program=None):
        pass

    def column_dependence_probability(self, bdb, generator_id, modelnos, colno0,
            colno1):
        return 0

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
            _constraints, num_samples=1, accuracy=None):
        mu_sigma = self._get_mus_sigmas(bdb, generator_id)
        return [[self.prng.gauss(mu_sigma[colno][0],
            math.sqrt(mu_sigma[colno][1])) for colno in targets]
            for _ in range(num_samples)]

    def logpdf_joint(self, bdb, generator_id, modelnos, rowid, targets,
            constraints):
        xsum = 0
        mu_sigma = self._get_mus_sigmas(bdb, generator_id)
        for (colno, value) in targets:
            xsum += self._logpdf_gaussian(value, mu_sigma[colno][0], mu_sigma[colno][1])
        return xsum

    def _logpdf_gaussian(self, x, mu, sigma):
        deviation = x - mu
        return - math.log(sigma) - HALF_LOG2PI - (0.5 * deviation * deviation / (sigma * sigma))

    def _get_mus_sigmas(self, bdb, generator_id):
        params_sql = '''
            SELECT colno, mu, sigma FROM bayesdb_truell_column
                WHERE generator_id = :generator_id
        '''
        with bdb.savepoint():
            cursor = bdb.sql_execute(params_sql, (generator_id,))
            mu_sigma = {}
            for (colno, mu, sigma) in cursor:
                mu_sigma[colno] = (mu, sigma)
            return mu_sigma
