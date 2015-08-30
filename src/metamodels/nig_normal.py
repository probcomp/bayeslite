# -*- coding: utf-8 -*-

#   Copyright (c) 2015, MIT Probabilistic Computing Project
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

"""A model that posts that all columns are independently Gaussian with
unknown parameters.

The parameters are taken from the normal and inverse-gamma conjuate
prior.

This module implements the :class:`bayeslite.IBayesDBMetamodel`
interface for the NIG-Normal model.
"""

import math
import random

import bayeslite.metamodel as metamodel

from bayeslite.exception import BQLError
from bayeslite.sqlite3_util import sqlite3_quote_name

nig_normal_schema_1 = '''
INSERT INTO bayesdb_metamodel (name, version) VALUES ('nig_normal', 1);

CREATE TABLE bayesdb_nig_normal_columns (
    generator_id    INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    colno       INTEGER NOT NULL,
    count       INTEGER NOT NULL,
    sum         REAL NOT NULL,
    sumsq       REAL NOT NULL,
    PRIMARY KEY(generator_id, colno),
    FOREIGN KEY(generator_id, colno)
        REFERENCES bayesdb_generator_column(generator_id, colno)
);

CREATE TABLE bayesdb_nig_normal_models (
    generator_id    INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    colno           INTEGER NOT NULL,
    modelno         INTEGER NOT NULL,
    mu              REAL NOT NULL,
    sigma           REAL NOT NULL,
    PRIMARY KEY(generator_id, colno, modelno),
    FOREIGN KEY(generator_id, modelno)
        REFERENCES bayesdb_generator_model(generator_id, modelno),
    FOREIGN KEY(generator_id, colno)
        REFERENCES bayesdb_nig_normal_columns(generator_id, colno)
);
'''

hardcoded_hypers = (0, 1, 1, 1)

class NIGNormalMetamodel(metamodel.IBayesDBMetamodel):
    """Normal-Inverse-Gamma-Normal metamodel for BayesDB.

    The metamodel is named ``nig_normal`` in BQL::

        CREATE GENERATOR t_nig FOR t USING nig_normal(..)

    Internally, the NIG Normal metamodel add SQL tables to the
    database with names that begin with ``nig_normal_``.

    """

    def __init__(self, seed=0):
        self.prng = random.Random(seed)

    def name(self): return 'nig_normal'

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
                for stmt in nig_normal_schema_1.split(';'):
                    bdb.sql_execute(stmt)
                version = 1
            if version != 1:
                raise BQLError(bdb, 'NIG-Normal already installed'
                    ' with unknown schema version: %d' % (version,))

    def create_generator(self, bdb, table, schema, instantiate):
        # The schema is the column list. May want to change this later
        # to make room for specifying the hyperparameters, etc.
        insert_column_sql = '''
            INSERT INTO bayesdb_nig_normal_columns
                (generator_id, colno, count, sum, sumsq)
                VALUES (:generator_id, :colno, :count, :sum, :sumsq)
        '''
        with bdb.savepoint():
            generator_id, column_list = instantiate(schema)
            for (colno, column_name, stattype) in column_list:
                print stattype
                if not stattype == 'numerical':
                    raise BQLError(bdb, 'NIG-Normal only supports'
                        ' numerical columns, but %s is %s'
                        % (column_name, stattype))
                (count, xsum, sumsq) = data_suff_stats(bdb, table, column_name)
                bdb.sql_execute(insert_column_sql, {
                    'generator_id': generator_id,
                    'colno': colno,
                    'count': count,
                    'sum': xsum,
                    'sumsq': sumsq,
                })

    def drop_generator(self, bdb, generator_id):
        with bdb.savepoint():
            self.drop_models(bdb, generator_id)
            delete_columns_sql = '''
                DELETE FROM bayesdb_nig_normal_columns
                    WHERE generator_id = ?
            '''
            bdb.sql_execute(delete_columns_sql, (generator_id,))

    def initialize_models(self, bdb, generator_id, modelnos, model_config):
        insert_sample_sql = '''
            INSERT INTO bayesdb_nig_normal_models
                (generator_id, colno, modelno, mu, sigma)
                VALUES (:generator_id, :colno, :modelno, :mu, :sigma)
        '''
        self._set_models(bdb, generator_id, modelnos, insert_sample_sql)

    def drop_models(self, bdb, generator_id, modelnos=None):
        if modelnos is None:
            delete_models_sql = '''
                DELETE FROM bayesdb_nig_normal_models WHERE generator_id = ?
            '''
            bdb.sql_execute(delete_models_sql, (generator_id,))
        else:
            delete_models_sql = '''
                DELETE FROM bayesdb_nig_normal_models
                    WHERE generator_id = ? AND modelno = ?
            '''
            for modelno in modelnos:
                bdb.sql_execute(delete_models_sql, (generator_id, modelno))

    def analyze_models(self, bdb, generator_id, modelnos=None, iterations=1,
            max_seconds=None, ckpt_iterations=None, ckpt_seconds=None):
        # Ignore analysis timing control, because one step reaches the
        # posterior anyway.
        update_sample_sql = '''
            UPDATE bayesdb_nig_normal_models SET mu = :mu, sigma = :sigma
                WHERE generator_id = :generator_id
                    AND colno = :colno
                    AND modelno = :modelno
        '''
        if modelnos is None:
            # This assumes that models x columns forms a dense
            # rectangle in the database, which it should.
            modelnos_sql = '''
                SELECT DISTINCT modelno FROM bayesdb_nig_normal_models
                    WHERE generator_id = ?
            '''
            modelnos = list(bdb.sql_execute(modelnos_sql, (generator_id,)))
        self._set_models(bdb, generator_id, modelnos, update_sample_sql)

    def _set_models(self, bdb, generator_id, modelnos, sql):
        collect_stats_sql = '''
            SELECT (colno, count, sum, sumsq) FROM
                bayesdb_nig_normal_columns WHERE generator_id = ?
        '''
        with bdb.savepoint():
            cursor = bdb.sql_execute(collect_stats_sql, (generator_id,))
            for (colno, count, xsum, sumsq) in cursor:
                stats = (count, xsum, sumsq)
                for modelno in modelnos:
                    (mu, sigma) = gibbs_step_params(self.prng, hardcoded_hypers, stats)
                    bdb.sql_execute(sql, {
                        'generator_id': generator_id,
                        'colno': colno,
                        'modelno': modelno,
                        'mu': mu,
                        'sigma': sigma,
                    })

    def simulate_joint(self, _bdb, _generator_id, targets, _constraints):
        return [self.prng.gauss(0, 1) for _ in targets]
    def logpdf(self, _bdb, _generator_id, targets, _constraints):
        return sum(logpdfOne(value, 0, 1) for (_, _, value) in targets)
    def insert(self, *args): pass
    def remove(self, *args): pass
    def infer(self, *args): pass

HALF_LOG2PI = 0.5 * math.log(2 * math.pi)

def logpdfOne(x, mu, sigma):
    deviation = x - mu
    return - math.log(sigma) - HALF_LOG2PI \
        - (0.5 * deviation * deviation / (sigma * sigma))

def data_suff_stats(bdb, table, column_name):
    gather_data_sql_pat = '''
        SELECT %s FROM %s
    '''
    qt = sqlite3_quote_name(table)
    qcn = sqlite3_quote_name(column_name)
    # TODO Do this computation inside the database?
    gather_data_sql = gather_data_sql_pat % (qt, qcn)
    cursor = bdb.sql_execute(gather_data_sql)
    count = 0
    xsum = 0
    sumsq = 0
    for item in cursor:
        count += 1
        xsum += item
        sumsq += item * item
    return (count, xsum, sumsq)

def gibbs_step_params(prng, hypers, stats):
    # This is UNigNormalAAALKernel.simulate packaged differently.
    (m, V, a, b) = hypers
    [ctN, xsum, xsumsq] = stats
    Vn = 1 / (1/V + ctN)
    mn = Vn*(1/V*m + ctN * xsum/ctN)
    an = a + ctN / 2
    bn = b + 0.5*(m**2/V + xsumsq - mn**2/Vn)
    newSigma2 = 1.0 / prng.gammavariate(an, bn) # shape, scale
    newMu = prng.gauss(mn, math.sqrt(newSigma2*Vn))
    return (newMu, math.sqrt(newSigma2))
