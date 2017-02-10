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

"""A model that posts that all columns are independently Gaussian with
unknown parameters.

The parameters are taken from the normal and inverse-gamma conjuate
prior.

This module implements the :class:`bayeslite.IBayesDBMetamodel`
interface for the NIG-Normal model.
"""

import math
import random

import bayeslite.core as core
import bayeslite.metamodel as metamodel

from bayeslite.exception import BQLError
from bayeslite.math_util import logmeanexp
from bayeslite.metamodel import bayesdb_metamodel_version
from bayeslite.sqlite3_util import sqlite3_quote_name
from bayeslite.util import cursor_value

nig_normal_schema_1 = '''
INSERT INTO bayesdb_metamodel (name, version) VALUES ('nig_normal', 1);

CREATE TABLE bayesdb_nig_normal_column (
    generator_id    INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    colno       INTEGER NOT NULL,
    count       INTEGER NOT NULL,
    sum         REAL NOT NULL,
    sumsq       REAL NOT NULL,
    PRIMARY KEY(generator_id, colno),
    FOREIGN KEY(generator_id, colno)
        REFERENCES bayesdb_generator_column(generator_id, colno)
);

CREATE TABLE bayesdb_nig_normal_model (
    generator_id    INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    colno           INTEGER NOT NULL,
    modelno         INTEGER NOT NULL,
    mu              REAL NOT NULL,
    sigma           REAL NOT NULL,
    PRIMARY KEY(generator_id, colno, modelno),
    FOREIGN KEY(generator_id, modelno)
        REFERENCES bayesdb_generator_model(generator_id, modelno),
    FOREIGN KEY(generator_id, colno)
        REFERENCES bayesdb_nig_normal_column(generator_id, colno)
);
'''

nig_normal_schema_2 = '''
UPDATE bayesdb_metamodel SET version = 2 WHERE name = 'nig_normal';

CREATE TABLE bayesdb_nig_normal_deviation (
    population_id       INTEGER NOT NULL REFERENCES bayesdb_population(id),
    generator_id        INTEGER NOT NULL REFERENCES bayesdb_generator(id),
    deviation_colno     INTEGER NOT NULL,
    observed_colno      INTEGER NOT NULL,
    PRIMARY KEY(generator_id, deviation_colno),
    FOREIGN KEY(generator_id, deviation_colno)
        REFERENCES bayesdb_variable(generator_id, colno),
    FOREIGN KEY(population_id, observed_colno)
        REFERENCES bayesdb_variable(population_id, colno)
);
'''

class NIGNormalMetamodel(metamodel.IBayesDBMetamodel):
    """Normal-Inverse-Gamma-Normal metamodel for BayesDB.

    The metamodel is named ``nig_normal`` in BQL::

        CREATE GENERATOR t_nig FOR t USING nig_normal(..)

    Internally, the NIG Normal metamodel add SQL tables to the
    database with names that begin with ``bayesdb_nig_normal_``.

    """

    def __init__(self, hypers=(0, 1, 1, 1), seed=0):
        self.hypers = hypers
        self.prng = random.Random(seed)

    def name(self): return 'nig_normal'

    def register(self, bdb):
        with bdb.savepoint():
            version = bayesdb_metamodel_version(bdb, self.name())
            if version is None:
                bdb.sql_execute(nig_normal_schema_1)
                version = 1
            if version == 1:
                bdb.sql_execute(nig_normal_schema_2)
                version = 2
            if version != 2:
                raise BQLError(bdb, 'NIG-Normal already installed'
                    ' with unknown schema version: %d' % (version,))

    def create_generator(self, bdb, generator_id, schema, **kwargs):
        # XXX Do something with the schema.
        insert_column_sql = '''
            INSERT INTO bayesdb_nig_normal_column
                (generator_id, colno, count, sum, sumsq)
                VALUES (:generator_id, :colno, :count, :sum, :sumsq)
        '''
        population_id = core.bayesdb_generator_population(bdb, generator_id)
        table = core.bayesdb_population_table(bdb, population_id)
        for colno in core.bayesdb_variable_numbers(bdb, population_id, None):
            column_name = core.bayesdb_variable_name(bdb, population_id, colno)
            stattype = core.bayesdb_variable_stattype(
                bdb, population_id, colno)
            if not stattype == 'numerical':
                raise BQLError(bdb, 'NIG-Normal only supports'
                    ' numerical columns, but %s is %s'
                    % (repr(column_name), repr(stattype)))
            (count, xsum, sumsq) = data_suff_stats(bdb, table, column_name)
            bdb.sql_execute(insert_column_sql, {
                'generator_id': generator_id,
                'colno': colno,
                'count': count,
                'sum': xsum,
                'sumsq': sumsq,
            })

        # XXX Make the schema a little more flexible.
        if schema == [[]]:
            return
        for clause in schema:
            if not (len(clause) == 3 and \
                    isinstance(clause[0], str) and \
                    clause[1] == 'deviation' and \
                    isinstance(clause[2], list) and \
                    len(clause[2]) == 1 and \
                    isinstance(clause[2][0], str)):
                raise BQLError(bdb, 'Invalid nig_normal clause: %r' %
                    (clause,))
            dev_var = clause[0]
            obs_var = clause[2][0]
            if not core.bayesdb_has_variable(bdb, population_id, None,
                    obs_var):
                raise BQLError(bdb, 'No such variable: %r' % (obs_var,))
            obs_colno = core.bayesdb_variable_number(bdb, population_id, None,
                obs_var)
            dev_colno = core.bayesdb_add_latent(bdb, population_id,
                generator_id, dev_var, 'numerical')
            bdb.sql_execute('''
                INSERT INTO bayesdb_nig_normal_deviation
                    (population_id, generator_id, deviation_colno,
                        observed_colno)
                    VALUES (?, ?, ?, ?)
            ''', (population_id, generator_id, dev_colno, obs_colno))

    def drop_generator(self, bdb, generator_id):
        with bdb.savepoint():
            self.drop_models(bdb, generator_id)
            delete_columns_sql = '''
                DELETE FROM bayesdb_nig_normal_column
                    WHERE generator_id = ?
            '''
            bdb.sql_execute(delete_columns_sql, (generator_id,))
            delete_deviations_sql = '''
                DELETE FROM bayesdb_nig_normal_deviation
                    WHERE generator_id = ?
            '''
            bdb.sql_execute(delete_deviations_sql, (generator_id,))

    def initialize_models(self, bdb, generator_id, modelnos):
        insert_sample_sql = '''
            INSERT INTO bayesdb_nig_normal_model
                (generator_id, colno, modelno, mu, sigma)
                VALUES (:generator_id, :colno, :modelno, :mu, :sigma)
        '''
        self._set_models(bdb, generator_id, modelnos, insert_sample_sql)

    def drop_models(self, bdb, generator_id, modelnos=None):
        with bdb.savepoint():
            if modelnos is None:
                delete_models_sql = '''
                    DELETE FROM bayesdb_nig_normal_model
                        WHERE generator_id = ?
                '''
                bdb.sql_execute(delete_models_sql, (generator_id,))
            else:
                delete_models_sql = '''
                    DELETE FROM bayesdb_nig_normal_model
                        WHERE generator_id = ? AND modelno = ?
                '''
                for modelno in modelnos:
                    bdb.sql_execute(delete_models_sql, (generator_id, modelno))

    def analyze_models(self, bdb, generator_id, modelnos=None, iterations=1,
            max_seconds=None, ckpt_iterations=None, ckpt_seconds=None,
            program=None):
        if program is not None:
            # XXX
            raise NotImplementedError('nig_normal analysis programs')

        # Ignore analysis timing control, because one step reaches the
        # posterior anyway.
        # NOTE: Does not update the model iteration count.  This would
        # manifest as failing to count the number of inference
        # iterations taken.  Since inference converges in one step,
        # this consists of failing to track the metadata of whether
        # that one step was done or not.
        update_sample_sql = '''
            UPDATE bayesdb_nig_normal_model SET mu = :mu, sigma = :sigma
                WHERE generator_id = :generator_id
                    AND colno = :colno
                    AND modelno = :modelno
        '''
        if modelnos is None:
            # This assumes that models x columns forms a dense
            # rectangle in the database, which it should.
            modelnos = self._modelnos(bdb, generator_id)
        self._set_models(bdb, generator_id, modelnos, update_sample_sql)

    def _set_models(self, bdb, generator_id, modelnos, sql):
        collect_stats_sql = '''
            SELECT colno, count, sum, sumsq FROM
                bayesdb_nig_normal_column WHERE generator_id = ?
        '''
        with bdb.savepoint():
            cursor = bdb.sql_execute(collect_stats_sql, (generator_id,))
            for (colno, count, xsum, sumsq) in cursor:
                stats = (count, xsum, sumsq)
                for modelno in modelnos:
                    (mu, sig) = self._gibbs_step_params(self.hypers, stats)
                    bdb.sql_execute(sql, {
                        'generator_id': generator_id,
                        'colno': colno,
                        'modelno': modelno,
                        'mu': mu,
                        'sigma': sig,
                    })

    def _modelnos(self, bdb, generator_id):
        modelnos_sql = '''
            SELECT DISTINCT modelno FROM bayesdb_nig_normal_model
                WHERE generator_id = ?
        '''
        with bdb.savepoint():
            return [modelno for (modelno,) in bdb.sql_execute(modelnos_sql,
                (generator_id,))]

    def simulate_joint(
            self, bdb, generator_id, rowid, targets, _constraints, modelno=None,
            num_samples=1, accuracy=None):
        # Note: The constraints are irrelevant because columns are
        # independent in the true distribution (except in the case of
        # shared, unknown hyperparameters), and cells in a column are
        # independent conditioned on the latent parameters mu and
        # sigma.  This method does not expose the inter-column
        # dependence induced by approximating the true distribution
        # with a finite number of full-table models.
        with bdb.savepoint():
            if modelno is None:
                modelnos = self._modelnos(bdb, generator_id)
                modelno = self.prng.choice(modelnos)
            (mus, sigmas) = self._model_mus_sigmas(bdb, generator_id, modelno)
            return [[self._simulate_1(bdb, generator_id, mus, sigmas, colno)
                     for colno in targets]
                    for _ in range(num_samples)]

    def _simulate_1(self, bdb, generator_id, mus, sigmas, colno):
        if colno < 0:
            dev_colno = colno
            cursor = bdb.sql_execute('''
                SELECT observed_colno FROM bayesdb_nig_normal_deviation
                    WHERE generator_id = ? AND deviation_colno = ?
            ''', (generator_id, dev_colno))
            obs_colno = cursor_value(cursor)
            return self.prng.gauss(0, sigmas[obs_colno])
        else:
            return self.prng.gauss(mus[colno], sigmas[colno])

    def _model_mus_sigmas(self, bdb, generator_id, modelno):
        # TODO Filter in the database by the columns I will actually use?
        # TODO Cache the results using bdb.cache?
        params_sql = '''
            SELECT colno, mu, sigma FROM bayesdb_nig_normal_model
                WHERE generator_id = ? AND modelno = ?
        '''
        cursor = bdb.sql_execute(params_sql, (generator_id, modelno))
        mus = {}
        sigmas = {}
        for (colno, mu, sigma) in cursor:
            assert colno not in mus
            mus[colno] = mu
            assert colno not in sigmas
            sigmas[colno] = sigma
        return (mus, sigmas)

    def logpdf_joint(self, bdb, generator_id, rowid, targets, _constraints,
            modelno=None):
        # Note: The constraints are irrelevant for the same reason as
        # in simulate_joint.
        (all_mus, all_sigmas) = self._all_mus_sigmas(bdb, generator_id)
        def model_log_pdf(modelno):
            mus = all_mus[modelno]
            sigmas = all_sigmas[modelno]
            def logpdf_1((colno, x)):
                return self._logpdf_1(bdb, generator_id, mus, sigmas, colno, x)
            return sum(map(logpdf_1, targets))
        modelwise = [model_log_pdf(m) for m in sorted(all_mus.keys())]
        return logmeanexp(modelwise)

    def _logpdf_1(self, bdb, generator_id, mus, sigmas, colno, x):
        if colno < 0:
            dev_colno = colno
            cursor = bdb.sql_execute('''
                SELECT observed_colno FROM bayesdb_nig_normal_deviation
                    WHERE generator_id = ? AND deviation_colno = ?
            ''', (generator_id, dev_colno))
            obs_colno = cursor_value(cursor)
            return logpdf_gaussian(x, 0, sigmas[obs_colno])
        else:
            return logpdf_gaussian(x, mus[colno], sigmas[colno])

    def _all_mus_sigmas(self, bdb, generator_id):
        params_sql = '''
            SELECT colno, modelno, mu, sigma FROM bayesdb_nig_normal_model
                WHERE generator_id = :generator_id
        ''' # TODO Filter in the database by the columns I will actually use?
        with bdb.savepoint():
            cursor = bdb.sql_execute(params_sql, (generator_id,))
            all_mus = {}
            all_sigmas = {}
            for (colno, modelno, mu, sigma) in cursor:
                if modelno not in all_mus:
                    all_mus[modelno] = {}
                if modelno not in all_sigmas:
                    all_sigmas[modelno] = {}
                assert colno not in all_mus[modelno]
                all_mus[modelno][colno] = mu
                assert colno not in all_sigmas[modelno]
                all_sigmas[modelno][colno] = sigma
            return (all_mus, all_sigmas)

    def column_dependence_probability(self, bdb, generator_id, modelno, colno0,
            colno1):
        # XXX Fix me!
        return 0

    def column_mutual_information(self, bdb, generator_id, modelno, colnos0,
            colnos1, constraints, numsamples):
        # XXX Fix me!
        return [0]

    def row_similarity(self, bdb, generator_id, modelno, rowid, target_rowid,
            colnos):
        # XXX Fix me!
        return 0

    def predict_confidence(self, bdb, generator_id, modelno, rowid, colno,
            numsamples=None):
        if colno < 0:
            return (0, 1)       # deviation of mode from mean is zero
        if modelno is None:
            modelnos = self._modelnos(bdb, generator_id)
            modelno = self.prng.choice(modelnos)
        mus, _sigmas = self._model_mus_sigmas(bdb, generator_id, modelno)
        return (mus[colno], 1.)

    def insert(self, bdb, generator_id, item):
        (_, colno, value) = item
        # Theoretically, I am supposed to detect and report attempted
        # repeat observations of already-observed cells, but since
        # there is no per-row latent structure, I will just treat all
        # row ids as fresh and not keep track of it.
        update_sql = '''
            UPDATE bayesdb_nig_normal_column
                SET count = count + 1, sum = sum + :x, sumsq = sumsq + :xsq
                WHERE generator_id = :generator_id
                    AND colno = :colno
        '''
        # This is Venture's SuffNormalSPAux.incorporate
        with bdb.savepoint():
            bdb.sql_execute(update_sql, {
                'generator_id': generator_id,
                'colno': colno,
                'x': value,
                'xsq': value * value
            })

    def remove(self, bdb, generator_id, item):
        (_, colno, value) = item
        update_sql = '''
            UPDATE bayesdb_nig_normal_column
                SET count = count - 1, sum = sum - :x, sumsq = sumsq - :xsq
                WHERE generator_id = :generator_id
                    AND colno = :colno
        '''
        # This is Venture's SuffNormalSPAux.unincorporate
        with bdb.savepoint():
            bdb.sql_execute(update_sql, {
                'generator_id': generator_id,
                'colno': colno,
                'x': value,
                'xsq': value * value
            })

    def infer(self, *args): return self.analyze_models(*args)

    def _gibbs_step_params(self, hypers, stats):
        # This is Venture's UNigNormalAAALKernel.simulate packaged differently.
        (mn, Vn, an, bn) = posterior_hypers(hypers, stats)
        new_var = self._inv_gamma(an, bn)
        new_mu = self.prng.gauss(mn, math.sqrt(new_var*Vn))
        ans = (new_mu, math.sqrt(new_var))
        return ans

    def _inv_gamma(self, shape, scale):
        return float(scale) / self.prng.gammavariate(shape, 1.0)

HALF_LOG2PI = 0.5 * math.log(2 * math.pi)

def logpdf_gaussian(x, mu, sigma):
    deviation = x - mu
    ans = - math.log(sigma) - HALF_LOG2PI \
        - (0.5 * deviation * deviation / (sigma * sigma))
    return ans

def data_suff_stats(bdb, table, column_name):
    # This is incorporate/remove in bulk, reading from the database.
    qt = sqlite3_quote_name(table)
    qcn = sqlite3_quote_name(column_name)
    # TODO Do this computation inside the database?
    gather_data_sql = '''
        SELECT %s FROM %s
    ''' % (qcn, qt)
    cursor = bdb.sql_execute(gather_data_sql)
    count = 0
    xsum = 0
    sumsq = 0
    for (item,) in cursor:
        count += 1
        xsum += item
        sumsq += item * item
    return (count, xsum, sumsq)

def posterior_hypers(hypers, stats):
    # This is Venture's CNigNormalOutputPSP.posteriorHypersNumeric
    (m, V, a, b) = hypers
    [ctN, xsum, xsumsq] = stats
    Vn = 1 / (1.0/V + ctN)
    mn = Vn*((1.0/V)*m + xsum)
    an = a + ctN / 2.0
    bn = b + 0.5*(m**2/float(V) + xsumsq - mn**2/Vn)
    ans = (mn, Vn, an, bn)
    return ans
