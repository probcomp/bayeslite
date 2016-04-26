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

"""Metamodel interface.

To be used to model data in a :class:`bayeslite.BayesDB` handle, a metamodel
must first be registered with :func:`bayesdb_register_metamodel`.

The Crosscat metamodel is registered by default, but we can suppress
that for illustration::

   import bayeslite
   import crosscat.LocalEngine
   from bayeslite.metamodels.crosscat import CrosscatMetamodel

   bdb = bayeslite.bayesdb_open(pathname='foo.bdb', builtin_metamodels=False)
   cc = crosscat.LocalEngine.LocalEngine(seed=0)
   bayeslite.bayesdb_register_metamodel(bdb, CrosscatMetamodel(cc))

Then you can model a table with Crosscat and query the probable
implications of the data in the table::

   bdb.execute('create generator t_cc for t using crosscat(guess(*))')
   bdb.execute('initialize 10 models for t_cc')
   bdb.execute('analyze t_cc for 10 iterations wait')
   for x in bdb.execute('estimate pairwise dependence probablity from t_cc'):
       print x
"""

builtin_metamodels = []
builtin_metamodel_names = set()

def bayesdb_builtin_metamodel(metamodel):
    name = metamodel.name()
    assert name not in builtin_metamodel_names
    builtin_metamodels.append(metamodel)
    builtin_metamodel_names.add(name)

def bayesdb_register_builtin_metamodels(bdb):
    """Register all builtin metamodels in `bdb`."""
    for metamodel in builtin_metamodels:
        bayesdb_register_metamodel(bdb, metamodel)

def bayesdb_register_metamodel(bdb, metamodel):
    """Register `metamodel` in `bdb`, creating any necessary tables.

    `metamodel` must not already be registered in any BayesDB, nor any
    metamodel by the same name.
    """
    name = metamodel.name()
    if name in bdb.metamodels:
        raise ValueError('Metamodel already registered: %s' % (name,))
    with bdb.savepoint():
        metamodel.register(bdb)
        bdb.metamodels[name] = metamodel

def bayesdb_deregister_metamodel(bdb, metamodel):
    """Deregister `metamodel`, which must have been registered in `bdb`."""
    name = metamodel.name()
    assert name in bdb.metamodels
    assert bdb.metamodels[name] == metamodel
    del bdb.metamodels[name]

class IBayesDBMetamodel(object):
    """BayesDB metamodel interface.

    Subclasses of :class:`IBayesDBMetamodel` implement the
    functionality needed by probabilistic BQL queries to sample from
    and inquire about the posterior distribution of a generative model
    conditioned on data in a table.  Instances of subclasses of
    `IBayesDBMetamodel` contain any in-memory state associated with
    the metamodel in the database.
    """

    def name(self):
        """Return the name of the metamodel as a str."""
        raise NotImplementedError

    def register(self, bdb):
        """Install any state needed for the metamodel in `bdb`.

        Called by :func:`bayeslite.bayesdb_register_metamodel`.

        Normally this will create SQL tables if necessary.
        """
        raise NotImplementedError

    def create_generator(self, bdb, table, schema, instantiate):
        """Create a generator for a table with the given schema.

        Called when executing ``CREATE GENERATOR``.

        Must parse `schema` to determine the column names and
        statistical types of the generator, and then call
        `instantiate` with a list of ``(column_name, stattype)``
        pairs.  `instantiate` will return a generator id and a list of
        ``(colno, column_name, stattype)`` triples.

        The generator id and column numbers may be used to create
        metamodel-specific records in the database for the generator
        with foreign keys referring to the ``bayesdb_generator`` and
        ``bayesdb_generator_column`` tables.

        `schema` is a list of schema items corresponding to the
        comma-separated ‘columns’ from a BQL ``CREATE GENERATOR``
        command.  Each schema item is a list of strings or lists of
        schema items, corresponding to whitespace-separated tokens and
        parenthesized lists.  Note that within parenthesized lists,
        commas are not excluded.
        """
        raise NotImplementedError

    def drop_generator(self, bdb, generator_id):
        """Drop any metamodel-specific records for a generator.

        Called when executing ``DROP GENERATOR``.
        """
        raise NotImplementedError

    def rename_column(self, bdb, generator_id, oldname, newname):
        """Note that a table column has been renamed.

        Not currently used.  To be used in the future when executing::

            ALTER TABLE <table> RENAME COLUMN <oldname> TO <newname>
        """
        raise NotImplementedError

    def initialize_models(self, bdb, generator_id, modelnos, model_config):
        """Initialize the specified model numbers for a generator."""
        raise NotImplementedError

    def drop_models(self, bdb, generator_id, modelnos=None):
        """Drop the specified model numbers of a generator.

        If none are specified, drop all models.
        """
        raise NotImplementedError

    def analyze_models(self, bdb, generator_id, modelnos=None, iterations=1,
            max_seconds=None, ckpt_iterations=None, ckpt_seconds=None):
        """Analyze the specified model numbers of a generator.

        If none are specified, analyze all of them.

        :param int iterations: maximum number of iterations of analysis for
            each model
        :param int max_seconds: requested maximum number of seconds to analyze
        :param int ckpt_iterations: number of iterations before committing
            results of analysis to the database
        :param int ckpt_seconds: number of seconds before committing results of
            anlaysis to the database
        """
        raise NotImplementedError

    def column_dependence_probability(self, bdb, generator_id, modelno, colno0,
            colno1):
        """Compute ``DEPENDENCE PROBABILITY OF <col0> WITH <col1>``."""
        raise NotImplementedError

    def column_mutual_information(self, bdb, generator_id, modelno, colno0,
            colno1, numsamples=100):
        """Compute ``MUTUAL INFORMATION OF <col0> WITH <col1>``."""
        raise NotImplementedError

    def row_similarity(self, bdb, generator_id, modelno, rowid, target_rowid,
            colnos):
        """Compute ``SIMILARITY TO <target_row>`` for given `rowid`."""
        raise NotImplementedError

    def predict(self, bdb, generator_id, modelno, colno, rowid, threshold,
            numsamples=None):
        """Predict a value for a column, if confidence is high enough."""
        value, confidence = self.predict_confidence(bdb, generator_id, modelno,
            colno, rowid, numsamples=numsamples)
        if confidence < threshold:
            return None
        return value

    def predict_confidence(self, bdb, generator_id, modelno, colno, rowid,
            numsamples=None):
        """Predict a value for a column and return confidence."""
        raise NotImplementedError

    def simulate_joint(self, bdb, generator_id, targets, constraints, modelno,
            num_predictions=1):
        """Simulate `targets` from a generator, subject to `constraints`.

        Returns a list of lists of values for the specified targets.

        `modelno` may be `None`, meaning "all models"

        `targets` is a list of ``(rowid, colno)`` pairs.

        `constraints` is a list of ``(rowid, colno, value)`` triples.

        `num_predictions` is the number of results to return.

        The results are samples from the distribution on targets,
        independent conditioned on (the latent state of the metamodel
        and) the constraints.
        """
        raise NotImplementedError

    def logpdf_joint(self, bdb, generator_id, targets, constraints,
            modelno=None):
        """Evalute the joint probability of `targets` subject to `constraints`.

        Returns the probability density of the targets (in log domain).

        `targets` is a list of ``(rowid, colno, value)`` triples.

        `constraints` is a list of ``(rowid, colno, value)`` triples.

        `modelno` is a model number or `None`, meaning all models.
        """
        raise NotImplementedError

    def insertmany(self, bdb, generator_id, rows):
        """Insert `rows` into a generator, updating analyses accordingly.

        `rows` is a list of tuples with one value for each column
        modelled by the generator.
        """
        raise NotImplementedError
