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

"""Backend interface.

To be used as a modeling and inference backend in a :class:`bayeslite.BayesDB`
handle, a backends must first be registered with
:func:`bayesdb_register_backend`.

The CGPM backend is registered by default, but we can suppress
that for illustration::

   import bayeslite
   from bayeslite.backends.cgpm_backend import CGPM_Backend

   bdb = bayeslite.bayesdb_open(pathname='foo.bdb', builtin_backends=False)
   backend = CGPM_Backend(cgpm_registry=dict(), multiprocess=False)
   bayeslite.bayesdb_register_backend(bdb, backend)

Then you can model a table and query the probable implications of the data in
the table::

   bdb.execute('create population p for t with schema(guess stattypes for (*))')
   bdb.execute('create generator p_cc for t using cgpm;')
   bdb.execute('initialize 10 models for t_cc')
   bdb.execute('analyze t_cc for 10 iterations wait')
   for x in bdb.execute('estimate pairwise dependence probability from t_cc'):
       print x
"""

from bayeslite.util import cursor_value

builtin_backends = []
builtin_backend_names = set()

def bayesdb_builtin_backend(backend):
    name = backend.name()
    assert name not in builtin_backend_names
    builtin_backends.append(backend)
    builtin_backend_names.add(name)

def bayesdb_register_builtin_backends(bdb):
    """Register all builtin backends in `bdb`."""
    for backend in builtin_backends:
        bayesdb_register_backend(bdb, backend)

def bayesdb_register_backend(bdb, backend):
    """Register `backend` in `bdb`, creating any necessary tables.

    `backend` must not already be registered in any BayesDB, nor any
    backend by the same name.
    """
    name = backend.name()
    if name in bdb.backends:
        raise ValueError('Backend already registered: %s' % (name,))
    with bdb.savepoint():
        backend.register(bdb)
        bdb.backends[name] = backend

def bayesdb_deregister_backend(bdb, backend):
    """Deregister `backend`, which must have been registered in `bdb`."""
    name = backend.name()
    assert name in bdb.backends
    assert bdb.backends[name] == backend
    del bdb.backends[name]

def bayesdb_backend_version(bdb, mm_name):
    cursor = bdb.sql_execute('''
        SELECT version FROM bayesdb_backend WHERE name = ?
    ''', (mm_name,))
    return cursor_value(cursor, nullok=True)

class BayesDB_Backend(object):
    """BayesDB backend interface.

    Subclasses of :class:`BayesDB_Backend` implement the
    functionality needed by probabilistic BQL queries to sample from
    and inquire about the posterior distribution of a generative model
    conditioned on data in a table.  Instances of subclasses of
    `BayesDB_Backend` contain any in-memory state associated with
    the backend in the database.
    """

    def name(self):
        """Return the name of the backend as a str."""
        raise NotImplementedError

    def register(self, bdb):
        """Install any state needed for the backend in `bdb`.

        Called by :func:`bayeslite.bayesdb_register_backend`.

        Normally this will create SQL tables if necessary.
        """
        raise NotImplementedError

    def set_multiprocess(self, switch):
        """Switch between multiprocessing and single processing.

        The boolean variable `switch` toggles between single (`False`) and multi
        (`True`) processing, if the choice is available, and otherwise ignores
        the request.
        """
        raise NotImplementedError

    def create_generator(self, bdb, table, schema, **kwargs):
        """Create a generator for a table with the given schema.

        Called when executing ``CREATE GENERATOR``.

        Must parse `schema` to build the generator.

        The generator id and column numbers may be used to create
        backend-specific records in the database for the generator
        with foreign keys referring to the ``bayesdb_generator`` and
        ``bayesdb_variable`` tables.

        `schema` is a list of schema items corresponding to the
        comma-separated ‘columns’ from a BQL ``CREATE GENERATOR``
        command.  Each schema item is a list of strings or lists of
        schema items, corresponding to whitespace-separated tokens and
        parenthesized lists.  Note that within parenthesized lists,
        commas are not excluded.
        """
        raise NotImplementedError

    def drop_generator(self, bdb, generator_id):
        """Drop any backend-specific records for a generator.

        Called when executing ``DROP GENERATOR``.
        """
        raise NotImplementedError

    def rename_column(self, bdb, generator_id, oldname, newname):
        """Note that a table column has been renamed.

        Not currently used.  To be used in the future when executing::

            ALTER TABLE <table> RENAME COLUMN <oldname> TO <newname>
        """
        raise NotImplementedError

    def add_column(self, bdb, generator_id, colno):
        """Add `colno` from the population as a variable in the backend.

        Used by the MML::

            ALTER POPULATION <population> ADD VARIABLE <variable> <stattype>
        """
        raise NotImplementedError

    def initialize_models(self, bdb, generator_id, modelnos):
        """Initialize the specified model numbers for a generator."""
        raise NotImplementedError

    def drop_models(self, bdb, generator_id, modelnos=None):
        """Drop the specified model numbers of a generator.

        If none are specified, drop all models.
        """
        raise NotImplementedError

    def alter(self, bdb, generator_id, modelnos, commands):
        """Modify the generator according to the metamdoel-specific commands.

        Used by the MML::

            ALTER GENERATOR <generator> [MODELS [(<modelnos>)]]
                commands...
        """
        raise NotImplementedError

    def analyze_models(self, bdb, generator_id, modelnos=None, iterations=1,
            max_seconds=None, ckpt_iterations=None, ckpt_seconds=None,
            program=None):
        """Analyze the specified model numbers of a generator.

        If none are specified, analyze all of them.

        :param int iterations: maximum number of iterations of analysis for
            each model
        :param int max_seconds: requested maximum number of seconds to analyze
        :param int ckpt_iterations: number of iterations before committing
            results of analysis to the database
        :param int ckpt_seconds: number of seconds before committing results of
            analysis to the database
        :param object program: None, or list of tokens of analysis program
        """
        raise NotImplementedError

    def column_dependence_probability(self, bdb, generator_id, modelnos, colno0,
            colno1):
        """Compute ``DEPENDENCE PROBABILITY OF <col0> WITH <col1>``."""
        raise NotImplementedError

    def column_mutual_information(self, bdb, generator_id, modelnos, colnos0,
            colnos1, constraints=None, numsamples=100):
        """Compute ``MUTUAL INFORMATION OF (<cols0>) WITH (<cols1>)``."""
        raise NotImplementedError

    def row_similarity(self, bdb, generator_id, modelnos, rowid, target_rowid,
            colnos):
        """Compute ``SIMILARITY TO <target_row>`` for given `rowid`."""
        raise NotImplementedError

    def predictive_relevance(self, bdb, generator_id, modelnos, rowid_target,
            rowid_query, hypotheticals, colno):
        """Compute predictive relevance, also known as relevance probability.

        `rowid_target` is an integer.

        `rowid_query` is a list of integers.

        `hypotheticals` is a list of hypothetical observations, where each item
            is itself a list of ``(colno, value)`` pairs.
        """
        raise NotImplementedError


    def predict(self, bdb, generator_id, modelnos, rowid, colno, threshold,
            numsamples=None):
        """Predict a value for a column, if confidence is high enough."""
        value, confidence = self.predict_confidence(
            bdb, generator_id, modelnos, rowid, colno, numsamples=numsamples)
        if confidence < threshold:
            return None
        return value

    def predict_confidence(self, bdb, generator_id, modelnos, rowid, colno,
            numsamples=None):
        """Predict a value for a column and return confidence."""
        raise NotImplementedError

    def simulate_joint(self, bdb, generator_id, modelnos, rowid, targets,
            constraints, num_samples=1, accuracy=None):
        """Simulate `targets` from a generator, subject to `constraints`.

        Returns a list of lists of values for the specified targets.

        `rowid` is an integer.

        `modelno` may be `None`, meaning "all models"

        `targets` is a list of ``(colno)``.

        `constraints` is a list of ``(colno, value)`` pairs.

        `num_samples` is the number of results to return.

        `accuracy` is a generic parameter (usually int) which specifies the
        desired accuracy, compute time, etc if the simulations are approximately
        distributed from the true target.

        The results are samples from the distribution on targets,
        independent conditioned on (the latent state of the backend
        and) the constraints.
        """
        raise NotImplementedError

    def logpdf_joint(self, bdb, generator_id, modelnos, rowid, targets,
            constraints):
        """Evalute the joint probability of `targets` subject to `constraints`.

        Returns the probability density of the targets (in log domain).

        `rowid` is an integer.

        `targets` is a list of ``(colno, value)`` pairs.

        `constraints` is a list of ``(colno, value)`` pairs.

        `modelno` is a model number or `None`, meaning all models.
        """
        raise NotImplementedError
