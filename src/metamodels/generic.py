'''Madeleine proposes that she write a GenericMetamodel class that implements
IBayesDBMetamodel. This class will wrap other models, including gpmcc and
Crosscat. It will replace src/metamodels/crosscat.py and the gpmcc_metamodel
package.

When a generator is initialized, it will create a ModelState for each model.
When a generator is analyzed, it will call the transition_* methods on the
models. When BQL-relevant calls are made, it will translate those to GPM calls
and call the appropriate methods on the model property of the GPM state.

The main purposes of this project are:
    (1) Move all metamodel SQL code into a single module. (Serialization will
        not be backward compatible; existing bdbs will use CrosscatMetamodel.)
    (2) Reduce the amount of bayeslite-specific code necessary to implement a
        new model to just the GPM interface.
    (3) Allow us to write alternative implementations of GenericMetamodel that
        are remote or distributed.
'''

from bayeslite.metamodel import IBayesDBMetamodel
from collections import namedtuple


class GenericMetamodel(IBayesDBMetamodel):
    # TODO: Code goes here.
    pass


class ColumnQuery(namedtuple('_ColumnQuery', ['row', 'colno'])):
    '''Specifies a cell in the data. row can be None to indicate "unobserved."
    colno is zero-based.'''
    pass


class ColumnValue(namedtuple('_ColumnValue', ['row', 'colno', 'value'])):
    '''Similar to ColumnQuery, but with a value assigned to that cell, in the
    original domain of the user-supplied data.'''
    pass


class GenerativeProbabilityModel(object):
    '''See the BayesDB paper for the semantics.'''

    def simulate(self, givens, targets):
        '''givens is a list of ColumnValue. targets is a list of
        ColumnQuery.'''
        raise NotImplementedError

    def logpdf(self, givens, query):
        '''givens and query are lists of ColumnValue.'''
        raise NotImplementedError

    def kl_divergence(self, measurements_a, measurements_b):
        '''measurements_a and measurements_b are lists of ColumnQuery.'''
        raise NotImplementedError


class ModelState(object):
    '''Represents a mutable model in training. Corresponds to a single BQL
    model.'''

    def serialize(self):
        '''Serializes the state of this object to a bytes and returns it.'''
        raise NotImplementedError

    @classmethod
    def deserialize(cls, data):
        '''Recreates a ModelState from a bytes returned by serialize. Returns a
        ModelState.'''
        raise NotImplementedError

    @classmethod
    def schema_column_names(cls, schema):
        '''schema is a list-of-lists representing a BQL schema. See
        IBayesDBMetamodel.create_generator for the format. Returns a list of
        unicode column names.'''

    @classmethod
    def from_records(cls, array, column_names, schema, random_seed):
        '''Creates a new ModelState from array, a numpy array with columns
        named by column_names. These column_names were extracted from schema by
        an earlier call to schema_column_names with the same schema. bayeslite
        can't implement it with a single call because it needs to know the
        names of the columns to load into array.'''
        raise NotImplementedError

    @property
    def iterations():
        '''The number of Markov chain transitions completed by this state.'''
        raise NotImplementedError

    def transition_iterations(self, iterations):
        '''Performs the specified number of Markov chain iterations. If it is
        interrupted, no methods will ever be called on the object again.'''
        raise NotImplementedError

    def transition_seconds(self, seconds):
        '''Performs the Markov chain iterations for the specified number of
        (floating point) seconds. If it is interrupted, no methods will ever be
        called on the object again.'''
        raise NotImplementedError

    @property
    def model(self):
        '''A GenerativeProbabilityModel that reflects the current
        state. This object may be invalidated by calls to transition_iterations
        or transition_seconds, but if it is, its methods must raise exceptions
        instead of silently returning wrong values.'''
        raise NotImplementedError
