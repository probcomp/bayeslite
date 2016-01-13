'''Parser for tokenized crosscat generator expressions.'''

import collections

from bayeslite.exception import BQLError
from bayeslite.util import casefold

# guess is bool. subsample is False or an int. columns is a list of pairs
# (column name, type). dep_constraints is a list of (column names, dep), where
# column names is a list of column names and dep is a bool indicating whether
# they're dependent or independent.
GeneratorSchema = collections.namedtuple(
    'GeneratorSchema',
    ['guess', 'subsample', 'columns', 'dep_constraints'])


def parse(schema, subsample_default):
    '''Parses a generator schema as passed to CrosscatMetamodel.

    schema is a tokenized expression of the form [['GUESS', ['*']], ['x',
    'NUMERICAL'], ...] that is passed to CrosscatMetamodel.create_generator and
    represents the argument to "crosscat" in CREATE GENERATOR ... FOR ... USING
    crosscat(...).

    Returns a GeneratorSchema.

    See test_crosscat_generator_schema.py for examples.
    '''

    guess = False
    subsample = subsample_default
    columns = []
    dep_constraints = []
    for directive in schema:

        if directive == []:
            # Skip extra commas so you can write
            #
            #    CREATE GENERATOR t_cc FOR t USING crosscat(
            #        x,
            #        y,
            #        z,
            #    )
            continue

        if (not isinstance(directive, list) or len(directive) != 2 or
                not isinstance(directive[0], basestring)):
            raise BQLError(
                None,
                'Invalid crosscat column model directive: %r' % (directive,))

        op = casefold(directive[0])
        if op == 'guess' and directive[1] == ['*']:
            guess = True
        elif (op == 'subsample' and isinstance(directive[1], list) and
                len(directive[1]) == 1):
            subsample = _parse_subsample_clause(directive[1][0])
        elif op == 'dependent':
            constraint = (_parse_dependent_clause(directive[1]), True)
            dep_constraints.append(constraint)
        elif op == 'independent':
            constraint = (_parse_dependent_clause(directive[1]), False)
            dep_constraints.append(constraint)
        elif op != 'guess' and casefold(directive[1]) != 'guess':
            columns.append((directive[0], directive[1]))
        else:
            raise BQLError(
                None, 'Invalid crosscat column model: %r' % (directive),)
    return GeneratorSchema(
        guess=guess, subsample=subsample, columns=columns,
        dep_constraints=dep_constraints)


def _parse_subsample_clause(clause):
    if isinstance(clause, basestring) and casefold(clause) == 'off':
        return False
    elif isinstance(clause, int):
        return clause
    else:
        raise BQLError(None, 'Invalid subsampling: %r' % (clause,))


def _parse_dependent_clause(args):
    i = 0
    dep_columns = []
    while i < len(args):
        dep_columns.append(args[i])
        if i + 1 < len(args) and args[i + 1] != ',':
            raise BQLError(None, 'Invalid dependent columns: %r' % (args,))
        i += 2
    return dep_columns
