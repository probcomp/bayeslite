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

import apsw
import json

import bayeslite.bqlfn as bqlfn


class Mutinf(object):
    MI = 0
    POPULATION_ID = 1
    GENERATOR_ID = 2
    TARGET_VARS = 3
    REFERENCE_VARS = 4
    CONDITIONS = 5
    NSAMPLES = 6


class MutinfModule(object):

    def __init__(self, bdb):
        self._bdb = bdb

    def Connect(self, connection, _modulename, _databasename, _tablename,
            *_args):
        schema = '''
            create table t(
                mi real not null,
                population_id integer not null,
                generator_id integer,
                target_vars text not null,      -- json list
                reference_vars text not null,   -- json list
                conditions text,                -- json dict
                nsamples integer
            )
        '''
        table = MutinfTable(self._bdb)
        return schema, table

    Create = Connect


class MutinfTable(object):

    def __init__(self, bdb):
        self._bdb = bdb

    def Open(self):
        return MutinfCursor(self._bdb)

    def BestIndex(self, constraints, _orderbys):
        # Parse all the constraints to find where the arguments we
        # care about are specified.
        #
        # XXX Tidy this up and generalize this mechanism.
        need = 0
        need |= 1 << Mutinf.POPULATION_ID
        need |= 1 << Mutinf.TARGET_VARS
        need |= 1 << Mutinf.REFERENCE_VARS
        have = 0
        population_id = -1
        generator_id = -1
        target_vars = -1
        reference_vars = -1
        conditions = -1
        nsamples = -1
        for i, (c, op) in enumerate(constraints):
            if op != apsw.SQLITE_INDEX_CONSTRAINT_EQ:
                continue
            if c == Mutinf.POPULATION_ID:
                population_id = i
            elif c == Mutinf.GENERATOR_ID:
                generator_id = i
            elif c == Mutinf.TARGET_VARS:
                target_vars = i
            elif c == Mutinf.REFERENCE_VARS:
                reference_vars = i
            elif c == Mutinf.CONDITIONS:
                conditions = i
            elif c == Mutinf.NSAMPLES:
                nsamples = i
            else:
                continue
            have |= 1 << c
        if need & ~have:
            # XXX Report clearer error message with names.
            raise Exception('Missing constraints: %x' % (need & ~have,))

        # Specify which constraints should be passed through as
        # arguments to the cursor's Filter function.
        index_info = [None] * len(constraints)
        count = _Count()
        index_info[population_id] = count.next()
        if have & (1 << Mutinf.GENERATOR_ID):
            index_info[generator_id] = count.next()
        index_info[target_vars] = count.next()
        index_info[reference_vars] = count.next()
        if have & (1 << Mutinf.CONDITIONS):
            index_info[conditions] = count.next()
        if have & (1 << Mutinf.NSAMPLES):
            index_info[nsamples] = count.next()

        return (index_info, have)


class MutinfCursor(object):

    def __init__(self, bdb):
        self._bdb = bdb
        self._rowid = None
        self._mi = None
        self._population_id = None
        self._generator_id = None
        self._target_vars = None
        self._reference_vars = None
        self._conditions = None
        self._nsamples = None

    def Close(self):
        pass

    def Column(self, number):
        return (
            self._rowid,
            self._mi[self._rowid],
            self._population_id,
            self._generator_id,
            self._target_vars,
            self._reference_vars,
            self._conditions,
            self._nsamples,
        )[number + 1]

    def Next(self):
        self._rowid += 1

    def Rowid(self):
        return self._rowid

    def Eof(self):
        return not self._rowid < len(self._mi)

    def Filter(self, indexnum, indexname, constraintargs):
        self._rowid = 0
        if self._population_id is not None:
            # Already initialized, reset only.
            return

        # MutinfTable.BestIndex should have guaranteed the required
        # arguments were passed through.
        assert indexnum & (1 << Mutinf.POPULATION_ID)
        assert indexnum & (1 << Mutinf.TARGET_VARS)
        assert indexnum & (1 << Mutinf.REFERENCE_VARS)

        # Grab the argument values that are available.
        count = _Count()
        self._population_id = constraintargs[count.next()]
        if indexnum & (1 << Mutinf.GENERATOR_ID):
            self._generator_id = constraintargs[count.next()]
        else:
            self._generator_id = None
        self._target_vars = constraintargs[count.next()]
        self._reference_vars = constraintargs[count.next()]
        if indexnum & (1 << Mutinf.CONDITIONS):
            self._conditions = constraintargs[count.next()]
        else:
            self._conditions = None
        if indexnum & (1 << Mutinf.NSAMPLES):
            self._nsamples = constraintargs[count.next()]
        else:
            self._nsamples = None

        # Parse the argument values that we need to parse.
        target_vars = json.loads(self._target_vars)
        reference_vars = json.loads(self._reference_vars)
        conditions_strkey = {} if self._conditions is None else \
            json.loads(self._conditions)
        conditions = \
            {int(k): v for k, v in conditions_strkey.iteritems()}

        # Compute the mutual information.
        #
        # XXX Expose this API better from bqlfn.
        mis = bqlfn._bql_column_mutual_information(
            self._bdb, self._population_id, self._generator_id,
            target_vars, reference_vars, self._nsamples,
            *_flatten2(sorted(conditions.iteritems())))
        self._mi = _flatten2(mis)


### Utilities

def _flatten2(xss):
    """Flatten a list of lists."""
    return [x for xs in xss for x in xs]

class _Count(object):
    """x = 0; f(x++); g(x++); h(x++) idiom from C."""
    def __init__(self):
        self._c = 0
    def next(self):
        c = self._c
        self._c += 1
        return c
