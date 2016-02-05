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

import pytest
import tempfile

from bayeslite import bayesdb_open
from bayeslite import bayesdb_upgrade_schema
from bayeslite.exception import BayesDBException
from bayeslite.schema import bayesdb_schema_required
from bayeslite.schema import STALE_VERSIONS, USABLE_VERSIONS

import test_core

def test_stale_schemas():
    for stale_version in STALE_VERSIONS:
        with pytest.raises(IOError):
            bayesdb_open(version=stale_version)

def test_in_memory():
    bayesdb_open()

def test_schema_upgrade_on_open():
    for old_version in USABLE_VERSIONS[:-1]:
        with tempfile.NamedTemporaryFile(prefix='bayeslite') as f:
            try:
                with bayesdb_open(pathname=f.name,
                                  version=old_version, compatible=True) as bdb:
                    for needs_version in USABLE_VERSIONS:
                        case = 'has%s needs%s' % (old_version, needs_version)
                        if needs_version <= old_version:
                            bayesdb_schema_required(
                                bdb, needs_version, case + ' ok')
                        else:
                            try:
                                with pytest.raises(BayesDBException):
                                    bayesdb_schema_required(
                                        bdb, needs_version, case + ' fail')
                            except:
                                print case, "should fail"
                                raise
                    test_core.t1_schema(bdb)
                    test_core.t1_data(bdb)
                with bayesdb_open(pathname=f.name, compatible=False) as bdb:
                    for needs_version in USABLE_VERSIONS:
                        bayesdb_schema_required(
                            bdb, needs_version,
                            'needs%s after upgrade' % (needs_version,))
                    with pytest.raises(BayesDBException):
                        # Nobody'll ever bump the schema version this many
                        # times, right?
                        bayesdb_schema_required(bdb, 1000000, 'a gazillion')
            except:
                print "old_version =", old_version, "file =", f.name
                raise

def test_schema_compatible():
    for i, old_version in enumerate(USABLE_VERSIONS[:-1]):
        for new_version in USABLE_VERSIONS[i+1:]:
            with tempfile.NamedTemporaryFile(prefix='bayeslite') as f:
                with bayesdb_open(pathname=f.name, version=old_version) as bdb:
                    for same_or_older_version in USABLE_VERSIONS[:i+1]:
                        bayesdb_schema_required(
                            bdb, same_or_older_version,
                            'has%s needs% ok' % (old_version,
                                                 same_or_older_version))
                    msg = 'has%s needs%s should fail' % (
                        old_version, new_version)
                    try:
                        with pytest.raises(BayesDBException):
                            bayesdb_schema_required(bdb, new_version, msg)
                    except:
                        print msg
                        raise
                # Now open it in compatible mode. Nothing should change.
                with bayesdb_open(pathname=f.name, compatible=True) as bdb:
                    bayesdb_schema_required(bdb, old_version,
                                            'opened compatible, old still ok')
                    with pytest.raises(BayesDBException):
                        bayesdb_schema_required(
                            bdb, new_version,
                            'opened compatible, needs%s still fails' % (
                                new_version,))
                    # Now explicitly upgrade. Then everything should be okay.
                    bayesdb_upgrade_schema(bdb)
                with bayesdb_open(pathname=f.name, compatible=True) as bdb:
                    for v in USABLE_VERSIONS:
                        bayesdb_schema_required(
                            bdb, v, 'after explicit upgrade, needs%s ok' % (v,))
