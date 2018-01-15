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

import StringIO

class BayesLiteException(Exception):
    """Parent exception for anything Bayeslite-specific."""
    pass

class BayesDBException(BayesLiteException):
    """Exceptions associated with a BayesDB instance.

    :ivar bayeslite.BayesDB bayesdb: associated BayesDB instance
    """
    # XXX: Consider renaming to BayesDBError to match the two below.
    def __init__(self, bayesdb, *args, **kwargs):
        self.bayesdb = bayesdb
        super(BayesDBException, self).__init__(*args, **kwargs)

class BQLError(BayesDBException):
    """Errors in interpreting or executing BQL on a particular database."""
    # XXX Consider separating the "no such foo" and "foo already exists" errors
    # that actually could be fine on another database, from the "foo is a
    # 1-row function" and "foo needs exactly two columns" type that are closer
    # to a BQLParseError.  Unsure what the "ESTIMATE * FROM COLUMNS OF subquery"
    # use really means as an error: need to look more closely.
    pass

class BQLParseError(BayesLiteException):
    """Errors in parsing BQL.

    As many parse errors as can be reasonably detected are listed
    together.

    :ivar list errors: list of strings describing parse errors
    """

    def __init__(self, errors):
        assert 0 < len(errors)
        self.errors = errors

    def __str__(self):
        if len(self.errors) == 1:
            return self.errors[0]
        else:
            out = StringIO.StringIO()
            for error in self.errors:
                out.write('  %s\n' % (error,))
            return out.getvalue()
