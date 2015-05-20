# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2014, MIT Probabilistic Computing Project
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

class BayesDBException(Exception):
    """Exceptions associated with a BayesDB instance.

    :ivar bayeslite.BayesDB bayesdb: associated BayesDB instance
    """

    def __init__(self, bayesdb, *args, **kwargs):
        self.bayesdb = bayesdb
        super(BayesDBException, self).__init__(*args, **kwargs)

class BQLError(BayesDBException):
    """Errors in compiling or executing BQL."""
    pass
