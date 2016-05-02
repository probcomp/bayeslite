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

import os
import sys

class StochasticError(Exception):
    def __init__(self, seed, exctype, excvalue):
        self.seed = seed
        self.exctype = exctype
        self.excvalue = excvalue
    def __str__(self):
        hexseed = self.seed.encode('hex')
        if hasattr(self.exctype, '__name__'):
            typename = self.exctype.__name__
        else:
            typename = repr(self.exctype)
        return '[seed %s]\n%s: %s' % (hexseed, typename, self.excvalue)

def stochastic(max_runs, min_passes):
    assert 0 < max_runs
    assert min_passes <= max_runs
    def wrap(f):
        def f_(seed=None):
            if seed is not None:
                return f(seed)
            npasses = 0
            last_seed = None
            last_exc_info = None
            for i in xrange(max_runs):
                seed = os.urandom(32)
                try:
                    value = f(seed)
                except:
                    last_seed = seed
                    last_exc_info = sys.exc_info()
                else:
                    npasses += 1
                    if min_passes <= npasses:
                        return value
            t, v, tb = last_exc_info
            raise StochasticError, StochasticError(last_seed, t, v), tb
        return f_
    return wrap
