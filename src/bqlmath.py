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

import inspect
import math

bqlmath_funcs = {
    'acos'      : lambda _bdb, x    : math.acos(x),
    'acosh'     : lambda _bdb, x    : math.acosh(x),
    'asin'      : lambda _bdb, x    : math.asin(x),
    'asinh'     : lambda _bdb, x    : math.asinh(x),
    'atan'      : lambda _bdb, x    : math.atan(x),
    'atan2'     : lambda _bdb, x    : math.atan2(x),
    'atanh'     : lambda _bdb, x    : math.atanh(x),
    'ceil'      : lambda _bdb, x    : math.ceil(x),
    'copysign'  : lambda _bdb, x, y : math.copysign(x, y),
    'cos'       : lambda _bdb, x    : math.cos(x),
    'cosh'      : lambda _bdb, x    : math.cosh(x),
    'degrees'   : lambda _bdb, x    : math.degrees(x),
    'erf'       : lambda _bdb, x    : math.erf(x),
    'erfc'      : lambda _bdb, x    : math.erfc(x),
    'exp'       : lambda _bdb, x    : math.exp(x),
    'expm1'     : lambda _bdb, x    : math.expm1(x),
    'fabs'      : lambda _bdb, x    : math.fabs(x),
    'factorial' : lambda _bdb, x    : math.factorial(x),
    'floor'     : lambda _bdb, x    : math.floor(x),
    'fmod'      : lambda _bdb, x, y : math.fmod(x,y),
    'frexp'     : lambda _bdb, x    : math.frexp(x),
    'gamma'     : lambda _bdb, x    : math.gamma(x),
    'hypot'     : lambda _bdb, x, y : math.hypot(x,y),
    'ldexp'     : lambda _bdb, x, i : math.ldexp(x,i),
    'lgamma'    : lambda _bdb, x    : math.lgamma(x),
    'log'       : lambda _bdb, x    : math.log(x),
}


def bayesdb_install_bqlmath(db, cookie):
    for name, fn in bqlmath_funcs.iteritems():
        nargs = len(inspect.getargspec(fn).args)-1
        db.createscalarfunction(name, (lambda *a: fn(cookie, *a)), nargs)
