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
    'acos'      : lambda x    : math.acos(x),
    'acosh'     : lambda x    : math.acosh(x),
    'asin'      : lambda x    : math.asin(x),
    'asinh'     : lambda x    : math.asinh(x),
    'atan'      : lambda x    : math.atan(x),
    'atan2'     : lambda x    : math.atan2(x),
    'atanh'     : lambda x    : math.atanh(x),
    'ceil'      : lambda x    : math.ceil(x),
    'copysign'  : lambda x, y : math.copysign(x, y),
    'cos'       : lambda x    : math.cos(x),
    'cosh'      : lambda x    : math.cosh(x),
    'degrees'   : lambda x    : math.degrees(x),
    'erf'       : lambda x    : math.erf(x),
    'erfc'      : lambda x    : math.erfc(x),
    'exp'       : lambda x    : math.exp(x),
    'expm1'     : lambda x    : math.expm1(x),
    'fabs'      : lambda x    : math.fabs(x),
    'factorial' : lambda x    : math.factorial(x),
    'floor'     : lambda x    : math.floor(x),
    'fmod'      : lambda x, y : math.fmod(x,y),
    'frexp'     : lambda x    : math.frexp(x),
    'gamma'     : lambda x    : math.gamma(x),
    'hypot'     : lambda x, y : math.hypot(x,y),
    'ldexp'     : lambda x, i : math.ldexp(x,i),
    'lgamma'    : lambda x    : math.lgamma(x),
    'log'       : lambda x    : math.log(x),
}


def bayesdb_install_bqlmath(db, _cookie):
    for name, fn in bqlmath_funcs.iteritems():
        nargs = len(inspect.getargspec(fn).args)
        db.createscalarfunction(name, fn, nargs)
