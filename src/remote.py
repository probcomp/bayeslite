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

try:
    from pkg_resources import parse_version
except ImportError:
    # XXX Consider requiring setuptools
    def parse_version(v):
        return 1
import requests
import warnings

from bayeslite.version import __version__

def version_check(warn_only=True):
    """Check bayeslite version against remote server.

    Warn, with `warnings.warn`, if the server reports the version not
    current.
    """
    SERVICE = 'https://projects.csail.mit.edu/probcomp/bayesdb/bayeslite.version'

    # arg: {'package':'bayeslite','version':'something'}
    # response: {'version':'0.5','url':'http://probcomp.org/bayesdb/release'}
    payload = [
        ('package', 'bayeslite'),
        ('version', __version__),
    ]
    headers = {
        'User-Agent': 'bayeslite %s' % (__version__,),
    }

    try:
        # TODO: It would be nice to be async about this. Set 1 second timeout.
        r = requests.get(SERVICE, params=payload, timeout=1, headers=headers)
        if r.status_code != 200:
            return
        d = r.json()
        if parse_version(__version__) < parse_version(d['version']):
            outofdate_warning = 'Bayeslite is not up to date.' \
                '\nYou are running %s; the latest version is %s.' \
                '\nSee <%s>.' \
                % (__version__, d['version'], d['url'])
            warnings.warn(outofdate_warning)
            if not warn_only:
                raise ValueError(outofdate_warning)
    except Exception:
        if warn_only:
            # Silently eat exceptions -- in the request and in parsing the
            # result, in case it is ill-formed.
            pass
        else:
            raise
