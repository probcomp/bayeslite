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

import requests
import json
import warnings

import bayeslite.version

FAIL_VERSION_CHECK = True

def version_check():
    """Send bayeslite version tracking bug to a remote server.

    Warn, with `warnings.warn`, if the server reports the version not
    current.
    """
    SERVICE = 'https://2wh8htmfnj.execute-api.us-east-1.amazonaws.com/prod/bdbVersionCheck'

    # arg: {'package':'bayeslite','version':'something','build':'something-else'}
    # response: {'result':'current'} or
    # {'result':'old', 'message':'A newer version of bayeslite is available',
    #  'version':'0.5','url':'http://probcomp.org/bayesdb/release'}
    payload = {
        'package': 'bayeslite',
        'version': bayeslite.version.__version__,
    }

    # TODO: It would be nice to be async about this. Set 1 second timeout.
    try:
        r = requests.post(SERVICE, data=json.dumps(payload), timeout=1)
    except Exception:
        # Silently eat exceptions.
        pass
    else:
        if FAIL_VERSION_CHECK or \
           (r.status_code == 200 and r.json.result != "current"):
            warnings.warn('Bayeslite is not up to date.'
                '  Version %s is available.\nSee %s'
                % (r.json.version, r.json.url))
