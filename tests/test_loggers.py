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

import json
import time

from bayeslite import loggers

class StubCallable:
    def __init__(self, sleep=None, throw=None):
        self.calls = []
        self.sleep = sleep
        self.throw = throw
    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.sleep:
            time.sleep(self.sleep)
        if self.throw:
            raise self.throw  # pylint: disable=raising-bad-type
            # https://github.com/PyCQA/pylint/issues/157

def test_failstub():
    failstub = StubCallable(throw=NotImplementedError('foo'))
    try:
        failstub('sanity')
        assert False
    except NotImplementedError:
        pass
    assert 1 == len(failstub.calls)
    assert "(('sanity',), {})" == str(failstub.calls[0])

def test_logged_query_no_name():
    stubc = StubCallable()
    lgr = loggers.CallHomeStatusLogger(post=stubc)
    with loggers.logged_query(query_string='q', bindings=('b',), logger=lgr):
        stubc('inside')
    # Just the one call: nothing posted.
    assert "[(('inside',), {})]" == str(stubc.calls)


THE_USUAL = {'query_string': 'q',
             'bindings': ('b',),
             'name': 'test_logged_query_success'}
def check_logcall(logcall):
    assert logcall[0][0].startswith('https://projects.csail.mit.edu/probcomp')
    msg = logcall[1]
    assert 'data' in msg
    msgdata = msg['data']
    assert 'User-Agent' in msgdata
    assert 'session_json' in msgdata
    logdata = json.loads(msgdata['session_json'])
    assert 'entries' in logdata
    assert 'fields' in logdata
    assert 'version' in logdata
    entries = logdata['entries']
    the_only_entry = entries[0]
    assert "test_logged_query_success" == the_only_entry[0]
    assert ['q', ['b']] == the_only_entry[2]

def test_logged_query_success():
    query_stub = StubCallable()
    post_stub = StubCallable()
    lgr = loggers.CallHomeStatusLogger(post=post_stub)
    with loggers.logged_query(logger=lgr, **THE_USUAL):
        query_stub('inside')
    assert 1 == len(query_stub.calls)
    assert "(('inside',), {})" == str(query_stub.calls[0])
    time.sleep(0.2)  # To let the call-home thread run, so this is less flaky.
    assert 1 == len(post_stub.calls)
    check_logcall(post_stub.calls[0])

def test_logged_query_dataframe():
    from pandas import DataFrame
    df = DataFrame({'a': [1, 2.3, -4], # complex(4, -5)],
                    # Complex is broken, even with the default
                    # handler special case.
                    # See https://github.com/pydata/pandas/issues/12554
                    'b': [float('nan'), None, 'N/A']})

    query_stub = StubCallable()
    post_stub = StubCallable()
    lgr = loggers.CallHomeStatusLogger(post=post_stub)
    with loggers.logged_query(logger=lgr,
                              query_string='q', bindings= (df,), name='n'):
        query_stub('inside')
    assert 1 == len(query_stub.calls)
    assert "(('inside',), {})" == str(query_stub.calls[0])
    time.sleep(0.2)  # To let the call-home thread run, so this is less flaky.
    assert 1 == len(post_stub.calls)
    posted = post_stub.calls[0][1]['data']['session_json']
    data = json.loads(posted)
    df = data['entries'][0][2][1]
    assert ['{"a":{"0":1.0,"1":2.3,"2":-4.0},'
            # {"mathjs":"Complex","re":4,"im":-5}},' Complex broken. See above.
            '"b":{"0":null,"1":null,"2":"N\/A"}}'] == df

def test_logged_query_successful_log_failure():
    okstub = StubCallable()
    failstub = StubCallable(throw=NotImplementedError('foo'))
    lgr = loggers.CallHomeStatusLogger(post=failstub)
    with loggers.logged_query(logger=lgr, **THE_USUAL):
        okstub('inside')
    assert 1 == len(okstub.calls)
    assert "[(('inside',), {})]" == str(okstub.calls)
    # There will have been a failure on another thread,
    # and it will have been ignored.
    time.sleep(0.2) # To let the call-home thread run, so this is less flaky.
    assert 1 == len(failstub.calls)
    check_logcall(failstub.calls[0])

def test_logged_query_fail():
    # If the query itself fails, we should not impede that floating to top.
    failstub = StubCallable(throw=NotImplementedError('foo'))
    okstub = StubCallable()
    lgr = loggers.CallHomeStatusLogger(post=okstub)
    try:
        with loggers.logged_query(logger=lgr, **THE_USUAL):
            failstub('die')
        assert False
    except NotImplementedError:
        pass
    time.sleep(0.2) # To let the call-home thread run, so this is less flaky.
    assert 1 == len(okstub.calls)
    check_logcall(okstub.calls[0])

    assert 1 == len(failstub.calls)
    assert "[(('die',), {})]" == str(failstub.calls)

def test_logged_query_reporting_timeout():
    okstub = StubCallable()
    slumbertime = 1
    slowstub = StubCallable(sleep=slumbertime)
    start_time = time.time()
    lgr = loggers.CallHomeStatusLogger(post=slowstub)
    with loggers.logged_query(logger=lgr, **THE_USUAL):
        okstub('inside')
    elapsed_time = time.time() - start_time
    time.sleep(0.2) # To let the call-home thread run, so this is less flaky.
    # Success after done.
    assert 1 == len(okstub.calls)
    assert "(('inside',), {})" == str(okstub.calls[0])
    assert elapsed_time < .9 * slumbertime  # Shouldn't even be close.

    # But the call should have registered already:
    assert 1 == len(slowstub.calls)
    check_logcall(slowstub.calls[0])
