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

import contextlib
import sys
import threading
import traceback

the_current_shell = threading.local()
the_current_shell.value = None


@contextlib.contextmanager
def set_current_shell(shell):
    outer = the_current_shell.value
    the_current_shell.value = shell
    try:
        yield
    finally:
        the_current_shell.value = outer


def current_shell():
    assert the_current_shell.value is not None, 'No current shell!'
    return the_current_shell.value


# make sure that the function that is hooked by the shell has the same
# __doc__
class bayesdb_shellhookexp(object):
    def __init__(self, func):
        self.func = func
        fdoc = func.__doc__
        if fdoc is None or len(fdoc.strip()) == 0:
            fdoc = 'NO DOCUMENTATION...\n...\n'

        if len(fdoc.split('\n')) == 1:
            fdoc += '\n...\n'

        self.__doc__ = fdoc

    def __call__(self, *args):
        try:
            return self.func(*args)
        except Exception as err:
            sys.stderr.write(traceback.format_exc())
            print err


def bayesdb_shell_cmd(name, autorehook=False):
    def wrapper(func):
        # because the cmd loop doesn't handle errors and just kicks people out
        current_shell()._hook(name, bayesdb_shellhookexp(func),
                              autorehook=autorehook)
    return wrapper


def bayesdb_shell_init(func):
    func(current_shell())
    return func
