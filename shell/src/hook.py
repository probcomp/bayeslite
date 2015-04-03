import threading
import contextlib

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
        self.__doc__ = func.__doc__

    def __call__(self, *args):
        try:
            return self.func(*args)
        except Exception as err:
            print err


def bayesdb_shell_cmd(name, autorehook=False):
    def wrapper(func):
        # because the cmd loop doesn't handle errors and just kicks people out
        current_shell()._hook(name, bayesdb_shellhookexp(func), autorehook=autorehook)
    return wrapper
