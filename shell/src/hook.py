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


def bayesdb_shell_cmd(name, autorehook=False):
    def wrapper(func):
        # because the cmd loop doesn't handle errors and just kicks people out
        def excepection_handling_func(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as err:
                print err

        current_shell()._hook(name, excepection_handling_func, autorehook=autorehook)
    return wrapper
