# Bayeslite

Bayeslite is a prototype reimplementation of BayesDB on SQLite3, to
enable the use of relational SQL queries on databases in addition to
probabilistic BQL queries.

## Install

After you have installed
[CrossCat](https://github.com/mit-probabilistic-computing-project/crosscat):

```
$ python setup.py build
$ python setup.py install
```

To check that everything is working:

```
$ ./check.sh
```

Bayeslite is tested on Ubuntu 14.04.  It should also run on other
operating systems with sqlite3 >= 3.7.17, but we don't regularly test
them.

## Use
*More complete documentation coming soon*
### Shell

Access the bayeslite shell with the `bayeslite` command. Type `.help`
in the shell to see a list of commands.

#### Example

```
$ bayeslite my_database.bdb
Welcome to the bayeslite shell.
Type `.help' for help.
bayeslite> .csv mytable from myfile.csv
bayeslite> .guess mytable_cc mytable.csv
bayeslite> INITIALIZE 10 MODELS FOR mytable_cc;
bayeslite> ANALYZE mytable_cc FOR 100 ITERATIONS WAIT;
bayeslite> .hook contrib.py
added command ".zmatrix"
added command ".pairplot"
added command ".ccstate"
bayeslite> .zmatrix ESTIMATE PAIRWISE DEPENDENCE PROBABILITY FROM mytable_cc -f zmat.png
```

#### Adding your own commands with `.hook`

```python
# my_contrib.py
from bayeslite.shell.hook import bayelite_shell_cmd

@bayeslite_shell_cmd("hello")
def say_hello_to_name(self, args):
    """ Says hello """
    self.stdout.write("Hello, %s.\n" % (args,))


@bayelite_shell_cmd("byebye", autorehook=True)
def say_hello_to_name(self, args):
    """ Says bye-bye """
    self.stdout.write("Bye-bye.\n")
```

From the shell, access your command with `.hook`
```
bayeslite> .hook my_contrib.py
added command ".hello"
added command ".byebye"
bayeslite .help hello
.hello Says hello
bayeslite> .hello Gary Oldman
Hello, Gary Oldman.
```

You are free to `.hook` a file multiple times. Re-hooking a file will reload the conents of the file. This can be especially useful for development. If you try to re-hook a file, you must confirm that you want to re-hook the file and confirm that you want to re-hook each function in that file for which `autorehook=False`. 
