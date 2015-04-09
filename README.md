# Bayeslite

Bayeslite is a BQL database built on SQLite3.  BQL is an extension to
SQL that supports queries about the probable implications of data.

## Install

After you have installed
[CrossCat](https://github.com/mit-probabilistic-computing-project/crosscat):

To check that everything is working:

```
$ ./check.sh
```

To install system-wide, or into the current virtual environment:

```
$ python setup.py build
$ python setup.py install
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
from bayeslite.shell import pretty
from bayeslite.shell.hook import bayelite_shell_cmd

@bayeslite_shell_cmd("hello")
def say_hello_to_name(self, args):
    """ Says hello """
    self.stdout.write("Hello, %s.\n" % (args,))

@bayelite_shell_cmd("byebye", autorehook=True)
def say_hello_to_name(self, args):
    """ Says bye-bye """
    self.stdout.write("Bye-bye.\n")

# Alias a long query you use a lot 
@bayelite_shell_cmd("mycmd", autorehook=True):
def get_cust_order_data_name(self, args):
    '''Get order id, order date, and cutomer name, by customer name 
    <customer_name>

    Example:
    bayeslite> .mycmd John Keats 
    '''
    query = '''
    SELECT Orders.OrderID, Orders.OrderDate, Customers.CustomerName
        FROM Customers, Orders
        WHERE Customers.CustomerName="{}" 
            AND Customers.CustomerID=Orders.CustomerID;
    '''.format(args)
    cursor = self.bql.execute_phrase(self._bdb, query)
    pretty.pp_cursor(self.stdout, cursor)

```

From the shell, access your command with `.hook`
```
bayeslite> .hook my_contrib.py
added command ".hello"
added command ".byebye"
added command ".mycmd"
bayeslite> .help hello
.hello Says hello
bayeslite> .hello Gary Oldman
Hello, Gary Oldman.
```

You are free to `.hook` a file multiple times. Re-hooking a file will reload the conents of the file. This can be especially useful for development. If you try to re-hook a file, you must confirm that you want to re-hook the file and confirm that you want to re-hook each function in that file for which `autorehook=False`.

#### the .bayesliterc
Manually hooking the utilities you frequently use every time you open the shell is annoying. To address this, the BayesLite shell looks for a `.bayesliterc` file in your home directory, which it runs on startup. Any file or path names in `.bayesliterc` should be absolute (this is subject to change, to allow paths relative to the rc file). Local, project-specific init files can be used using the `-f` option. 

For example, we may have a small set of utilities in our `~/.bayesliterc`:

```
-- contents of ~/.bayesliterc
.hook /User/bax/my_bayesdb_utils/plotting.py
.hook /User/bax/my_bayesdb_utils/cleaning.py
```

You can prevent the shell from loading `~/.bayesliterc` with the `--no-init-file` argument.


