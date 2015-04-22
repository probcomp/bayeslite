# The BayesDB shell

Access the bayeslite shell with the `bayeslite` command. Type `.help`
in the shell to see a list of commands.

## Arguments
- `--no-init-file`: do not source `~/bayeslite.rc`
- `-f <path>`: source a file of commands
    + Ex: `$ bayeslite mydatabase.bdb -f hooks/myhooks.bql` 


## Dot command reference
By default, the bayeslite shell will interpret commands as bql. Commands that
lead with a dot (*dot commands*; e.g., `.sql pragma table_info(mytable)`
perform special functionality.

### `.help`
The only command you'll need.

    bayeslite> .help

## Example

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

## Adding your own commands with `.hook`

```python
# my_contrib.py
from bayeslite.shell import pretty
from bayeslite.shell.hook import bayesdb_shell_cmd

@bayesdb_shell_cmd("hello")
def say_hello_to_name(self, args):
    """ Says hello """
    self.stdout.write("Hello, %s.\n" % (args,))

@bayesdb_shell_cmd("byebye", autorehook=True)
def say_hello_to_name(self, args):
    """ Says bye-bye """
    self.stdout.write("Bye-bye.\n")

# Alias a long query you use a lot 
@bayesdb_shell_cmd("mycmd", autorehook=True):
def get_cust_order_data_name(self, args):
    '''Get order id, order date, and cutomer name, by customer name 
    <customer_name>

    Example:
    bayeslite> .mycmd John Keats 
    '''
    query = '''
    SELECT Orders.OrderID, Orders.OrderDate, Customers.CustomerName
        FROM Customers, Orders
        WHERE Customers.CustomerName = ? 
            AND Customers.CustomerID = Orders.CustomerID;
    '''
    cursor = self._bdb.execute(query, (args,))
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

You are free to `.hook` a file multiple times. Re-hooking a file will reload the contents of the file. This can be especially useful for development. If you try to re-hook a file, you must confirm that you want to re-hook the file and confirm that you want to re-hook each function in that file for which `autorehook=False`.

## The `.bayesliterc`
Manually hooking the utilities you frequently use every time you open the shell is annoying. To address this, the BayesLite shell looks for a `.bayesliterc` file in your home directory, which it runs on startup. Any file or path names in `.bayesliterc` should be absolute (this is subject to change, to allow paths relative to the rc file). Local, project-specific init files can be used using the `-f` option. 

For example, we may have a small set of utilities in our `~/.bayesliterc`:

```
-- contents of ~/.bayesliterc
.hook /User/bax/my_bayesdb_utils/plotting.py
.hook /User/bax/my_bayesdb_utils/cleaning.py
```

You can prevent the shell from loading `~/.bayesliterc` with the `--no-init-file` argument.

