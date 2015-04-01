# Bayeslite

Bayeslite is a prototype reimplementation of BayesDB on SQLite3, to
enable the use of relational SQL queries on databases in addition to
probabilistic BQL queries.

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
