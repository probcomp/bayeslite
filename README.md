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

## Documentation

Run

```
$ make doc
```

to build all documentation in build/doc, one directory per output
format, e.g. build/doc/pdf/bayeslite.pdf.
