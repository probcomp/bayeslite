# Bayeslite

Bayeslite is a BQL database built on SQLite3.  BQL is an extension to
SQL that supports queries about the probable implications of data.

## Dependencies

Bayeslite is written in Python 2.7, using the sqlite3 module with
SQLite3 >=3.7.17, and does not itself depend on any external software
beyond that.

Bayeslite is most useful in conjunction with
[Crosscat](https://github.com/mit-probabilistic-computing-project/crosscat),
which provides a general-purpose metamodel.

Running the automatic tests requires [pytest](http://pytest.org/) and
Crosscat.

## Test

To check that everything is working (requires pytest and Crosscat):

```
$ ./check.sh
```

## Install

To install system-wide, or into the current virtual environment:

```
$ python setup.py build
$ python setup.py install
```

## Use

Import the `bayeslite` module.  See the documentation for details on
the Python API.

## Run

To enter the interactive bayeslite shell, storing data in `foo.bdb`:

```
$ bayeslite foo.bdb
```

## Documentation

Run

```
$ make doc
```

to build all documentation in build/doc, one directory per output
format, e.g. build/doc/pdf/bayeslite.pdf.
