# Bayeslite

Bayeslite is a BQL database built on SQLite3.  BQL is an extension to
SQL that supports queries about the probable implications of data.

## Dependencies

Bayeslite is written in Python 2.7, using the sqlite3 module with
SQLite3 >=3.7.17.

Bayeslite is most useful in conjunction with
[Crosscat](https://github.com/mit-probabilistic-computing-project/crosscat),
which provides a general-purpose metamodel.

Running the automatic tests requires [pytest](http://pytest.org/) and
Crosscat.

Building the documentation requires [Sphinx](http://sphinx-doc.org/).

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

To build the documentation (requires sphinx):

```
$ make doc
```

The result will be placed in `build/doc`, with one subdirectory per
output format.

To build only one output format, e.g. HTML because you don't want to
install TeX:

```
$ make html
```
