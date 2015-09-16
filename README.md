# Bayeslite

Bayeslite is a BQL database built on SQLite3.  BQL is an extension to
SQL that supports queries about the probable implications of data.

## Dependencies

Bayeslite is written in Python 2.7, using the sqlite3 module with
SQLite3 >=3.7.17.

Bayeslite depends on:

- [Crosscat](https://github.com/mit-probabilistic-computing-project/crosscat),
  a general-purpose generative model for populations.
- [requests](http://www.python-requests.org/), an HTTP/HTTPS library,
  which we use to track users.

The bayeslite documentation depends on:

- [Sphinx](http://sphinx-doc.org/)

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
