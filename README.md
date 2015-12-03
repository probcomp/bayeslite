# Bayeslite

Bayeslite is a BQL database built on SQLite3.  BQL is an extension to
SQL that supports queries about the probable implications of data.

Bayeslite is part of an ongoing research project.  For more
information, contact bayesdb@mit.edu.

## Dependencies

Bayeslite is written in Python 2.7, using the sqlite3 module with
SQLite3 >=3.8.0.

Bayeslite depends on:

- [Crosscat](https://github.com/probcomp/crosscat),
  a general-purpose nonparametric Bayesian population model which
  serves as a default in the absence of a domain-specific model.
- [apsw](https://rogerbinns.github.io/apsw/), a sqlite3 wrapper for
  Python more articulated than the builtin sqlite3 module
- [numpy](http://www.numpy.org), a library of vectorized numerical
  algorithms
- [requests](http://www.python-requests.org/), an HTTP/HTTPS library,
  which we use to track users.

The bayeslite automatic tests depend on:

- [pytest](https://pytest.org/)
- [pandas](http://pandas.pydata.org), a library for tabular data
  structures

The bayeslite documentation depends on:

- [Sphinx](http://sphinx-doc.org/)

## Test

To check that everything will work, before installing:

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
