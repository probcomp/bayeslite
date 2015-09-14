bayeslite: A probabilistic database built on SQLite 3
=====================================================

Bayeslite is a probabilistic database built on `SQLite 3
<https://www.sqlite.org/>`__.  In addition to SQL queries on
conventional SQL tables, it supports probabilistic BQL queries on
generative models for data in a table.

Quick start for querying a pre-analyzed database::

    import bayeslite
    bdb = bayeslite.bayesdb_open("foo.bdb")
    cursor = bdb.execute("SOME BQL QUERY")
    ...

.. toctree::
   :maxdepth: 2

   api
   bql
   internals

If you would like to analyze your own data with BayesDB, please
contact bayesdb@mit.edu to participate in our research project.

.. toctree::
   :maxdepth: 1

   analysis

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
