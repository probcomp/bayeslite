BayesDB Cheat Sheet
============================

NOTE: Not all of the details below are part of the BQL language specification,
and are not guaranteed to persist for any period of time.

Intended Semantics of BQL Keywords
---------------------------------

The logic behind the structure the top-level BQL keywords are discussed below:

``ESTIMATE``
^^^^^^^^^^^
Estimate the expectation of a function (the `model estimator`) that takes a
model as input, by averaging across some set of models.

Model estimators are frequently indexed by a specific datapoint indices (row)
or variable indices (column), or a combination of datapoints and/or columns.
For example:

-   ``MUTUAL INFORMATION`` is a model estimator that is indexed by two variables
    indices (columns): it returns the mutual information between those two
    variables under a specific model.

-   ``MUTUAL INFORMATION WITH <column2>`` is a model estimator that is indexed by
    a single variable index (column), because the other variable index is fixed
    (column2)

-   ``MUTUAL INFORMATION OF <column1> WITH <column2>`` is a model estimator that
    is not indexed.

-   ``SIMILARITY`` is a model estimator that is indexed by two datapoint indices.

-   ``SIMILARITY TO <expression>`` is a model estimator that is indexed by a
    single datapoint index.

-   ``PREDICTIVE PROBABILITY OF <column>`` is a model estimator that is indexed
    by a datapoint index.

-   ``PROBABILITY OF <column> = <value>`` is a model estimator that is indexed by
    a datapoint index.

The four flavors of ``ESTIMATE`` evaluate model estimators with a particular index
type to a collection of indices of the appropritae type. Specifically:

-   ``ESTIMATE .. FROM <generator>`` evaluates model estimators that are indexed
    by a single datapoint index to a collection of datapoint indices and
    returns a collection of model estimator results indexed by a datapoint
    index. The fields available to select from are:

    -   All columns from the data table.
    -   ``rowid``: The index of the datapoint in the data table.
    -   All model estimators that are indexed by a single datapoint index.
        Currently this is only ``PREDICTIVE PROBABILITY OF <column>``

    Note that ``ESTIMATE * FROM <generator>`` returns only the original fields
    from the data table, and not the other fields mentioned above.

-   ``ESTIMATE .. FROM PAIRWISE <generator>`` evaluates model estimators that are indexed
    by a two datapoint indices to a collection of pairs of datapoint indices and
    returns a collection of model estimator results indexed by pairs of
    datapoint indices. The fields available to select from are:

    -   ``rowid0``: The first datapoint index in the pair.
    -   ``rowid1``: The second datapoint index in the pair.
    -   All model estimators that are indexed by two datapoint indices.
        Currently this is only ``SIMILARITY``.

    Note that ``ESTIMATE * FROM PAIRWISE <generator>`` is not currently
    supported although ``ESTIMATE *, SIMILARITY FROM PAIRWISE <generator>`` is
    (`Issue #262 <https://github.com/probcomp/bayeslite/issues/262>`_)

-   ``ESTIMATE .. FROM COLUMNS OF <generator>`` evaluates model estimators
    that are indexed by a single variable index on a collection of variable
    indices. The fields available to select from are:

    -    foo

-   ``ESTIMATE .. FROM PAIRWISE COLUMNS OF <generator>`` evaluates model
    estimators that are indexed by pairs of variable indices on a collection of
    pairs of variable indices. The fields available to select from are:

    -   bar


``INFER``
^^^^^^^^


``SIMULATE``
^^^^^^^^^^^^
alskjdf


Using BQL Keywords
-----------------




Useful Tricks
-------------

Sorting the output of ``ESTIMATE`` queries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Currently, queries of the form ``ESTIMATE .. ORDER BY ..`` are slow, because
evaluation of the expressions being estimated occurs more often than necessary.
A temporary workaround is to create a temporary table to store the unordered
output of ``ESTIMATE``, and then sort the table, and drop it.  For example,
instead of:::

    ESTIMATE Purpose, PREDICTIVE PROBABILITY OF Purpose AS pp FROM satellites_cc ORDER BY pp

which will be slow, use the following workaround:::

    CREATE TEMP TABLE tt AS ESTIMATE Purpose, PREDICTIVE PROBABILITY OF Purpose AS pp FROM satellites_cc
    SELECT * FROM tt ORDER BY pp
    DROP TABLE tt

Returning column names from ``ESTIMATE .. FROM COLUMNS OF ..``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The command ``ESTIMATE .. FROM COLUMNS OF ..`` returns the values of the model
estimators evaluated on each column. To also obtain the column names, use
``*, [..]``. For example, instead of::

    ESTIMATE DEPENDENCE PROBABILITY WITH Purpose as dp FROM COLUMNS OF satellites_cc

which only contains a single field with the dependence probabilities, instead
use::

    ESTIMATE *, DEPENDENCE PROBABILITY WITH Purpose as dp FROM COLUMNS OF satellites_cc

Note that the field `name` contains the column name. However, it is a bug that
you can't say ``ESTIMATE name FROM COLUMNS OF ..``


See `Issue #260 <https://github.com/probcomp/bayeslite/issues/260>`_.

