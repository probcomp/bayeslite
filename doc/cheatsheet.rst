BayesDB Cheat Sheet
===================

NOTE: Not all of the details below are part of the BQL language specification,
and are not guaranteed to persist for any period of time.

Useful Tricks
-------------

Listing the column names and statistical types
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This requires accessing the built-in table ``bayesdb_column``::
    
    SELECT tabname, colno, name FROM bayesdb_column
        WHERE tabname=? ORDER BY tabname ASC, colno ASC

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

