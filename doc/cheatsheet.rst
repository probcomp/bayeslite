BayesDB Cheat Sheet
===================

NOTE: Not all of the details below are part of the BQL language specification,
and are not guaranteed to persist for any period of time.


The BayesDB World
-----------------

These are definitions for types objects that are used in BayesDB.

-   **data-table**
  
    A table of user data that may contain missing values. The
    user wishes to learn about the process that generated the data, as well as
    unobserved properties of specific data-points.

-   **model**
  
    A hypothesis for the process that generated a **data-table**.
    Associated with every model is a single **model distribution** (see below). A
    model may also possess **model properties** (see below).

-   **model distribution**
  
    A probability distribution over a random data table
    of dimension equal to the size of the **data-table** plus one hypothetical
    next data-point. This distribution must support evaluation of probability
    (densities) and sampling from conditional and marginal distributions.  The
    set of conditioning and marginalization operations that are supported by a
    **model**\'s model distribution limit the set of **model properties** that
    it can possess.
    
-   **model property**
  
    A function of a **model** and an associated **data-table**. There are
    sub-types of model properties:

    -   **data-independent model property**
      
        A model property that is independent of the **data-table** given the
        model. Examples include any function of the marginal distribution over
        the next hypothetical data point, such as the mututal information
        between two variables under this marginal.

    -   **data-dependent model property**
      
        A model property that requires the **data-table** to be evaluated.
        Examples include any function that involves conditioning on observed
        values in the data table, such as the probability of an observed value,
        or any unknown variable that is specific to an observed data-point.
      
    Also known as a **model estimator**.

-   **meta-model**
  
    A set of **models** associated with a **data-table**. Also known
    as a **generator**. The existence of more than one model indicates
    uncertainty about the correct model.

-   **predictive distribution**
  
    The average of a set of **model distributions** induced by a
    **meta-model**. 

-   **predictive property**
  
    A function of a **predictive distribution**.  Examples include the value of
    a missing cell and some measure of confidence associated with such a
    prediction. Also known as a **model prediction**.

-   **estimation**

    The operation of averaging the value of a **model property** across the
    **models** in  a **meta-model**

-   **inference**

    The operation of evaluating a **predictive property** given a
    **meta-model**.

-   **simulation**

    The operation of sampling data from a **model distribution** or from a
    **predictive distribution**.

-   **BQL (Bayesian Query Language)**
  
    An extension of **SQL** that includes functionality for **estimation**,
    **inference**, and **simulation** given a **meta model**.
   
-   **MML (Meta Modeling Language)**
  
    A language for creating **meta-models** for
    **data-tables**. Previously part of **BQL**.


  
Intended Semantics of BQL Keywords
---------------------------------

The logic behind the structure the BQL keywords are discussed below:

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

    -    TODO

-   ``ESTIMATE .. FROM PAIRWISE COLUMNS OF <generator>`` evaluates model
    estimators that are indexed by pairs of variable indices on a collection of
    pairs of variable indices. The fields available to select from are:

    -   TODO


``PREDICT``
^^^^^^^^^^

Evaluate functions of the predictive distribution that is obtained by averaging
over a set of models. Note that this is not necessarily the same as averaging
across models the result of evaluations of a function of the each model, which
is the semantics of 'model estimators' used by ``ESTIMATE``.

One common class of such function are point summaries associated with the
predictive distribution, such as the median or mode. These are the functions
implemented by predict currently.


``INFER``
^^^^^^^^

TODO


``SIMULATE``
^^^^^^^^^^^^

TODO



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

