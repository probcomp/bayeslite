BQL: Bayesian Query Language
============================

BQL is a probabilistic extension to SQL that supports

* standard deterministic SQL queries,
* estimating probabilities and strengths of relationships between rows
  and columns,
* inferring missing values within confidence intervals, and
* simulating data from generative models.

BQL does not currently include any of the SQL Data Modification
Language (DML) -- ``INSERT``, ``UPDATE``, and other commands to modify
the contents of tables -- and includes only limited subset of the SQL
Data Definition Language (DDL) -- ``CREATE TABLE`` and commands to
modify the database schema.

A BQL phrase is either a command or a query.  In contexts admitting
multiple BQL phrases, e.g. at the bayeslite shell, each phrase must be
terminated by a semicolon before the next one begins.

Expressions in BQL are like SQL, and may involve standard arithmetic,
SQL functions such as ``IFNULL``.  Expressions in BQL may additionally
involve model functions such as ``PREDICTIVE PROBABILITY OF <column>``
in appropriate contexts.

In the following syntax description, square brackets denote optional
terms.  For example, the pattern

   ``DEPENDENCE PROBABILITY [[OF <column1>] WITH <column2>]``

allows

* ``DEPENDENCE PROBABILITY``
* ``DEPENDENCE PROBABILITY WITH quagga``
* ``DEPENDENCE PROBABILITY OF eland WITH quagga``

but not ``DEPENDENCE PROBABILITY OF eland``.

BQL Queries
-----------

.. index:: ``SELECT``

``SELECT <columns>``

   Standard SQL constant ``SELECT``: yield a single row by evaluating
   the specified columns.

``SELECT [DISTINCT|ALL] <columns> FROM <table> [WHERE <condition>] [GROUP BY <grouping>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Standard SQL ``SELECT``.  Model estimators are not allowed, except
   in subqueries of types that allow them.

   ``<columns>``
      Comma-separated list of BQL expressions, each with an optional
      ``AS <name>`` to name the column in the resulting table.

   ``FROM <table>``
      *Table* is a comma-separated list of table names or subqueries,
      each with an optional ``AS <name>`` to qualify the table name in
      references to its columns.  When multiple tables are specified
      separated by commas, their join (cartesian product) is selected
      from.

      FUTURE: All SQL joins will be supported.

   ``WHERE <condition>``
      *Condition* is a BQL expression selecting a subset of the input
      rows from *table* for which output rows will be computed.

   ``GROUP BY <grouping>``
      *Grouping* is a BQL expression specifying a key on which to
      group output rows.  May be the name of an output column with
      ``AS <name>`` in *columns*.

   ``ORDER BY *expression* [ASC|DESC]``
      *Expression* is a BQL expression specifying a key by which to
      order output rows, after grouping if any.  Rows are yielded in
      ascending order of the key by default or if ``ASC`` is
      specified, or in descending order of the key if ``DESC`` is
      specified.

   ``LIMIT <n> [OFFSET <offset>]`` or ``LIMIT <offset>, <n>``
      *N* and *offset* are BQL expressions.  Only up to *n*
      (inclusive) rows are returned after grouping and ordering,
      starting at *offset* from the beginning.

.. index:: ``ESTIMATE BY``

``ESTIMATE <columns> BY <generator> [USING MODEL <modelno>]``

   Like constant ``SELECT``, extended with model estimators of one
   implied row.

   ``USING MODEL <modelno>``
      *Modelno* is a BQL expression specifying the number of the model
      of *generator* to use in model estimators.  Values of model
      estimators are averaged over all models if ``USING MODEL`` is
      not specified.  Models are zero-indexed.

.. index:: ``ESTIMATE``

``ESTIMATE [DISTINCT|ALL] <columns> FROM <generator> [USING MODEL <modelno>] [WHERE <condition>] [GROUP BY <grouping>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Like ``SELECT`` on the table associated with *generator*, extended
   with model estimators of one implied row.

   ``USING MODEL <modelno>``
      *Modelno* is a BQL expression specifying the number of the model
      of *generator* to use in model estimators.  Values of model
      estimators are averaged over all models if ``USING MODEL`` is
      not specified.  Models are zero-indexed.

.. index:: ``ESTIMATE FROM COLUMNS OF``

``ESTIMATE <columns> FROM COLUMNS OF <generator> [USING MODEL <modelno>] [WHERE <condition>] [GROUP BY <grouping>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Like ``SELECT`` on the modelled columns of *generator*, extended
   with model estimators of one implied column.

.. index:: ``ESTIMATE FROM PAIRWISE COLUMNS OF``

``ESTIMATE <columns> FROM PAIRWISE COLUMNS OF <generator> [FOR <subcolumns>] [USING MODEL <modelno>] [WHERE <condition>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Like ``SELECT`` on the self-join of the modelled columns of
   *generator*, extended with model estimators of two implied columns.

   In addition to a literal list of column names, the list of
   subcolumns may be an ``ESTIMATE * FROM COLUMNS OF`` subquery.

.. index:: ``ESTIMATE, PAIRWISE``

``ESTIMATE <expression> FROM PAIRWISE <generator> [USING MODEL <modelno>] [WHERE <condition>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Like ``SELECT`` on the self-join of the table assocated with
   *generator*, extended with model estimators of two implied rows.

   (Currently the only functions of two implied rows are
   ``SIMILARITY`` and ``SIMILARITY WITH RESPECT TO (...)``.)

.. index:: ``INFER``

``INFER <colnames> [WITH CONFIDENCE <conf>] FROM <generator> [USING MODEL <modelno>] [WHERE <condition>] [GROUP BY <grouping>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Select the specified *colnames* from *generator*, filling in
   missing values if they can be filled in with confidence at least
   *conf*, a BQL expression.  Only missing values *colnames* will be
   filled in; missing values in columns named in *condition*,
   *grouping*, and *ordering* will not be.  Model estimators and model
   predictions are allowed in the expressions.

   *Colnames* is a comma-separated list of column names, **not**
   arbitrary BQL expressions.

   FUTURE: *Colnames* will be allowed to have arbitrary expressions,
   with any references to columns inside automatically filled in if
   missing.

.. index:: ``INFER EXPLICIT``

``INFER EXPLICIT <columns> FROM <generator> [USING MODEL <modelno>] [WHERE <condition>] [GROUP BY <grouping>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Like ``SELECT`` on the table associated with *generator*, extended
   with model estimators of one implied row and with model predictions.

   In addition to normal ``SELECT`` columns, *columns* may include
   columns of the form

      ``PREDICT <name> [AS <rename>] CONFIDENCE <confname>``

   This results in two resulting columns, one named *rename*, or
   *name* if *rename* is not supplied, holding a predicted value of
   the column *name*, and one named *confname* holding the confidence
   of the prediction.

.. index:: ``SIMULATE``

``SIMULATE <colnames> FROM <generator> [USING MODEL <modelno>] [GIVEN <constraints>] [LIMIT <limit>]``

   Select the requested *colnames* from rows sampled from *generator*.
   *Constraints* is a comma-separated list of constraints of the form

      ``<colname> = <expression>``

   representing equations that the returned rows satisfy.

   The number of rows in the result will be *limit*.

   Each row is drawn from a single model, but if ``USING MODEL`` is
   not specified, different rows may be drawn from different models.

BQL Expressions
---------------

BQL expressions, like SQL expressions, may name columns, include query
parameters, use standard arithmetic operators, and use SQL functions
such as ``ABS(<x>)``, as documented in the `SQLite3 Manual`_.

.. _SQLite3 Manual: https://www.sqlite.org/lang.html

In addition, BQL expressions in ``ESTIMATE`` and ``INFER`` queries may
use model estimators, and BQL expressions in ``INFER`` queries may use
model predictions.

Model Estimators
^^^^^^^^^^^^^^^^

Model estimators are functions of a model, up to two columns, and up
to one row.

WARNING: Due to limitations in the sqlite3 query engine that bayeslite
relies on (Github issue #308), repeated references to a model
estimator may be repeatedly evaluated for each row, even if they are
being stored in the output of queries.  For example,

    ESTIMATE MUTUAL INFORMATION AS mutinf
        FROM PAIRWISE COLUMNS OF t ORDER BY mutinf

has the effect of estimating mutual information twice for each row
because it is mentioned twice, once in the output and once in the
ORDER BY, which is twice as slow as it needs to be.  (Actually,
approximately four times, because mutual information is symmetric, but
that is an orthogonal issue.)

To avoid this double evaluation, you can order the results of a
subquery instead:

    SELECT *
        FROM (ESTIMATE MUTUAL INFORMATION AS mutinf
                FROM PAIRWISE COLUMNS OF t)
        ORDER BY mutinf

.. index:: ``PREDICTIVE PROBABILITY``

``PREDICTIVE PROBABILITY OF <column>``

   Function of one implied row.  Returns the predictive probability of
   the row's value for the column named *column*, given all the other
   data in the row.

.. index:: ``PROBABILITY OF``

``PROBABILITY OF <column> = <value> [GIVEN (<constraints>)]``
``PROBABILITY OF (<targets>) [GIVEN (<constraints>)]``

   Constant.  Returns the probability density of the value of the BQL
   expression *value* for the column *column*.  If *targets* is
   specified instead, it is a comma-separated list of
   ``<column> = <value>`` terms, and the result is the joint density
   for all the specified target column values.

   If *constraints* is specified, it is also a comma-separated list of
   ``<column> = <value>`` terms, and the result is the conditional
   joint density given the specified constraint column values.

   WARNING: The value this function is not a normalized probability in
   [0, 1], but rather a probability density with a normalization
   constant that is common to the column but may vary between columns.
   So it may take on values above 1.

``PROBABILITY OF VALUE <value> [GIVEN (<constraints>)]``

   Function of one implied column.  Returns the probability density of
   the value of the BQL expression *value* for the implied column.  If
   *constraints* is specified, it is a comma-separated list of
   ``<column> = <value>`` terms, and the result is the conditional
   density given the specified constraint column values.

.. index:: ``SIMILARITY``

``SIMILARITY [TO (<expression>)] [WITH RESPECT TO (<columns>)]``

   Function of one or two implied rows.  If given ``TO``, returns a
   measure of the similarity of the implied row with the first row
   satisfying <expression>.  Otherwise, returns a measure of the
   similarity of the two implied rows.  The similarity may be
   considered with respect to a subset of columns.

   *Columns* is a comma-separated list of column names or
   ``ESTIMATE * FROM COLUMNS OF ...`` subqueries.

.. index:: ``CORRELATION``

``CORRELATION [[OF <column1>] WITH <column2>]``

   Constant, or function of one or two implied columns.  Returns
   standard measures of correlation between columns:

   * Pearson correlation coefficient for two numerical columns.
   * Cramer's phi for two categorical columns.
   * ANOVA R^2 for a categorical column and a numerical column.

   Cyclic columns are not supported.

.. index:: ``DEPENDENCE PROBABILITY``

``DEPENDENCE PROBABILITY [[OF <column1>] WITH <column2>]``

   Constant, or function of one or two implied columns.  Returns the
   probability (density) that the two columns are dependent.

.. index:: ``MUTUAL INFORMATION``

``MUTUAL INFORMATION [[OF <column1>] WITH <column2>] [USING <n> SAMPLES]``

   Constant, or function of one or two implied columns.  Returns the
   strength of dependence between the two columns, in units of bits.

   If ``USING <n> SAMPLES`` is specified and the underlying metamodel
   uses Monte Carlo integration for each model to estimate the mutual
   information (beyond merely the integral averaging all models), the
   integration is performed using *n* samples for each model.

Model Predictions
^^^^^^^^^^^^^^^^^

.. index:: ``PREDICT``

``PREDICT <column> [WITH CONFIDENCE <confidence>]``

   Function of one implied row.  Samples a value for the column named
   *column* from the model given the other values in the row, and
   returns it if the confidence of the prediction is at least the
   value of the BQL expression *confidence*; otherwise returns null.

BQL Commands
------------

BQL commands change the state of the database.

Transactions
^^^^^^^^^^^^

Transactions are groups of changes to a database that happen all at
once or not at all.  Transactions do not nest.

FUTURE: BQL will additionally support savepoints (Github issue #36),
which are like transactions but may be named and nested.

.. index:: ``BEGIN``

``BEGIN``

   Begin a transaction.  Subsequent commands take effect within the
   transaction, but will not be made permanent until ``COMMIT``, and
   may be undone with ``ROLLBACK``.

.. index:: ``COMMIT``

``COMMIT``

   End a transaction, and commit to all changes made since the last
   ``BEGIN``.

.. index:: ``ROLLBACK``

``ROLLBACK``

   End a transaction, and discard all changes made since the last
   ``BEGIN``.

Data Definition Language
^^^^^^^^^^^^^^^^^^^^^^^^

The BQL DDL is currently limited to creating tables from the results
of queries, and dropping and renaming tables.

FUTURE: The complete SQL DDL supported by sqlite3 will be supported by
BQL (Github issue #37).  Until then, one can always fall back to
executing SQL instead of BQL in Bayeslite.

.. index:: ``CREATE TABLE``

``CREATE [TEMP|TEMPORARY] TABLE [IF NOT EXISTS] <name> AS <query>``

   Create a table named *name* to hold the results of the query
   *query*.

.. index:: ``DROP TABLE``

``DROP TABLE [IF EXISTS] <name>``

   Drop the table *name* and all its contents.

   May fail if there are foreign key constraints that refer to this
   table.

.. index:: ``ALTER TABLE``

``ALTER TABLE <name> <alterations>``

   Alter the specified properties of the table *name*.  *Alterations*
   is a comma-separated list of alterations.  The following
   alterations are supported:

   .. index:: ``RENAME TO``

   ``RENAME TO <newname>``

      Change the table's name to *newname*.  Foreign key constraints
      are updated; triggers and views are not, and must be dropped
      and recreated separately, due to limitations in sqlite3.

   .. index:: ``SET DEFAULT GENERATOR``

   ``SET DEFAULT GENERATOR TO <generator>``

      Set the default generator of the table to be *generator*.

   .. index:: ``UNSET DEFAULT GENERATOR``

   ``UNSET DEFAULT GENERATOR``

      Remove any default generator associated with the table.

   FUTURE: Renaming columns (Github issue #35).
