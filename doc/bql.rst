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

``CREATE [TEMP|TEMPORARY] TABLE [IF NOT EXISTS] <name> FROM <pathname>``

   Create a table named *name* from the csv file at *pathname*. *Pathname* should
   be surrounded by single quotes.

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

   FUTURE: Renaming columns (Github issue #35).

Metamodeling Language (MML)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+++++++++++
Populations
+++++++++++

A population specifies which columns in a table should be modeled or ignored.
For those that are modeled, it specifies their statistical type.

.. index:: ``GUESS SCHEMA``

``GUESS SCHEMA FOR <name>``

   Guess a population schema for the table *name*. The schema maps each column
   in *name* to its heuristically guessed statistical type and the heuristic
   reason for the guess. Columns can be guessed to be ``NOMINAL`` or
   ``NUMERICAL`` or to be ignored (``IGNORE``). The query yields a table created
   as if by the following ``CREATE TABLE``:

   .. code-block:: sql

      CREATE TABLE guessed_stattypes (
         name TEXT NOT NULL UNIQUE,
         stattype TEXT NOT NULL,
         reason TEXT NOT NULL
      )

.. index:: ``CREATE POPULATION``

``CREATE POPULATION [IF NOT EXISTS] FOR <name> WITH SCHEMA (<schema>)``

   Create a population for *name* with schema *schema*. *Schema* can be defined
   using any combination of the following statements, separated by semicolons:

      ``GUESS STATTYPES FOR (<column(s)>)``

         Guess the statistical type for the column(s) *column(s)* by
         heuristically examining the data.

      ``MODEL <column(s)> AS <stattype>``

         Model the column(s) *column(s)* with the statistical type *stattype*.

      ``IGNORE <column(s)>``

         Ignore the column(s) *column(s)*.

.. index:: ``DROP POPULATION``

``DROP POPULATION [IF EXISTS] <population>``

   Drop the population *population* and all its contents.
   Will fail if there are still generators associated with this population.

.. index:: ``ALTER POPULATION``

``ALTER POPULATION <population>``

   Alter the specified properties of the population *population*. The following
   alterations are supported:

   .. index:: ``ADD VARIABLE``

   ``ADD VARIABLE <variable> [<stattype>]``

      Add the variable *variable* to the population *population*. Specify that
      it should be modeled with the statistical type *stattype* (optional);
      otherwise its statistical type will be heuristically guessed.

   .. index:: ``SET STATTYPE``

   ``SET STATTYPE OF <variable(s)> TO <stattype>``

      Change the statistical type of variable(s) *variable(s)* in population
      *population* to *stattype*.

++++++++++
Generators
++++++++++

A generator is a probabilistic model for the variables in a population.

.. index:: ``CREATE GENERATOR``

``CREATE GENERATOR <g> FOR <pop>``

``CREATE GENERATOR <g> FOR <pop> [USING <backend>] (<customization>)``

   Create generator *g* for the population *pop*, optionally specifying which
   *backend* to use (the default is cgpm_backend). The *customization* is a
   comma-separated list of clauses customizing the schema:

      ``OVERRIDE GENERATIVE MODEL FOR <target> [GIVEN <variable(s)>] USING <predictor>``

         Specify that the variable *target* is to be predicted by
         *predictor*, conditional on the input variables
         *variable(s)*.

      ``SUBSAMPLE(<nrows>)``

         Use a randomly chosen subsample of *nrows* rows to train each
         model.

.. index:: ``DROP GENERATOR``

``DROP [[MODEL <num>] | [MODELS <num0>-<num1>] FROM] GENERATOR [IF EXISTS] <g>``

   Drop the generator *g* and all its contents. Optionally drop only
   the model numbered *num* or the models ranging from *num0* to *num1*.

.. index:: ``INITIALIZE MODELS``

``INITIALIZE <num> MODELS [IF NOT EXISTS] FOR <g>``

   Initialize *num* models for the generator *g*. Using ``IF NOT EXISTS`` will
   initialize all models in the range 0 to *num - 1* that do not already exist.

.. index:: ``ANALYZE GENERATOR``

``ANALYZE <g> FOR <duration> [CHECKPOINT <duration>] WAIT``
``ANALYZE <g> FOR <duration> [CHECKPOINT <duration>] (<clauses>)``

   Perform analysis on the models in generator *g*. *Duration* can
   take on values of ``<n> SECOND(S)``, ``<n> MINUTE(S)``, or
   ``<n> ITERATION(S)``.  The ``FOR`` duration specifies how long to perform
   analysis.  The ``CHECKPOINT`` duration specifies how often to commit the
   intermediate results of analysis to the database on disk.  The
   semicolon-separated *clauses* may further configure the analysis:

      ``OPTIMIZED``

          Use the faster analysis for Crosscat-modelled variables
          only.

      ``QUIET``

         Suppress progress bar.

      ``SKIP <variables>``

         Analyze only variables *except* the comma-separated list of
         *variables*.

      ``VARIABLES <variables>``

         Analyze only the comma-separated list of *variables*.

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

``ESTIMATE <columns> BY <population>``

   Like constant ``SELECT``, extended with model estimators of one
   implied row.

.. index:: ``ESTIMATE``

``ESTIMATE [DISTINCT|ALL] <columns> FROM <population> [MODELED BY <g>] [USING [MODEL <num>] [MODELS <num0>-<num1>]] [WHERE <condition>] [GROUP BY <grouping>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Like ``SELECT`` on the table associated with *population*, extended
   with model estimators of one implied row.

.. index:: ``ESTIMATE FROM VARIABLES OF``

``ESTIMATE <columns> FROM VARIABLES OF <population> [MODELED BY <g>] [USING [MODEL <num>] [MODELS <num0>-<num1>]] [WHERE <condition>] [GROUP BY <grouping>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Like ``SELECT`` on the modelled columns of *population*, extended
   with model estimators of one implied column.

.. index:: ``ESTIMATE FROM PAIRWISE VARIABLES OF``

``ESTIMATE <columns> FROM PAIRWISE VARIABLES OF <population> [FOR <subcolumns>] [MODELED BY <g>] [USING [MODEL <num>] [MODELS <num0>-<num1>]] [WHERE <condition>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Like ``SELECT`` on the self-join of the modelled columns of
   *population*, extended with model estimators of two implied columns.

   In addition to a literal list of column names, the list of
   subcolumns may be an ``ESTIMATE * FROM VARIABLES OF`` subquery.

.. index:: ``ESTIMATE, PAIRWISE``

``ESTIMATE <expression> FROM PAIRWISE <population> [MODELED BY <g>] [USING [MODEL <num>] [MODELS <num0>-<num1>]] [WHERE <condition>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Like ``SELECT`` on the self-join of the table assocated with
   *population*, extended with model estimators of two implied rows.

   (Currently the only functions of two implied rows are
   ``SIMILARITY`` and ``SIMILARITY WITH IN THE CONTEXT OF (...)``.)

.. index:: ``INFER``

``INFER <colnames> [WITH CONFIDENCE <conf>] FROM <population> [MODELED BY <g>] [USING [MODEL <num>] [MODELS <num0>-<num1>]] [WHERE <condition>] [GROUP BY <grouping>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Select the specified *colnames* from *population*, filling in
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

``INFER EXPLICIT <columns> FROM <population> [MODELED BY <g>] [USING [MODEL <num>] [MODELS <num0>-<num1>]] [WHERE <condition>] [GROUP BY <grouping>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Like ``SELECT`` on the table associated with *population*, extended
   with model estimators of one implied row and with model predictions.

   In addition to normal ``SELECT`` columns, *columns* may include
   columns of the form

      ``PREDICT <name> [AS <rename>] CONFIDENCE <confname>``

   This results in two resulting columns, one named *rename*, or
   *name* if *rename* is not supplied, holding a predicted value of
   the column *name*, and one named *confname* holding the confidence
   of the prediction.

.. index:: ``SIMULATE``

``SIMULATE <colnames> FROM <population> [MODELED BY <g>] [USING [MODEL <num>] [MODELS <num0>-<num1>]] [GIVEN <constraints>] [LIMIT <limit>]``

   Select the requested *colnames* from rows sampled from *population*.
   *Constraints* is a comma-separated list of constraints of the form

      ``<colname> = <expression>``

   representing equations that the returned rows satisfy.

   The number of rows in the result will be *limit*.

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

    .. code-block:: sql

        ESTIMATE
            MUTUAL INFORMATION AS mutinf
        FROM PAIRWISE VARIABLES OF p
        ORDER BY mutinf

has the effect of estimating mutual information twice for each row because it is
mentioned twice, once in the output and once in the ORDER BY, which is twice as
slow as it needs to be.   (Actually, approximately four times, because mutual
information is symmetric, but that is an orthogonal issue.)

To avoid this double evaluation, you can order the results of a
subquery instead:

    .. code-block:: sql

        SELECT *
        FROM (
            ESTIMATE MUTUAL INFORMATION AS mutinf
            FROM PAIRWISE VARIABLES OF p
        )
        ORDER BY mutinf

.. index:: ``PREDICTIVE PROBABILITY``

``PREDICTIVE PROBABILITY OF <column> [GIVEN (<column(s)>)]``

   Function of one implied row.  Returns the predictive probability of
   the row's value for the column named *column*, optionally given the
   data in *column(s)* in the row.

.. index:: ``PROBABILITY DENSITY OF``

``PROBABILITY DENSITY OF <column> = <value> [GIVEN (<constraints>)]``

``PROBABILITY DENSITY OF (<targets>) [GIVEN (<constraints>)]``

   Constant.  Returns the probability density of the value of the BQL
   expression *value* for the column *column*.  If *targets* is
   specified instead, it is a comma-separated list of
   ``<column> = <value>`` terms, and the result is the joint density
   for all the specified target column values.

   If *constraints* is specified, it is also a comma-separated list of
   ``<column> = <value>`` terms, and the result is the conditional
   joint density given the specified constraint column values.

   WARNING: The value this function returns is not a normalized probability in
   [0, 1], but rather a probability density with a normalization
   constant that is common to the column but may vary between columns.
   So it may take on values above 1.

``PROBABILITY DENSITY OF VALUE <value> [GIVEN (<constraints>)]``

   Function of one implied column.  Returns the probability density of
   the value of the BQL expression *value* for the implied column.  If
   *constraints* is specified, it is a comma-separated list of
   ``<column> = <value>`` terms, and the result is the conditional
   density given the specified constraint column values.

.. index:: ``SIMILARITY``

``SIMILARITY [OF (<expression0>)] [TO (<expression1>)] IN THE CONTEXT OF <column>``

   Constant, or function of one or two implied rows. If given both ``OF`` and
   ``TO``, returns a constant measure of similarity between the first row
   satisfied by *expression0* and the first row satisfied by *expression1*. If
   given only  ``TO`` returns a measure of the similarity of the implied row
   with the first row satisfying *expression1*. Otherwise, returns a measure of
   the similarity of the two implied rows.  The similarity may be
   considered within the context of a column.

.. index:: ``PREDICTIVE RELEVANCE``

``PREDICTIVE RELEVANCE [OF (<expression0>)] TO EXISTING ROWS (<expression1>) IN THE CONTEXT OF <column>``

``PREDICTIVE RELEVANCE [OF (<expression0>)] TO HYPOTHETICAL ROWS (<expression1>) IN THE CONTEXT OF <column>``

``PREDICTIVE RELEVANCE [OF (<expression0>)] TO EXISTING ROWS (<expression1>) AND HYPOTHETICAL ROWS (<expression2>) IN THE CONTEXT OF <column>``

   If given ``OF``, returns a measure of predictive relevance of the first row
   satisfying *expression0* for the existing and/or hypothetical rows satisfying
   *expression1* (and *expression2* in the case of both) in the context of
   *column*. Otherwise, returns a measure of predictive relevance of all rows to
   the specified existing and/or hypothetical rows.

.. index:: ``CORRELATION``

``CORRELATION [[OF <column1>] WITH <column2>]``

   Constant, or function of one or two implied columns.  Returns
   standard measures of correlation between columns:

   * Pearson correlation coefficient squared for two numerical columns.
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

   If ``USING <n> SAMPLES`` is specified and the underlying generator
   uses Monte Carlo integration for each model to estimate the mutual
   information (beyond merely the integral averaging all generators), the
   integration is performed using *n* samples for each model.

Model Predictions
^^^^^^^^^^^^^^^^^

.. index:: ``PREDICT``

``PREDICT <column> [WITH CONFIDENCE <confidence>]``

   Function of one implied row.  Samples a value for the column named
   *column* from the model given the other values in the row, and
   returns it if the confidence of the prediction is at least the
   value of the BQL expression *confidence*; otherwise returns null.
