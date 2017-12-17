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

The complete SQL DDL supported by sqlite3 is not supported by BQL.  Note that
one can always fall back to executing SQL instead of BQL in Bayeslite.

.. index:: ``CREATE TABLE``

``CREATE [TEMP|TEMPORARY] TABLE [IF NOT EXISTS] <name> FROM '<pathname>'``

   Create a table named *name* from the csv file at *pathname*. Note that
   *pathname* is a string, and should be surrounded by single quotes.

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

   Alter the specified properties of the table *name*. The *alterations*
   are a comma-separated list of alterations.  The following
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

A BQL *population* is a named database object which contains a collection of
*variables* and their *statistical data types*. The variables of a population
correspond to (a subset of the) columns in a given SQL table *t*, known as the
''base table'' of the population. Analogously to each column in a SQL table
having a data type (such as ``INT``, ``FLOAT``, or ``VARCHAR``) which determines
what kind of data can be stored for that column, every variable in a population
has a statistical data type (such as ``NUMERICAL``, ``COUNT``, ``MAGNITUDE``, or
``NOMINAL``) which determines which probabilistic models are applicable to that
variable.

**Note**: While the terms 'column' and 'variable' are often used exchangeably,
formally a 'column' belongs to a SQL table, whereas a 'variable' belongs to a
BQL population.

.. index:: ``CREATE POPULATION``

``CREATE POPULATION [IF NOT EXISTS] <pop> FOR <table> WITH SCHEMA (<schema>)``

   Create a population with base *table* and statistical data types given by
   *schema*. The *schema* is defined using any combination of the following
   statements, separated by semicolons:

      ``GUESS STATTYPE(S) OF (<column(s)>)``

         Guess the statistical type for the given comma-separated list of
         *column(s)*, using data-dependent heuristics. Use `(*)` to indicate all
         columns in the table.

      ``SET STATTYPE(S) OF <column(s)> TO <stattype>``

         Set the statistical data type of *column(s)* to *stattype*.

      ``IGNORE <column(s)>``

         Ignore *column(s)*; no variable in the population will be created
         for these columns in the base table

.. index:: ``DROP POPULATION``

``DROP POPULATION [IF EXISTS] <pop>``

   Drop population *pop* and all its contents. Will fail if there are still
   generators associated with this population.

.. index:: ``ALTER POPULATION``

``ALTER POPULATION <pop>``

   Alter the specified properties of *pop*. The following alterations are
   supported:

   .. index:: ``ADD VARIABLE``

   ``ADD VARIABLE <varname> [<stattype>]``

      Add the given variable to the population, optionally specifying its
      statistical data type. If unspecified, the statistical type will be
      heuristically guessed.

      Note that *varname* must correspond to an existing column in the base
      table of the population; it is either a column that was specified as
      `IGNORE` when creating the population, or a column that was added later
      using e.g. the SQL command `ALTER TABLE <t> ADD COLUMN`.

   .. index:: ``SET STATTYPE``

   ``SET STATTYPE OF <variable(s)> TO <stattype>``

      Change the statistical type of the given *variable(s)* to *stattype*.

.. index:: ``GUESS SCHEMA``

``GUESS SCHEMA FOR <table>``

   Guess a population schema for *table*. The schema maps each column in *table*
   to its guessed statistical type, and gives the heuristic reason for the
   guess. Columns in *table* will be guessed to be ``NOMINAL``, ``NUMERICAL`` or
   ``IGNORE``. The query yields a table with three columns: ``name``,
   ``stattype``, and ``reason``.

++++++++++
Generators
++++++++++

A BQL *generator* is a generative probabilistic model which describes the joint
distribution of all the variables in a given base population.

.. index:: ``CREATE GENERATOR``

``CREATE GENERATOR <g> FOR <population> [USING <backend>] (<customization>)``

   Create generator *g* for *population*, optionally specifying which *backend*
   to use.

   The default backend is ``cgpm``, which uses CrossCategorization as the
   default generative model. This backend supports the following *customization*
   statements for overriding parts of the default model:

      ``OVERRIDE GENERATIVE MODEL FOR <variable(s)> [GIVEN <variable(s)>] USING <predictor>``

         Use *predictor* as the generative model for the specified (conditional)
         distribution.

      ``SUBSAMPLE(<nrows>)``

         Use a randomly chosen subsample of *nrows* rows from the base table of
         the population to use for fitting the generative model.

.. index:: ``INITIALIZE MODELS``

``INITIALIZE <n> MODELS [IF NOT EXISTS] FOR <g>``

   Initialize an ensemble of *n* models for the generator *g*.

   Each model can be thought of as a different sample of all unknown parameters
   specified by the generative model of the generative model. For example, if
   the generator used is Bayesian factor analysis, then each model may
   correspond to a different posterior sample of the factor loading matrix.

   Using ``IF NOT EXISTS`` will initialize all models in the range 0 to
   *num -1* that do not already exist.

.. index:: ``ANALYZE GENERATOR``

``ANALYZE <g> [MODELS (<indexes>)] FOR <duration> [CHECKPOINT <duration>] (<customization>)``

   Perform analysis on models in generator *g*. An optional subset of models can
   be specified by giving their *indexes*; by default, analysis will be applied
   to all models. The *duration* can take on values of ``<n> SECOND(S)``,
   ``<n> MINUTE(S)``, or ``<n> ITERATION(S)``.  The ``FOR`` duration specifies how
   long to perform analysis.  The ``CHECKPOINT`` duration specifies how often to
   commit the intermediate results of analysis to the database on disk.

   When the generator is created using the default ``cgpm`` backend, then
   the following semicolon-separated *customization* commands are supported:

      ``OPTIMIZED``

          Use a faster MCMC implementation for fitting CrossCat-modeled
          variables.

      ``QUIET``

         Suppress progress bar.

      ``SKIP <variables>``

         Analyze all variables in the population, except for the comma-separated
         list of *variables*.

      ``VARIABLES <variables>``

         Analyze only the comma-separated list of *variables*.

      ``ROWS <rows>``

         Analyze only the specified rows.

      ``SUBPROBLEMS (VARIABLE HYPERPARAMETERS, VARIABLE CLUSTERING, VARIABLE CLUSTERING CONCENTRATION, ROW CLUSTERING, ROW CLUSTERING CONCENTRATION)``

         Specify an optional set of CrossCat subproblems to apply analysis to.
         By default, analysis will cycle randomly through all subproblems.

.. index:: ``DROP GENERATOR``

``DROP [[MODEL <num>] | [MODELS <num0>-<num1>] FROM] GENERATOR [IF EXISTS] <g>``

   Drop the generator *g* and all its contents. Optionally, drop only
   the model numbered *num*, or the models ranging from *num0* to *num1*.

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

      The *table* is a comma-separated list of table names or subqueries,
      each with an optional ``AS <name>`` to qualify the table name in
      references to its columns.  When multiple tables are specified
      separated by commas, their join (cartesian product) is selected
      from.

   ``WHERE <condition>``

      The *condition* is a BQL expression selecting a subset of the input
      rows from *table* for which output rows will be computed.

   ``GROUP BY <grouping>``

      The *grouping* is a BQL expression specifying a key on which to group
      output rows.  May be the name of an output column with ``AS <name>`` in
      *columns*.

   ``ORDER BY *expression* [ASC|DESC]``

      The *expression* is a BQL expression specifying a key by which to order
      output rows, after grouping if any.  Rows are yielded in ascending order
      of the key by default or if ``ASC`` is specified, or in descending order
      of the key if ``DESC`` is specified.

   ``LIMIT <n> [OFFSET <offset>]`` or ``LIMIT <offset>, <n>``

      Both *n* and *offset* are BQL expressions.  Only up to *n* (inclusive)
      rows are returned after grouping and ordering, starting at *offset* from
      the beginning.

.. index:: ``ESTIMATE BY``

``ESTIMATE <expression> BY <population>``

   Like constant ``SELECT``, extended with model estimators of one implied row.

.. index:: ``ESTIMATE``

``ESTIMATE [DISTINCT|ALL] <expression> FROM <population> [MODELED BY <g>] [USING [MODEL <num>] [MODELS <num0>-<num1>]] [WHERE <condition>] [GROUP BY <grouping>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Like ``SELECT`` on the table associated with *population*, extended
   with model estimators of one implied row.

.. index:: ``ESTIMATE FROM VARIABLES OF``

``ESTIMATE <expression> FROM VARIABLES OF <population> [MODELED BY <g>] [USING [MODEL <num>] [MODELS <num0>-<num1>]] [WHERE <condition>] [GROUP BY <grouping>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Like ``SELECT`` on the modeled columns of *population*, extended
   with model estimators of one implied column.

.. index:: ``ESTIMATE FROM PAIRWISE VARIABLES OF``

``ESTIMATE <expression> FROM PAIRWISE VARIABLES OF <population> [FOR <subcolumns>] [MODELED BY <g>] [USING [MODEL <num>] [MODELS <num0>-<num1>]] [WHERE <condition>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Like ``SELECT`` on the self-join of the modeled columns of
   *population*, extended with model estimators of two implied columns.

   In addition to a literal list of column names, the list of
   *subcolumns* may be an ``ESTIMATE * FROM VARIABLES OF`` subquery.

.. index:: ``ESTIMATE, PAIRWISE``

``ESTIMATE <expression> FROM PAIRWISE <population> [MODELED BY <g>] [USING [MODEL <num>] [MODELS <num0>-<num1>] [WHERE <condition>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Like ``SELECT`` on the self-join of the table associated with *population*,
   extended with model estimators of two implied rows.

   (Currently the only *expression* functions of two implied rows are
   ``SIMILARITY`` and ``SIMILARITY IN THE CONTEXT OF (...)``.)

.. index:: ``INFER``

``INFER <colnames> [WITH CONFIDENCE <conf>] FROM <population> [MODELED BY <g>] [USING [MODEL <num>] [MODELS <num0>-<num1>]] [WHERE <condition>] [GROUP BY <grouping>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Select the specified *colnames* from *population*, filling in missing values
   if they can be filled in with confidence at least *conf*, a BQL expression.
   Only missing values *colnames* will be filled in; missing values in columns
   named in *condition*, *grouping*, and *ordering* will not be.  Model
   estimators and model predictions are allowed in the expressions.

   The *colnames* is a comma-separated list of column names, **not** arbitrary
   BQL expressions.

.. index:: ``INFER EXPLICIT``

``INFER EXPLICIT <expression> FROM <population> [MODELED BY <g>] [USING [MODEL <num>] [MODELS <num0>-<num1>]] [WHERE <condition>] [GROUP BY <grouping>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Like ``SELECT`` on the table associated with *population*, extended
   with model estimators of one implied row and with model predictions.

   In addition to normal ``SELECT`` columns, *expression* may include:

      ``PREDICT <name> [AS <rename>] CONFIDENCE <confname>``

   This results in two resulting columns, one named *rename*, or
   *name* if *rename* is not supplied, holding a predicted value of
   the column *name*, and one named *confname* holding the confidence
   of the prediction.

.. index:: ``SIMULATE``

``SIMULATE <colnames> FROM <population> [MODELED BY <g>] [USING [MODEL <num>] [MODELS <num0>-<num1>]] [GIVEN <constraints>] [LIMIT <limit>]``

   Select the requested *colnames* from rows sampled from *population*.
   The *constraints* is a comma-separated list of constraints of the form

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
            MUTUAL INFORMATION AS "mutinf"
        FROM PAIRWISE VARIABLES OF p
        ORDER BY "mutinf"

has the effect of estimating mutual information twice for each row because it is
mentioned twice, once in the output and once in the ORDER BY, which is twice as
slow as it needs to be.   (Actually, approximately four times, because mutual
information is symmetric, but that is an orthogonal issue.)

To avoid this double evaluation, you can order the results of a subquery
instead:

    .. code-block:: sql

        SELECT *
        FROM (
            ESTIMATE MUTUAL INFORMATION AS "mutinf"
            FROM PAIRWISE VARIABLES OF p
        )
        ORDER BY "mutinf"

.. index:: ``PREDICTIVE PROBABILITY``

``PREDICTIVE PROBABILITY OF <column> [GIVEN (<column(s)>)]``

   Function of one implied row.  Returns the predictive probability of the row's
   value for the column named *column*, optionally given the data in *column(s)*
   in the row.

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

``SIMILARITY [OF (<boolexpr0>)] [TO (<boolexpr1>)] IN THE CONTEXT OF <column>``

   Constant, or function of one or two implied rows. If given both ``OF`` and
   ``TO``, returns a constant measure of similarity between the first row
   satisfied by *boolexpr0* and the first row satisfied by *boolexpr1*. If
   given only  ``TO`` returns a measure of the similarity of the implied row
   with the first row satisfying *boolexpr1*. Otherwise, returns a measure of
   the similarity of the two implied rows.  The similarity may be
   considered within the context of a column.

.. index:: ``PREDICTIVE RELEVANCE``

``PREDICTIVE RELEVANCE [OF (<boolexpr0>)] TO EXISTING ROWS (<boolexpr1>) IN THE CONTEXT OF <column>``

``PREDICTIVE RELEVANCE [OF (<boolexpr0>)] TO HYPOTHETICAL ROWS (<boolexpr1>) IN THE CONTEXT OF <column>``

``PREDICTIVE RELEVANCE [OF (<boolexpr0>)] TO EXISTING ROWS (<boolexpr1>) AND HYPOTHETICAL ROWS (<boolexpr2>) IN THE CONTEXT OF <column>``

   If given ``OF``, returns a measure of predictive relevance of the first row
   satisfying *boolexpr0* for the existing and/or hypothetical rows satisfying
   *boolexpr1* (and *boolexpr2* in the case of both) in the context of
   *column*. Otherwise, returns a measure of predictive relevance of all rows to
   the specified existing and/or hypothetical rows.

.. index:: ``CORRELATION``

``CORRELATION [[OF <column1>] WITH <column2>]``

   Constant, or function of one or two implied columns.  Returns standard
   measures of correlation between columns:

   * Pearson correlation coefficient squared for two numerical columns.
   * Cramer's phi for two nominal columns.
   * ANOVA R^2 for a nominal column and a numerical column.

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
