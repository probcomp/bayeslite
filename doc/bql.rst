BQL: Bayesian Query Language
============================

BQL is a probabilistic extension to SQL that supports

* standard deterministic SQL queries,
* estimating probabilities and strengths of relationships between rows
  and columns,
* inferring missing values within confidence intervals, and
* simulating data from generative models.

BQL does not currently include any of the SQL Data Modification
Language (DML), i.e. ``INSERT``, ``UPDATE``, &c., and includes only
limited subset of the SQL Data Definition Language (DDL), i.e. ``CREATE
TABLE``.

A BQL phrase is either a command or a query.  In contexts admitting
multiple BQL phrases, e.g. at the bayeslite shell, each phrase must be
terminated by a semicolon before the next one begins.

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

   Alter the specified properties of the table *name*.  The following
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

Data Modelling Language
^^^^^^^^^^^^^^^^^^^^^^^

.. index:: ``CREATE GENERATOR``
``CREATE [DEFAULT] GENERATOR <name> [IF NOT EXISTS] FOR <table> USING <metamodel> (<schema>)``

   Create a generative model named *name* for *table* in the language
   of *metamodel*.  *Schema* describes the generative model in syntax
   that depends on the metamodel.  Typically, it is a comma-separated
   list of clauses of the form

      ``<column> <type>``

   requesting the column *column* to be modelled with the statistical
   type *type*, with some additional types of clauses.  For example,

   .. code-block:: sql

      CREATE GENERATOR t_cc FOR t USING crosscat (
          SUBSAMPLE(1000),      -- Subsample down to 1000 rows;
          GUESS(*),             -- guess all column types, except
          name IGNORE,          -- ignore the name column, and
          angle CYCLIC          -- treat angle as CYCLIC.
      )

   If ``DEFAULT`` is specified, then *name* will become the default
   generator of *table*: anywhere a generator is required, *table* may
   be used in its place, and the generator *name* will be understood.
   The default generator may be changed with :index:`ALTER TABLE` and
   :index:`SET DEFAULT GENERATOR` or :index:`UNSET DEFAULT GENERATOR`.

.. index:: ``DROP GENERATOR``
``DROP GENERATOR [IF EXISTS] <name>``

   Drop the generator *name* and all its models.

.. index:: ``ALTER GENERATOR``
``ALTER GENERATOR <name> <alterations>``

   Alter the specified properties of the generator named *name*.  The
   following alterations are supported:

   .. index:: ``RENAME TO``
   ``RENAME TO <newname>``

      Change the generator's name to *newname*.

.. index:: ``INITIALIZE MODELS``
``INITIALIZE <n> MODEL[S] [IF NOT EXISTS] FOR <name>``

   Perform metamodel-specific initialization of up to *n* models for
   the generator *name*.  If the generator already had models, the
   ones it had are unchanged.

.. index:: ``DROP MODELS``
``DROP MODELS <modelset> FROM <name>``

   Drop the specified models from the generator *name*.  *Modelset* is
   a comma-separated list of model numbers or hyphenated model number
   ranges, inclusive on both bounds.

   Example:

      ``DROP MODELS 1-3 FROM t_cc``

   Equivalent:

      ``DROP MODEL 1 FROM t_cc; DROP MODEL 2 FROM t_cc; DROP MODEL 3 FROM t_cc``

.. index:: ``ANALYZE MODELS``
``ANALYZE <name> [MODEL[S] <modelset>] [FOR <duration>] [CHECKPOINT <duration>] WAIT``

   Perform metamodel-specific analysis of the specified models of the
   generator *name*.  *Modelset* is a comma-separated list of model
   numbers or hyphenated model number ranges.  *Duration* is either
   ``<n> SECOND[S]``, ``<n> MINUTE[S]``, or ``<n> ITERATION[S]``.

   The ``FOR`` duration specifies how long to perform analysis.  The
   ``CHECKPOINT`` duration specifies how often to commit the
   intermediate results of analysis to the database on disk.

   Examples:

      ``ANALYZE t_cc FOR 10 MINUTES CHECKPOINT 30 SECONDS``

      ``ANALYZE t_cc MODELS 1-3,7-9 FOR 10 ITERATIONS CHECKPOINT 1 ITERATION``

BQL Queries
-----------

.. index:: ``SELECT``
``SELECT [DISTINCT|ALL] <columns> FROM <table> [WHERE <condition>] [GROUP BY <grouping>] [ORDER BY <ordering>] [LIMIT <limit>]``

   Standard SQL select.

.. index:: ``ESTIMATE``
``ESTIMATE [DISTINCT|ALL] <columns> FROM <generator> [USING MODEL <modelno>] [WHERE <condition>] [GROUP BY <grouping>] [ORDER BY <ordering>] [LIMIT <limit>]``

   ...

.. index:: ``ESTIMATE COLUMNS``
``ESTIMATE COLUMNS [<columns>] FROM <generator> [USING MODEL <modelno>] [WHERE <condition>] [GROUP BY <grouping>] [ORDER BY <ordering>] [LIMIT <limit>]``

   ...

.. index:: ``ESTIMATE PAIRWISE``
``ESTIMATE PAIRWISE <columns> FROM <generator> [FOR <subcolumns>] [USING MODEL <modelno>] [WHERE <condition>] [ORDER BY <ordering>] [LIMIT <limit>]``

   ...

.. index:: ``ESTIMATE PAIRWISE ROW``
``ESTIMATE PAIRWISE ROW <expression> FROM <generator> [USING MODEL <modelno>] [WHERE <condition>] [ORDER BY <ordering>] [LIMIT <limit>]``

   ...

.. index:: ``INFER``
``INFER <colnames> [WITH CONFIDENCE <conf>] FROM <generator> [USING MODEL <modelno>] [WHERE <condition>] [GROUP BY <grouping>] [ORDER BY <ordering>] [LIMIT <limit>]``

   ...

.. index:: ``INFER EXPLICIT``
``INFER EXPLICIT <columns> FROM <generator> [USING MODEL <modelno>] [WHERE <condition>] [GROUP BY <grouping>] [ORDER BY <ordering>] [LIMIT <limit>]``

   ...

.. index:: ``SIMULATE``
``SIMULATE <colnames> FROM <generator> [USING MODEL <modelno>] [GIVEN <constraint>]``

   ...
