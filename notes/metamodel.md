# Plan for Staged Refactoring of Metamodel Interface

- - -

## Section 1: Introduction

Below are the suggested changes to the signature of each function in the current
Bayeslite `metamodel.py` interface. The main change is that the functions are
simply being generalized to query arbitrary patterns of cells. There is an
explicit difference between *observed* and *hypothetical* rows. For a table
with `R` rows, an *observed* row `r_i` is one where `r_i` is in
`[1,...,R]`. A hypothetical row is indicated by sampling `r_i ~ Uniform[0,1]`
to ensure all hypothetical members are unique.

This document outlines the list of functions currently in `metamodel.py` and
proposes a new function signature, a generic definition on how it can be
implemented natively, and an explanation as to how the behavior of the current
definition can be realized in CrossCat using the new definition.

An important note is that the staged refactoring of Bayeslite metamodels
is a separate project from updating the CrossCat implementation such that it
can fully implement the new interface. Instead, this document will show how to
use the inputs to the generalized functions such that they are fully compatible
with CrossCat's current implementation. The long-term goal is to replace the
current CrossCat implementation with a more powerful, general version written in
VentureScript, but such a project is beyond the scope of this document.

A particular metamodel can admit optimized approximations for each function
based on its internal structure. For instance, CrossCat defines dependence in
terms of column and row partitions. A mixture of Gaussian distributions has a
closed form for mutual information. Naive Bayes assumes that all columns are
mutually independent. A classic regression metamodel can only specify
*target* columns [which correspond to *response* variables] and explicitly use
*given* columns [which are *regressor* variables] in each query; this regression
model overrides default implementations (which do not make sense
under the model assumptions) with a `NOT IMPLEMETED` error.

The minimum primitives that each metamodel ascribing to the interface
must implement are `SIMULATE` and `LOGPDF`. The rest of the functions can be
implemented generically by invoking these two primitives and forming Monte Carlo
estimates.

(note to self: how will BQL queries allow the user to specify the difference between
observed and hypothetical members of the population?)

- - -

## Section 2: Notation

We will use `(c_i, r_j)` denote the cell `X[j,k]` in the table, which is a
univariate random variable for which we have (typically) one realization.
However, multiple realization for a single cell are also possible (note to self:
how is this indicated in a database table?). Random vectors are expressed as
arbitrary collections of cells, over which we can define joint distributions.

For a table `X` with `C` columns and `R` rows, the allowed values of the indices
are:

- `c_i` in `{1, ... , C}`       (definition disallows hypothetical cols)
- `r_j` in `{1, ... , R} \U (0,1)` (indices in (0,1) indicate hypothetical rows)

We will use capital letters to denote collections of tuples, where each tuple is
a cell from the table. In other words:

- `A = [(c_i^a, r_i^a) for i = 1 ... |A|]`
- `B = [(c_i^b, r_i^b) for i = 1 ... |B|]`

Sometimes we will need to specify values for cells `X[c,r] = x`, so we will use
3-tuples of the form
- `Fx = [(c_i^f, r_i^f, x_{(c_i^f,r_i^f)}) for i = 1 ... |Fx|]`

For example the notation `p(A,B|Fx)` is a shorthand for the joint distribution:
```python
p( X[c_1^a,r_1^a],...,X[c_|A|^a,r_|A|^a],
   X[c_1^b,r_1^b],...,X[c_|B|^b,r_|B|^b] | # <- NOTE WE ARE CONIDTIONING
   X[c_i^f, r_i^f] = x_{(c_i^f,r_i^f)})
  )
```

- - -

## Section 3: Proposed Changes to Existing Functions

### DEPENDENCE PROBABILITY

#### CURRENT DEFINITION
```python
def column_dependence_probability(self, bdb, generator_id, modelno,
    colno0, colno1)
```

#### PROPOSED DEFINITION
```python
# Computes the probability that the joint distribution p(A,B|Gx)
# factors as p(A|G)*p(B|G). Alternatively computes the probability that the KL
# divergence between the two said distributions is zero.
def dependence_probability(self, bdb, generator_id, modelno, A, B, Gx)
```

#### GENERIC IMPLEMENTATION
TODO

#### CROSSCAT IMPLEMENTATION
current
```python
column_dependence_probability(self, bdb, generator_id, modelno,
    colno0 = c0, colno1 = c1)
````

proposed
```python
dependence_probability(self, bdb, generator_id, modelno,
    A = [(c0,r*)], B = [(c1,r*)], G = NONE)
```
- Only allow `A` to have one column `A = [(c_1^a,r*)]`. Ignore the row `r*`
- Same for `B`.
- Ignore `G` (conditionals).
- Proceed as `column_dependence_probability` currently does.

- - -

### Mutual Information

#### CURRENT DEFINITION
```python
def column_mutual_information(self, bdb, generator_id, modelno, colno0,
    colno1, numsamples=None):
```


#### PROPOSED DEFINITION
```python
# Computes the expectation of the mutual information of the two abstract
# distributions P(A|G,Fx), P(B|G,Fx) under the distribution of G:
# In other words MI(X:Y|Z,Fx).
# NOTE: multivariate mutual information of the form MI(X:Y:W|Z,Fx)
# is not supported, due to weak theory about this object and its
# interpretablity.
def mutual_information(self, bdb, generator_id, modelno, A, B, G, Fx)
```

#### GENERIC IMPLEMENTATION
Please see [here](https://docs.google.com/document/d/11u6uLNBzlveZVPkBADjvTi9f9m0Y7VIhzy4WOlWFbaY/edit#bookmark=id.8wtqyb1urgz8)
on how `SIMULATE` and `LOGPDF` can implement this.


#### CROSSCAT IMPLEMENTATION
current
```python
column_mutual_information(self, bdb, generator_id, modelno,
    colno0=c0, colno1=c1, numsamples=None)
```

proposed
```python
mutual_information(self, bdb, generator_id, modelno,
    A = [(c0,r*)], B = [(c1,r*)], G=NONE, Fx=NONE).
```
- Only allow `A` to have one column `A = [(c_1^a,r_1^a)]`. Ignore row `r*`.
- Same for `B`.
- Ignore givens entirely.
- Do the same thing as mutual information currently does.

- - -

### SIMULATE

#### CURRENT DEFINITION
```python
def simulate(self, bdb, generator_id, modelno, constraints, colnos,
    numpredictions=1)
```

#### PROPOSED DEFINITION
```python
# Simulates from the distribution of p(A|Gx).
def simulate(self, bdb, generator_id, modelno, A, Gx)
```

#### GENERIC IMPLEMENTATION
Metamodel-specific.


#### CROSSCAT IMPLEMENTATION
current
```python
simulate(self, bdb, generator_id, modelno, constraints, colnos,
    numpredictions=1)
```

proposed
```python
simulate(self, bdb, generator_id, modelno,
    A = [(colnos,r),...], Gx = constraints])
```
- Rather than use `fake row` in Y and Q (see current SIMULATE source),
use the rows from A and Gx (if they are not hypothetical).
- If any of the rows in A or Gx is hypothetical, then use a fake row
(as is currently being done).

- - -

### LOGPDF

#### CURRENT DEFINITION
Currently there are two ways to evaluate the pdf, different for hypothetical
vs observed members. These can now be unified into a single function.
CrossCat cannot evaluate pdf of multivariate densities, so we will avoid
the behavior by only allowing one column in the generalized version.
```python
def column_value_probability(self, bdb, generator_id, modelno, colno,
    value)
```
```python
def row_column_predictive_probability(self, bdb, generator_id, modelno,
    rowid, colno)
```

#### PROPOSED DEFINITION
```python
# Evaluates the density p(A=Ax|G=Gx).
def logpdf(self, bdb, generator_id, modelno, A, Gx)
```

#### GENERIC IMPLEMENTATION
Meta-model specific.


#### CROSSCAT IMPLEMENTATION
current for `column_value_probability`:
```python
column_value_probability(self, bdb, generator_id, modelno, colno, value)
```

proposed using `logpdf`:
```python
logpdf(self, bdb, generator_id, modelno, A=[(colno,r*,value)], Gx=NONE)
```
- `A` can only contain one cell, and the row `r*` will be ignored.
- `Gx` is ignored entirely.
- Proceed with current implementation.

current for `column_predictive_probability`:
```python
def row_column_predictive_probability(self, bdb, generator_id, modelno,
    rowid, colno)
```

proposed using `logpdf`:
```
logpdf(self, bdb, generator_id, modelno, A=[(colno,rowid,value)],
    Gx=[(col1,rowid,value1),(col2,rowid,value2),...)
```python
- `A` can only contain one cell, `rowid` is not ignored. `value` is taken from
the current value for `(colno,rowid)` in the table.
- `Gx` contains a list of cells along the same `rowid`, with the `value`s taken
from the current table. Any cell from a different `rowid` is ignored.
- Proceed with current implementation.


# TODO
- Row Similarity
```python
def row_similarity(self, bdb, generator_id, modelno, rowid, target_rowid,
    colnos)
```
- Column Typicality
```python
def column_typicality(self, bdb, generator_id, modelno, colno)
```
- Feedback, iterations, comments, updates.
