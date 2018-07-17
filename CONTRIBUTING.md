# Contributing Guidelines

This document contains general software standards for contributing to
this repository.

__Please make sure you have addressed each item in this document__
before starting work on a feature branch, writing a patch, or
submitting a pull request.

### Git standards

- All branch names must be of the form
  `[year][month][day]-[username]-[description]`.
  A well named branch is `20180206-fsaad-update-contributing`.
  A poorly named branch is `conda`.

- Do not delete branches; all history should be maintained.

- Git commit messages should be full, comprehensible, punctuated, and
  informative English sentences.
  Good commit message: `"Add Git section to CONTRIBUTING.md."`.
  Bad commit message: `"ugh"`, or `"fix contributing"`.

- Each minimal change of functionality should be in its own Git
  commit.  Avoid mixing non-functional diffs that do not alter the
  behavior of the code (e.g. removing whitespace, improving
  formatting, reorganizing imports), with functional diffs that alter
  the behavior the code.

- Maintainers: when merging feature branches into `master`, always
  create an explicit merge commit using `git merge --no-ff`.  __Avoid
  merge via fast-forward__. Reasoning is: (i) merge commits can be
  easily reverted, (ii) the branch topology of the Github history is
  easier to track, and (iii) each merge commit in the history of the
  master branch is confirmed to pass the test suite.

### Python coding style

Generally follow [PEP 8](https://www.python.org/dev/peps/pep-0008/);
emphasis on items below:

- Consistency is critical; follow the style of the module that you are
  editing.

- Continuation lines must use an [explicit line break][1], or [four
  spaces][2].  __[Avoid alignment style with hanging indent][3]__, it
  penalizes descriptive function names, renaming, and source
  maintenance.

- Always use single-quoted strings `'hello world'` instead of double
  quotes `"hello world"` in program code, including for triply-quoted
  strings.  Standardizing on the single-quote character improves source
  readability and maintenance.

- For doc strings, use a triple double-quote `"""docstring here"""`,
  instead of a triple single-quote `'''docstring here'''`.

- Imports should be organized into blocks:

    1. standard library
    2. third-party packages
    3. current package imports, named
    4. current package imports, relative

  Each import block should be organized alphabetically.  First write
  all unqualified imports, e.g. `import sys`, then all named imports,
  e.g. `from StringIO import StringIO`, separated by one line.  Long
  blocks can be further separated by one line. Here is an
  [example][4].

### Bayeslite-specific standards

Refer to [HACKING](./HACKING) for detailed guidelines on how to
develop software in a good style for bayeslite, in a way that
maximizes readability and minimizes unexpected bugs.

### Automatic tests

Every commit on long-term branches including master should pass

    $ ./check.sh

It builds bayeslite and runs the automatic smoke tests.  Consider running this
in your `.git/hooks/pre-commit` script during development, and in each
commit please add or update automatic tests for any bugs you add or
features you fix in that commit.

`check.sh` has two modes: if you pass no arguments, it runs smoke
tests only.  There are tests that are slower, use the network, or
otherwise are better suited to testing during continuous integration
only. If you run a particular test, e.g.:

    $ ./check.sh --pdb tests/test_foo.py

that will run even the tests that would otherwise have been run during
continuous integration, for that file (and with `--pdb` it will drop
you into the python debugger on the first failure). These tests are
marked with `__ci_` in their names, and you can `git grep __ci_` to
find them.

If you want to run all tests, use:

    $ ./check.sh tests/ shell/tests

Because this may take awhile, we do not require it of all commits, but
you should monitor the continuous integration suite, and rapidly roll
back your change if it causes a failure.

This software is automatically tested on
[Travis](https://travis-ci.org/probcomp/bayeslite).

### Copyright headers

Files should have appropriate copyright headers, refer to [`src/__init__.py`][5]
for the probcomp template.

### Versions

Our version scheme, compatible with [PEP 440](https://www.python.org/dev/peps/pep-0440/):

    <major>.<minor>[.<teeny>]a<date>    (prerelease snapshot)
    <major>.<minor>[.<teeny>]rc<N>      (release candidate)
    <major>.<minor>[.<teeny>]           (release)

We do not currently make any semantic API compatibility guarantees about the
meaning of `<major>`, `<minor>`, and `<teeny>`.

To create a new release (only creating tagged releases from `master` branch):

  1. Check-out the master branch with a clean working directory.

  2. Create an annotated tag using one of the three forms compatible with PEP
     440 shown above. For example: `git tag -a -m v0.2.42 v0.2.42`

  3. Run `python setup.py build`.

  4. Run `cat src/version.py` and confirm that the version matches the tag. If
     the version contains `dirty`, `post`, or a commit id then you have done
     something wrong, so please ask for help.

  5. Run `git push origin master --tags`.

[1]: https://github.com/probcomp/bayeslite/blob/9555f5fd614e7dd960dcf8b54ae8edc5b69d7d1a/src/backends/cgpm_backend.py#L835-L836
[2]: https://github.com/probcomp/bayeslite/blob/9555f5fd614e7dd960dcf8b54ae8edc5b69d7d1a/src/bqlfn.py#L95-L96
[3]: https://github.com/scikit-learn/scikit-learn/blob/0788cd0c6a91c0d1cae17340cdf5d2af3c59ec57/sklearn/ensemble/iforest.py#L215-L217
[4]: https://github.com/probcomp/bayeslite/blob/9555f5fd614e7dd960dcf8b54ae8edc5b69d7d1a/src/backends/loom_backend.py#L23-L58
[5]: https://github.com/probcomp/bayeslite/blob/858be761448f6c537b81b7a2bc9876c9e226c72e/src/__init__.py#L1-L15
