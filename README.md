# Bayeslite
Bayeslite is a prototype reimplementation of BayesDB on SQLite3, to enable the use of relational SQL queries on databases in addition to probabilistic BQL queries.

## Install
**Bayeslite is targeted at Ubuntu 14.04**

After you have installed [CrossCat](https://github.com/mit-probabilistic-computing-project/crosscat):

```bash
$ python setup.py build
$ python setup.py install
```

To check that everything is working:

```bash
$ ./check.sh
```
