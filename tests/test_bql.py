# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2014, MIT Probabilistic Computing Project
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import StringIO
import pytest
import tempfile

import bayeslite.ast as ast
import bayeslite.bql as bql
import bayeslite.parse as parse

import test_core
import test_csv

def bql2sql(string):
    with test_core.t1() as (bdb, _table_id):
        phrases = parse.parse_bql_string(string)
        out = StringIO.StringIO()
        for phrase in phrases:
            assert ast.is_query(phrase)
            bql.compile_query(bdb, phrase, out)
            out.write(';')
        return out.getvalue()

# XXX Kludgey mess.  Please reorganize.
def bql2sqlparam(string):
    with test_core.t1() as (bdb, _table_id):
        phrases = parse.parse_bql_string(string)
        out0 = StringIO.StringIO()
        for phrase in phrases:
            out = None
            if isinstance(phrase, ast.Parametrized):
                bindings = (None,) * phrase.n_numpar
                out = bql.Output(phrase.n_numpar, phrase.nampar_map, bindings)
                phrase = phrase.phrase
            else:
                out = StringIO.StringIO()
            assert ast.is_query(phrase)
            bql.compile_query(bdb, phrase, out)
            # XXX Do something about the parameters.
            out0.write(out.getvalue())
            out0.write(';')
        return out0.getvalue()

def bql_execute(bdb, string, bindings=()):
    return map(tuple, bdb.execute(string, bindings))

def test_select_trivial():
    assert bql2sql('select null;') == 'SELECT NULL;'
    assert bql2sql("select 'x';") == "SELECT 'x';"
    assert bql2sql("select 'x''y';") == "SELECT 'x''y';"
    assert bql2sql('select "x";') == 'SELECT "x";'
    assert bql2sql('select "x""y";') == 'SELECT "x""y";'
    assert bql2sql('select 0;') == 'SELECT 0;'
    assert bql2sql('select 0.;') == 'SELECT 0.0;'
    assert bql2sql('select .0;') == 'SELECT 0.0;'
    assert bql2sql('select 0.0;') == 'SELECT 0.0;'
    assert bql2sql('select 1e0;') == 'SELECT 1.0;'
    assert bql2sql('select 1e+1;') == 'SELECT 10.0;'
    assert bql2sql('select 1e-1;') == 'SELECT 0.1;'
    assert bql2sql('select .1e0;') == 'SELECT 0.1;'
    assert bql2sql('select 1.e10;') == 'SELECT 10000000000.0;'
    assert bql2sql('select all 0;') == 'SELECT 0;'
    assert bql2sql('select distinct 0;') == 'SELECT DISTINCT 0;'
    assert bql2sql('select 0 as z;') == 'SELECT 0 AS "z";'
    assert bql2sql('select * from t;') == 'SELECT * FROM "t";'
    assert bql2sql('select t.* from t;') == 'SELECT "t".* FROM "t";'
    assert bql2sql('select c from t;') == 'SELECT "c" FROM "t";'
    assert bql2sql('select c as d from t;') == 'SELECT "c" AS "d" FROM "t";'
    assert bql2sql('select t.c as d from t;') == \
        'SELECT "t"."c" AS "d" FROM "t";'
    assert bql2sql('select t.c as d, p as q, x from t;') == \
        'SELECT "t"."c" AS "d", "p" AS "q", "x" FROM "t";'
    assert bql2sql('select * from t, u;') == 'SELECT * FROM "t", "u";'
    assert bql2sql('select * from t as u;') == 'SELECT * FROM "t" AS "u";'
    assert bql2sql('select * from (select 0);') == 'SELECT * FROM (SELECT 0);'
    assert bql2sql('select t.c from (select d as c from u) as t;') == \
        'SELECT "t"."c" FROM (SELECT "d" AS "c" FROM "u") AS "t";'
    assert bql2sql('select * where x;') == 'SELECT * WHERE "x";'
    assert bql2sql('select * from t where x;') == \
        'SELECT * FROM "t" WHERE "x";'
    assert bql2sql('select * group by x;') == 'SELECT * GROUP BY "x";'
    assert bql2sql('select * from t where x group by y;') == \
        'SELECT * FROM "t" WHERE "x" GROUP BY "y";'
    assert bql2sql('select * from t where x group by y, z;') == \
        'SELECT * FROM "t" WHERE "x" GROUP BY "y", "z";'
    assert bql2sql('select * order by x;') == 'SELECT * ORDER BY "x";'
    assert bql2sql('select * order by x asc;') == 'SELECT * ORDER BY "x";'
    assert bql2sql('select * order by x desc;') == \
        'SELECT * ORDER BY "x" DESC;'
    assert bql2sql('select * order by x, y;') == 'SELECT * ORDER BY "x", "y";'
    assert bql2sql('select * order by x desc, y;') == \
        'SELECT * ORDER BY "x" DESC, "y";'
    assert bql2sql('select * order by x, y asc;') == \
        'SELECT * ORDER BY "x", "y";'
    assert bql2sql('select * limit 32;') == 'SELECT * LIMIT 32;'
    assert bql2sql('select * limit 32 offset 16;') == \
        'SELECT * LIMIT 32 OFFSET 16;'
    assert bql2sql('select * limit 16, 32;') == 'SELECT * LIMIT 32 OFFSET 16;'
    assert bql2sql('select (select0);') == 'SELECT "select0";'
    assert bql2sql('select (select 0);') == 'SELECT (SELECT 0);'
    assert bql2sql('select f(f(), f(x), y);') == \
        'SELECT "f"("f"(), "f"("x"), "y");'
    assert bql2sql('select a and b or c or not d is e is not f like j;') == \
        'SELECT ((("a" AND "b") OR "c") OR' \
        + ' (NOT ((("d" IS "e") IS NOT "f") LIKE "j")));'
    assert bql2sql('select a like b not like c like d escape e;') == \
        'SELECT ((("a" LIKE "b") NOT LIKE "c") LIKE "d" ESCAPE "e");'
    assert bql2sql('select a like b escape c glob d not glob e;') == \
        'SELECT ((("a" LIKE "b" ESCAPE "c") GLOB "d") NOT GLOB "e");'
    assert bql2sql('select a not glob b glob c escape d;') == \
        'SELECT (("a" NOT GLOB "b") GLOB "c" ESCAPE "d");'
    assert bql2sql('select a glob b escape c regexp e not regexp f;') == \
        'SELECT ((("a" GLOB "b" ESCAPE "c") REGEXP "e") NOT REGEXP "f");'
    assert bql2sql('select a not regexp b regexp c escape d;') == \
        'SELECT (("a" NOT REGEXP "b") REGEXP "c" ESCAPE "d");'
    assert bql2sql('select a regexp b escape c not regexp d escape e;') == \
        'SELECT (("a" REGEXP "b" ESCAPE "c") NOT REGEXP "d" ESCAPE "e");'
    assert bql2sql('select a not regexp b escape c match e not match f;') == \
        'SELECT ((("a" NOT REGEXP "b" ESCAPE "c") MATCH "e") NOT MATCH "f");'
    assert bql2sql('select a not match b match c escape d;') == \
        'SELECT (("a" NOT MATCH "b") MATCH "c" ESCAPE "d");'
    assert bql2sql('select a match b escape c not match d escape e;') == \
        'SELECT (("a" MATCH "b" ESCAPE "c") NOT MATCH "d" ESCAPE "e");'
    assert bql2sql('select a not match b escape c between d and e;') == \
        'SELECT (("a" NOT MATCH "b" ESCAPE "c") BETWEEN "d" AND "e");'
    assert bql2sql('select a between b and c and d;') == \
        'SELECT (("a" BETWEEN "b" AND "c") AND "d");'
    assert bql2sql('select a like b like c escape d between e and f;') == \
        'SELECT ((("a" LIKE "b") LIKE "c" ESCAPE "d") BETWEEN "e" AND "f");'
    assert bql2sql('select a between b and c not between d and e;') == \
        'SELECT (("a" BETWEEN "b" AND "c") NOT BETWEEN "d" AND "e");'
    assert bql2sql('select a not between b and c in (select f);') == \
        'SELECT (("a" NOT BETWEEN "b" AND "c") IN (SELECT "f"));'
    assert bql2sql('select a in (select b) and c not in (select d);') == \
        'SELECT (("a" IN (SELECT "b")) AND ("c" NOT IN (SELECT "d")));'
    assert bql2sql('select a in (select b) isnull notnull!=c<>d<e<=f>g;') == \
        'SELECT ((((("a" IN (SELECT "b")) ISNULL) NOTNULL) != "c") !=' \
        + ' ((("d" < "e") <= "f") > "g"));'
    assert bql2sql('select a>b>=c<<d>>e&f|g+h-i*j/k;') == \
        'SELECT (("a" > "b") >= (((("c" << "d") >> "e") & "f") |' \
        + ' (("g" + "h") - (("i" * "j") / "k"))));'
    assert bql2sql('select a/b%c||~~d collate e collate\'f\'||1;') == \
        'SELECT (("a" / "b") % (("c" || (((~ (~ "d")) COLLATE "e")' \
        + ' COLLATE "f")) || 1));'
    assert bql2sql('select cast(f(x) as binary blob);') == \
        'SELECT CAST("f"("x") AS "binary" "blob");'
    assert bql2sql('select cast(42 as varint(73));') == \
        'SELECT CAST(42 AS "varint"(73));'
    assert bql2sql('select cast(f(x, y, z) as varchar(12 ,34));') == \
        'SELECT CAST("f"("x", "y", "z") AS "varchar"(12, 34));'
    assert bql2sql('select exists (select a) and not exists (select b);') == \
        'SELECT (EXISTS (SELECT "a") AND (NOT EXISTS (SELECT "b")));'
    assert bql2sql('select case when a - b then c else d end from t;') == \
        'SELECT CASE WHEN ("a" - "b") THEN "c" ELSE "d" END FROM "t";'
    assert bql2sql('select case f(a) when b + c then d else e end from t;') \
        == \
        'SELECT CASE "f"("a") WHEN ("b" + "c") THEN "d" ELSE "e" END FROM "t";'

def test_select_bql():
    assert bql2sql('select predictive probability of weight from t1;') == \
        'SELECT bql_row_column_predictive_probability(1, _rowid_, 2) FROM "t1";'
    assert bql2sql('select label, predictive probability of weight from t1;') \
        == \
        'SELECT "label", bql_row_column_predictive_probability(1, _rowid_, 2)' \
        + ' FROM "t1";'
    assert bql2sql('select predictive probability of weight, label from t1;') \
        == \
        'SELECT bql_row_column_predictive_probability(1, _rowid_, 2), "label"' \
        + ' FROM "t1";'
    assert bql2sql('select predictive probability of weight + 1 from t1;') == \
        'SELECT (bql_row_column_predictive_probability(1, _rowid_, 2) + 1)' \
        + ' FROM "t1";'
    with pytest.raises(ValueError):
        # Need a table.
        bql2sql('select predictive probability of weight;')
    with pytest.raises(ValueError):
        # Need at most one table.
        bql2sql('select predictive probability of weight from t1, t1;')
    with pytest.raises(ValueError):
        # Need a btable, not a subquery.
        bql2sql('select predictive probability of weight from (select 0);')
    with pytest.raises(parse.ParseError):
        # Need a column.
        bql2sql('select predictive probability from t1;')
    assert bql2sql('select probability of weight = 20 from t1;') == \
        'SELECT bql_column_value_probability(1, 2, 20) FROM "t1";'
    assert bql2sql('select probability of weight = (c + 1) from t1;') == \
        'SELECT bql_column_value_probability(1, 2, ("c" + 1)) FROM "t1";'
    assert bql2sql('select probability of weight = f(c) from t1;') == \
        'SELECT bql_column_value_probability(1, 2, "f"("c")) FROM "t1";'
    assert bql2sql('select typicality from t1;') == \
        'SELECT bql_row_typicality(1, _rowid_) FROM "t1";'
    assert bql2sql('select typicality of age from t1;') == \
        'SELECT bql_column_typicality(1, 1) FROM "t1";'
    assert bql2sql('select similarity to 5 from t1;') == \
        'SELECT bql_row_similarity(1, _rowid_, 5, 0, 1, 2) FROM "t1";'
    assert bql2sql('select similarity to 5 with respect to age from t1') == \
        'SELECT bql_row_similarity(1, _rowid_, 5, 1) FROM "t1";'
    assert bql2sql('select similarity to 5 with respect to (age, weight)' +
        ' from t1;') == \
        'SELECT bql_row_similarity(1, _rowid_, 5, 1, 2) FROM "t1";'
    assert bql2sql('select similarity to 5 with respect to (*) from t1;') == \
        'SELECT bql_row_similarity(1, _rowid_, 5, 0, 1, 2) FROM "t1";'
    assert bql2sql('select similarity to 5 with respect to (age, weight)' +
        ' from t1;') == \
        'SELECT bql_row_similarity(1, _rowid_, 5, 1, 2) FROM "t1";'
    assert bql2sql('select dependence probability of age with weight' +
        ' from t1;') == \
        'SELECT bql_column_dependence_probability(1, 1, 2) FROM "t1";'
    with pytest.raises(ValueError):
        # Need both columns fixed.
        bql2sql('select dependence probability with age from t1;')
    with pytest.raises(ValueError):
        # Need both columns fixed.
        bql2sql('select dependence probability from t1;')
    assert bql2sql('select mutual information of age with weight' +
        ' from t1;') == \
        'SELECT bql_column_mutual_information(1, 1, 2, NULL) FROM "t1";'
    assert bql2sql('select mutual information of age with weight' +
        ' using 42 samples from t1;') == \
        'SELECT bql_column_mutual_information(1, 1, 2, 42) FROM "t1";'
    with pytest.raises(ValueError):
        # Need both columns fixed.
        bql2sql('select mutual information with age from t1;')
    with pytest.raises(ValueError):
        # Need both columns fixed.
        bql2sql('select mutual information from t1;')
    with pytest.raises(ValueError):
        # Need both columns fixed.
        bql2sql('select mutual information using 42 samples with age from t1;')
    with pytest.raises(ValueError):
        # Need both columns fixed.
        bql2sql('select mutual information using 42 samples from t1;')
    assert bql2sql('select correlation of age with weight from t1;') == \
        'SELECT bql_column_correlation(1, 1, 2) FROM "t1";'
    with pytest.raises(ValueError):
        # Need both columns fixed.
        bql2sql('select correlation with age from t1;')
    with pytest.raises(ValueError):
        # Need both columns fixed.
        bql2sql('select correlation from t1;')
    assert bql2sql('select infer age conf 0.9 from t1;') == \
        'SELECT bql_infer(1, 1, _rowid_, "age", 0.9) FROM "t1";'

def test_estimate_columns_trivial():
    prefix = 'SELECT name FROM bayesdb_table_column WHERE table_id = 1'
    assert bql2sql('estimate columns from t1;') == \
        prefix + ';'
    assert bql2sql('estimate columns from t1 where' +
            ' (probability of value 42) > 0.5') == \
        prefix + ' AND (bql_column_value_probability(1, colno, 42) > 0.5);'
    # XXX ESTIMATE COLUMNS FROM T1 WHERE PROBABILITY OF 1 > 0.5
    with pytest.raises(ValueError):
        # Must omit column.
        bql2sql('estimate columns from t1 where (probability of x = 0) > 0.5;')
    with pytest.raises(ValueError):
        # Must omit column.  PREDICTIVE PROBABILITY makes no sense
        # without row.
        bql2sql('estimate columns from t1 where' +
            ' predictive probability of x > 0;')
    assert bql2sql('estimate columns from t1 where typicality > 0.5;') == \
        prefix + ' AND (bql_column_typicality(1, colno) > 0.5);'
    with pytest.raises(ValueError):
        # Must omit column.
        bql2sql('estimate columns from t1 where typicality of c > 0.5;')
    with pytest.raises(ValueError):
        # SIMILARITY makes no sense without row.
        bql2sql('estimate columns from t1 where' +
            ' similarity to x with respect to c > 0;')
    assert bql2sql('estimate columns from t1 where' +
            ' dependence probability with age > 0.5;') == \
        prefix + ' AND (bql_column_dependence_probability(1, 1, colno) > 0.5);'
    with pytest.raises(ValueError):
        # Must omit exactly one column.
        bql2sql('estimate columns from t1 where' +
            ' dependence probability of age with weight > 0.5;')
    with pytest.raises(ValueError):
        # Must omit exactly one column.
        bql2sql('estimate columns from t1 where dependence probability > 0.5;')
    assert bql2sql('estimate columns from t1 order by' +
            ' mutual information with age;') == \
        prefix + ' ORDER BY bql_column_mutual_information(1, 1, colno, NULL);'
    assert bql2sql('estimate columns from t1 order by' +
            ' mutual information with age using 42 samples;') == \
        prefix + ' ORDER BY bql_column_mutual_information(1, 1, colno, 42);'
    with pytest.raises(ValueError):
        # Must omit exactly one column.
        bql2sql('estimate columns from t1 order by' +
            ' mutual information of age with weight;')
    with pytest.raises(ValueError):
        # Must omit exactly one column.
        bql2sql('estimate columns from t1 where mutual information > 0.5;')
    with pytest.raises(ValueError):
        # Must omit exactly one column.
        bql2sql('estimate columns from t1 order by' +
            ' mutual information of age with weight using 42 samples;')
    with pytest.raises(ValueError):
        # Must omit exactly one column.
        bql2sql('estimate columns from t1 where' +
            ' mutual information using 42 samples > 0.5;')
    assert bql2sql('estimate columns from t1 order by' +
            ' correlation with age desc;') == \
        prefix + ' ORDER BY bql_column_correlation(1, 1, colno) DESC;'
    with pytest.raises(ValueError):
        # Must omit exactly one column.
        bql2sql('estimate columns from t1 order by' +
            ' correlation of age with weight;')
    with pytest.raises(ValueError):
        # Must omit exactly one column.
        bql2sql('estimate columns from t1 where correlation > 0.5;')
    with pytest.raises(ValueError):
        # Makes no sense.
        bql2sql('estimate columns from t1 where infer age conf 0.9 > 30;')

def test_estimate_pairwise_trivial():
    prefix = 'SELECT 1 AS table_id, c0.name AS name0, c1.name AS name1, '
    infix = ' AS value'
    infix += ' FROM bayesdb_table_column AS c0, bayesdb_table_column AS c1'
    infix += ' WHERE c0.table_id = 1 AND c1.table_id = 1'
    assert bql2sql('estimate pairwise dependence probability from t1;') == \
        prefix + 'bql_column_dependence_probability(1, c0.colno, c1.colno)' + \
        infix + ';'
    with pytest.raises(ValueError):
        # PROBABILITY OF = is a row function.
        bql2sql('estimate pairwise mutual information from t1 where'
            ' (probability of x = 0) > 0.5;')
    with pytest.raises(ValueError):
        # PROBABILITY OF = is a row function.
        bql2sql('estimate pairwise mutual information using 42 samples'
            ' from t1 where (probability of x = 0) > 0.5;')
    with pytest.raises(ValueError):
        # PROBABILITY OF VALUE is 1-column.
        bql2sql('estimate pairwise correlation from t1 where' +
            ' (probability of value 0) > 0.5;')
    with pytest.raises(ValueError):
        # PREDICTIVE PROBABILITY OF is a row function.
        bql2sql('estimate pairwise dependence probability from t1 where' +
            ' predictive probability of x > 0.5;')
    with pytest.raises(ValueError):
        # TYPICALITY OF is a row function.
        bql2sql('estimate pairwise mutual information from t1' +
            ' where typicality of x > 0.5;')
    with pytest.raises(ValueError):
        # TYPICALITY OF is a row function.
        bql2sql('estimate pairwise mutual information using 42 samples from t1'
            ' where typicality of x > 0.5;')
    with pytest.raises(ValueError):
        # TYPICALITY is 1-column.
        bql2sql('estimate pairwise correlation from t1 where typicality > 0.5;')
    with pytest.raises(ValueError):
        # Must omit both columns.
        bql2sql('estimate pairwise dependence probability from t1 where' +
            ' dependence probability of age with weight > 0.5;')
    with pytest.raises(ValueError):
        # Must omit both columns.
        bql2sql('estimate pairwise mutual information from t1 where' +
            ' dependence probability with weight > 0.5;')
    with pytest.raises(ValueError):
        # Must omit both columns.
        bql2sql('estimate pairwise mutual information using 42 samples'
            ' from t1 where dependence probability with weight > 0.5;')
    assert bql2sql('estimate pairwise correlation from t1 where' +
            ' dependence probability > 0.5;') == \
        prefix + 'bql_column_correlation(1, c0.colno, c1.colno)' + \
        infix + ' AND' + \
        ' (bql_column_dependence_probability(1, c0.colno, c1.colno) > 0.5);'
    with pytest.raises(ValueError):
        # Must omit both columns.
        bql2sql('estimate pairwise dependence probability from t1 where' +
            ' mutual information of age with weight > 0.5;')
    with pytest.raises(ValueError):
        # Must omit both columns.
        bql2sql('estimate pairwise dependence probability from t1 where' +
            ' mutual information of age using 42 samples with weight > 0.5;')
    with pytest.raises(ValueError):
        # Must omit both columns.
        bql2sql('estimate pairwise mutual information from t1 where' +
            ' mutual information with weight > 0.5;')
    with pytest.raises(ValueError):
        # Must omit both columns.
        bql2sql('estimate pairwise mutual information using 42 samples from t1'
            ' where mutual information with weight using 42 samples > 0.5;')
    assert bql2sql('estimate pairwise correlation from t1 where' +
            ' mutual information > 0.5;') == \
        prefix + 'bql_column_correlation(1, c0.colno, c1.colno)' + \
        infix + ' AND' + \
        ' (bql_column_mutual_information(1, c0.colno, c1.colno, NULL) > 0.5);'
    assert bql2sql('estimate pairwise correlation from t1 where' +
            ' mutual information using 42 samples > 0.5;') == \
        prefix + 'bql_column_correlation(1, c0.colno, c1.colno)' + \
        infix + ' AND' + \
        ' (bql_column_mutual_information(1, c0.colno, c1.colno, 42) > 0.5);'
    with pytest.raises(ValueError):
        # Must omit both columns.
        bql2sql('estimate pairwise dependence probability from t1 where' +
            ' correlation of age with weight > 0.5;')
    with pytest.raises(ValueError):
        # Must omit both columns.
        bql2sql('estimate pairwise mutual information from t1 where' +
            ' correlation with weight > 0.5;')
    with pytest.raises(ValueError):
        # Must omit both columns.
        bql2sql('estimate pairwise mutual information using 42 samples from t1'
            ' where correlation with weight > 0.5;')
    assert bql2sql('estimate pairwise correlation from t1 where' +
            ' correlation > 0.5;') == \
        prefix + 'bql_column_correlation(1, c0.colno, c1.colno)' + \
        infix + ' AND' + \
        ' (bql_column_correlation(1, c0.colno, c1.colno) > 0.5);'
    with pytest.raises(ValueError):
        # Makes no sense.
        bql2sql('estimate pairwise dependence probability columns from t1' +
            ' where infer age conf 0.9 > 30;')

def test_estimate_pairwise_row():
    prefix = 'SELECT r0._rowid_, r1._rowid_'
    infix = ' FROM t1 AS r0, t1 AS r1'
    assert bql2sql('estimate pairwise row similarity from t1;') == \
        prefix + ', bql_row_similarity(1, r0._rowid_, r1._rowid_, 0, 1, 2)' + \
        infix + ';'
    assert bql2sql('estimate pairwise row similarity with respect to age' +
            ' from t1;') == \
        prefix + ', bql_row_similarity(1, r0._rowid_, r1._rowid_, 1)' + \
        infix + ';'
    with pytest.raises(ValueError):
        # INFER is a 1-row function.
        bql2sql('estimate pairwise row infer age conf 0.9 from t1;')

def test_trivial_commands():
    with test_csv.bayesdb_csv_file(test_csv.csv_data) as (bdb, fname):
        # XXX Query parameters!
        bdb.execute("create btable t from '%s'" % (fname,))
        bdb.execute("create btable if not exists t from '%s'" % (fname,))
        bdb.execute('initialize 2 models for t')
        bdb.execute('initialize 1 model if not exists for t')
        bdb.execute('initialize 2 models if not exists for t')
        bdb.execute('analyze t model 0 for 1 iteration wait')
        bdb.execute('analyze t models 0-1 for 1 iteration wait')
        bdb.execute('analyze t models 0,1 for 1 iteration wait')
        bdb.execute('analyze t for 1 iteration wait')
        bdb.execute('select * from t')
        bdb.execute('select * from T')
        bdb.execute('estimate pairwise row similarity from t')
        bdb.execute('select infer age conf 0.9 from t')
        bdb.execute('select infer AGE conf 0.9 from T')
        bdb.execute('select infer aGe conf 0.9 from T')
        with pytest.raises(AssertionError):
            # XXX Assertion error is a bug here, please fix.
            bdb.execute('select infer agee conf 0.9 from t')

def test_trivial_deadline():
    with test_core.t1() as (bdb, _table_id):
        bdb.execute('initialize 1 model for t1')
        bdb.execute('analyze t1 for 1 second wait')

def test_parametrized():
    assert bql2sqlparam('select * from t where id = ?') == \
        'SELECT * FROM "t" WHERE ("id" = ?1);'
    assert bql2sqlparam('select * from t where id = :foo') == \
        'SELECT * FROM "t" WHERE ("id" = ?1);'
    assert bql2sqlparam('select * from t where id = $foo') == \
        'SELECT * FROM "t" WHERE ("id" = ?1);'
    assert bql2sqlparam('select * from t where id = @foo') == \
        'SELECT * FROM "t" WHERE ("id" = ?1);'
    assert bql2sqlparam('select * from t where id = ?123') == \
        'SELECT * FROM "t" WHERE ("id" = ?1);'
    assert bql2sqlparam('select * from t where a = $foo and b = ?1;') == \
        'SELECT * FROM "t" WHERE (("a" = ?1) AND ("b" = ?1));'
    assert bql2sqlparam('select * from t' +
            ' where a = ?123 and b = :foo and c = ?124') == \
        'SELECT * FROM "t" WHERE' + \
        ' ((("a" = ?1) AND ("b" = ?2)) AND ("c" = ?2));'
    with test_csv.bayesdb_csv_file(test_csv.csv_data) as (bdb, fname):
        bdb.execute("create btable t from '%s'" % (fname,))
        assert bql_execute(bdb, 'select * from t where height > ?', (70,)) == \
            [
                ('41', 'M', '65600', '72', 'marketing', '4'),
                ('30', 'M', '70000', '73', 'sales', '4'),
                ('30', 'F', '81000', '73', 'engineering', '3'),
            ]
        assert bql_execute(bdb, 'select * from t where height > ?123',
                (0,)*122 + (70,)) == \
            [
                ('41', 'M', '65600', '72', 'marketing', '4'),
                ('30', 'M', '70000', '73', 'sales', '4'),
                ('30', 'F', '81000', '73', 'engineering', '3'),
            ]
        assert bql_execute(bdb, 'select age from t where division = :division',
                {':division': 'sales'}) == \
            [('34',), ('30',)]
        assert bql_execute(bdb, 'select division from t' +
                    ' where age < @age and rank > ?;',
                (40, 4)) == \
            [('accounting',)]
        assert bql_execute(bdb, 'select division from t' +
                    ' where age < @age and rank > :rank;',
                {':RANK': 4, '@aGe': 40}) == \
            [('accounting',)]
        with pytest.raises(ValueError):
            bdb.execute('select * from t where age < ? and rank > :r',
                {':r': 4})
        def sqltraced_execute(query, *args):
            sql = []
            def trace(string, _bindings):
                sql.append(' '.join(string.split()))
            bdb.sql_trace(trace)
            bdb.execute(query, *args)
            bdb.sql_untrace(trace)
            return sql
        bdb.execute('initialize 1 model for t;')
        bdb.execute('analyze t for 1 iteration wait;')
        assert sqltraced_execute('select similarity to 1 with respect to' +
                ' (estimate columns from t limit 1) from t;') == [
            'SELECT id FROM bayesdb_table WHERE name = ?',
            'SELECT id FROM bayesdb_table WHERE name = ?',
            # *** ESTIMATE COLUMNS:
            'SELECT name FROM bayesdb_table_column WHERE table_id = 1' +
                ' LIMIT 1',
            'SELECT colno FROM bayesdb_table_column WHERE table_id = ?' +
                ' AND name = ?',
            # *** SELECT SIMILARITY TO 1:
            'SELECT bql_row_similarity(1, _rowid_, 1, 0) FROM "t"',
            'SELECT metamodel_id FROM bayesdb_table WHERE id = ?',
            'SELECT metadata FROM bayesdb_table WHERE id = ?',
            'SELECT count(*) FROM bayesdb_model WHERE table_id = ?',
            'SELECT theta FROM bayesdb_model' +
                ' WHERE table_id = ? AND modelno = ?',
            'SELECT count(*) FROM bayesdb_model WHERE table_id = ?',
        ]
        assert sqltraced_execute('select similarity to 1 with respect to' +
                ' (estimate columns from t limit ?) from t;', (1,)) == [
            'SELECT id FROM bayesdb_table WHERE name = ?',
            'SELECT id FROM bayesdb_table WHERE name = ?',
            # *** ESTIMATE COLUMNS:
            'SELECT name FROM bayesdb_table_column WHERE table_id = 1' +
                ' LIMIT ?1',
            'SELECT colno FROM bayesdb_table_column WHERE table_id = ?' +
                ' AND name = ?',
            # *** SELECT SIMILARITY TO 1:
            'SELECT bql_row_similarity(1, _rowid_, 1, 0) FROM "t"',
            'SELECT metamodel_id FROM bayesdb_table WHERE id = ?',
            'SELECT metadata FROM bayesdb_table WHERE id = ?',
            'SELECT count(*) FROM bayesdb_model WHERE table_id = ?',
            'SELECT theta FROM bayesdb_model' +
                ' WHERE table_id = ? AND modelno = ?',
            'SELECT count(*) FROM bayesdb_model WHERE table_id = ?',
        ]

def test_createtab():
    with test_csv.bayesdb_csv_file(test_csv.csv_data) as (bdb, fname):
        bdb.execute("create btable t from '%s'" % (fname,))
        bdb.execute("create table u as select * from t where gender = 'F'")
        assert bql_execute(bdb, 'select * from u') == [
            ('23', 'F', '81000', '67', 'data science', '3'),
            ('36', 'F', '96000', '70', 'management', '2'),
            ('30', 'F', '81000', '73', 'engineering', '3'),
        ]
