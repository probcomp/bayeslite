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

def bql2sql(string):
    with test_core.t1() as (bdb, _table_id):
        phrases = parse.parse_bql_string(string)
        out = StringIO.StringIO()
        for phrase in phrases:
            assert ast.is_query(phrase)
            bql.compile_query(bdb, phrase, out)
            out.write(';')
        return out.getvalue()

def bql_execute(bdb, string):
    phrases = parse.parse_bql_string(string)
    for phrase in phrases:
        bql.execute_phrase(bdb, phrase)

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
        'SELECT row_column_predictive_probability(1, rowid, 3) FROM "t1";'
    assert bql2sql('select label, predictive probability of weight from t1;') \
        == \
        'SELECT "label", row_column_predictive_probability(1, rowid, 3)' \
        + ' FROM "t1";'
    assert bql2sql('select predictive probability of weight, label from t1;') \
        == \
        'SELECT row_column_predictive_probability(1, rowid, 3), "label"' \
        + ' FROM "t1";'
    assert bql2sql('select predictive probability of weight + 1 from t1;') == \
        'SELECT (row_column_predictive_probability(1, rowid, 3) + 1)' \
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
    with pytest.raises(Exception): # XXX Use a specific parse error.
        # Need a column.
        bql2sql('select predictive probability from t1;')
    assert bql2sql('select probability of weight = 20 from t1;') == \
        'SELECT column_value_probability(1, 3, 20) FROM "t1";'
    assert bql2sql('select probability of weight = (c + 1) from t1;') == \
        'SELECT column_value_probability(1, 3, ("c" + 1)) FROM "t1";'
    assert bql2sql('select probability of weight = f(c) from t1;') == \
        'SELECT column_value_probability(1, 3, "f"("c")) FROM "t1";'
    assert bql2sql('select typicality from t1;') == \
        'SELECT row_typicality(1, rowid) FROM "t1";'
    assert bql2sql('select typicality of age from t1;') == \
        'SELECT column_typicality(1, 2) FROM "t1";'
    assert bql2sql('select similarity to 5 from t1;') == \
        'SELECT row_similarity(1, rowid, 5, 0, 1, 2, 3) FROM "t1";'
    assert bql2sql('select similarity to 5 with respect to age from t1') == \
        'SELECT row_similarity(1, rowid, 5, 2) FROM "t1";'
    assert bql2sql('select similarity to 5 with respect to (age, weight)' +
        ' from t1;') == \
        'SELECT row_similarity(1, rowid, 5, 2, 3) FROM "t1";'
    assert bql2sql('select similarity to 5 with respect to (*) from t1;') == \
        'SELECT row_similarity(1, rowid, 5, 0, 1, 2, 3) FROM "t1";'
    assert bql2sql('select similarity to 5 with respect to (age, weight)' +
        ' from t1;') == \
        'SELECT row_similarity(1, rowid, 5, 2, 3) FROM "t1";'
    assert bql2sql('select dependence probability of age with weight' +
        ' from t1;') == \
        'SELECT column_dependence_probability(1, 2, 3) FROM "t1";'
    with pytest.raises(ValueError):
        # Need both columns fixed.
        bql2sql('select dependence probability with age from t1;')
    with pytest.raises(ValueError):
        # Need both columns fixed.
        bql2sql('select dependence probability from t1;')
    assert bql2sql('select mutual information of age with weight' +
        ' from t1;') == \
        'SELECT column_mutual_information(1, 2, 3) FROM "t1";'
    with pytest.raises(ValueError):
        # Need both columns fixed.
        bql2sql('select mutual information with age from t1;')
    with pytest.raises(ValueError):
        # Need both columns fixed.
        bql2sql('select mutual information from t1;')
    assert bql2sql('select correlation of age with weight from t1;') == \
        'SELECT column_correlation(1, 2, 3) FROM "t1";'
    with pytest.raises(ValueError):
        # Need both columns fixed.
        bql2sql('select correlation with age from t1;')
    with pytest.raises(ValueError):
        # Need both columns fixed.
        bql2sql('select correlation from t1;')

def test_estimate_columns_trivial():
    prefix = 'SELECT name FROM bayesdb_table_column WHERE table_id = 1'
    assert bql2sql('estimate columns from t1;') == \
        prefix + ';'
    assert bql2sql('estimate columns from t1 where' +
            ' (probability of value 42) > 0.5') == \
        prefix + ' AND (column_value_probability(1, colno, 42) > 0.5);'
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
        prefix + ' AND (column_typicality(1, colno) > 0.5);'
    with pytest.raises(ValueError):
        # Must omit column.
        bql2sql('estimate columns from t1 where typicality of c > 0.5;')
    with pytest.raises(ValueError):
        # SIMILARITY makes no sense without row.
        bql2sql('estimate columns from t1 where' +
            ' similarity to x with respect to c > 0;')
    assert bql2sql('estimate columns from t1 where' +
            ' dependence probability with age > 0.5;') == \
        prefix + ' AND (column_dependence_probability(1, 2, colno) > 0.5);'
    with pytest.raises(ValueError):
        # Must omit exactly one column.
        bql2sql('estimate columns from t1 where' +
            ' dependence probability of age with weight > 0.5;')
    with pytest.raises(ValueError):
        # Must omit exactly one column.
        bql2sql('estimate columns from t1 where dependence probability > 0.5;')
    assert bql2sql('estimate columns from t1 order by' +
            ' mutual information with age;') == \
        prefix + ' ORDER BY column_mutual_information(1, 2, colno);'
    with pytest.raises(ValueError):
        # Must omit exactly one column.
        bql2sql('estimate columns from t1 order by' +
            ' mutual information of age with weight;')
    with pytest.raises(ValueError):
        # Must omit exactly one column.
        bql2sql('estimate columns from t1 where mutual information > 0.5;')
    assert bql2sql('estimate columns from t1 order by' +
            ' correlation with age desc;') == \
        prefix + ' ORDER BY column_correlation(1, 2, colno) DESC;'
    with pytest.raises(ValueError):
        # Must omit exactly one column.
        bql2sql('estimate columns from t1 order by' +
            ' correlation of age with weight;')
    with pytest.raises(ValueError):
        # Must omit exactly one column.
        bql2sql('estimate columns from t1 where correlation > 0.5;')

def test_estimate_pairwise_trivial():
    prefix = 'SELECT c0.name, c1.name'
    prefix += ' FROM bayesdb_table_column AS c0, bayesdb_table_column AS c1'
    prefix += ' WHERE c0.table_id = 1 AND c1.table_id = 1'
    assert bql2sql('estimate pairwise from t1;') == prefix + ';'
    with pytest.raises(ValueError):
        # PROBABILITY OF = is a row function.
        bql2sql('estimate pairwise from t1 where'
            ' (probability of x = 0) > 0.5;')
    with pytest.raises(ValueError):
        # PROBABILITY OF VALUE is 1-column.
        bql2sql('estimate pairwise from t1 where' +
            ' (probability of value 0) > 0.5;')
    with pytest.raises(ValueError):
        # PREDICTIVE PROBABILITY OF is a row function.
        bql2sql('estimate pairwise from t1 where' +
            ' predictive probability of x > 0.5;')
    with pytest.raises(ValueError):
        # TYPICALITY OF is a row function.
        bql2sql('estimate pairwise from t1 where typicality of x > 0.5;')
    with pytest.raises(ValueError):
        # TYPICALITY is 1-column.
        bql2sql('estimate pairwise from t1 where typicality > 0.5;')
    with pytest.raises(ValueError):
        # Must omit both columns.
        bql2sql('estimate pairwise from t1 where' +
            ' dependence probability of age with weight > 0.5;')
    with pytest.raises(ValueError):
        # Must omit both columns.
        bql2sql('estimate pairwise from t1 where' +
            ' dependence probability with weight > 0.5;')
    assert bql2sql('estimate pairwise from t1 where' +
            ' dependence probability > 0.5;') == \
        prefix + ' AND' + \
        ' (column_dependence_probability(1, c0.colno, c1.colno) > 0.5);'
    with pytest.raises(ValueError):
        # Must omit both columns.
        bql2sql('estimate pairwise from t1 where' +
            ' mutual information of age with weight > 0.5;')
    with pytest.raises(ValueError):
        # Must omit both columns.
        bql2sql('estimate pairwise from t1 where' +
            ' mutual information with weight > 0.5;')
    assert bql2sql('estimate pairwise from t1 where' +
            ' mutual information > 0.5;') == \
        prefix + ' AND' + \
        ' (column_mutual_information(1, c0.colno, c1.colno) > 0.5);'
    with pytest.raises(ValueError):
        # Must omit both columns.
        bql2sql('estimate pairwise from t1 where' +
            ' correlation of age with weight > 0.5;')
    with pytest.raises(ValueError):
        # Must omit both columns.
        bql2sql('estimate pairwise from t1 where' +
            ' correlation with weight > 0.5;')
    assert bql2sql('estimate pairwise from t1 where' +
            ' correlation > 0.5;') == \
        prefix + ' AND' + \
        ' (column_correlation(1, c0.colno, c1.colno) > 0.5);'

def test_estimate_pairwise_row():
    prefix = 'SELECT r0.rowid, r1.rowid'
    infix = ' FROM t1 AS r0, t1 AS r1'
    assert bql2sql('estimate pairwise row similarity from t1;') == \
        prefix + ', row_similarity(1, r0.rowid, r1.rowid, 0, 1, 2, 3)' + \
        infix + ';'
    assert bql2sql('estimate pairwise row similarity with respect to age' +
            ' from t1;') == \
        prefix + ', row_similarity(1, r0.rowid, r1.rowid, 2)' + infix + ';'

def test_trivial_commands():
    with test_core.bayesdb_csv(test_core.csv_data) as (bdb, fname):
        # XXX Query parameters!
        bql_execute(bdb, "create btable t from '%s'" % (fname,))
        bql_execute(bdb, "create btable if not exists t from '%s'" % (fname,))
        bql_execute(bdb, 'initialize 2 models for t')
        bql_execute(bdb, 'initialize 1 model if not exists for t')
        bql_execute(bdb, 'initialize 2 models if not exists for t')
        bql_execute(bdb, 'analyze t model 0 for 1 iteration wait')
        bql_execute(bdb, 'analyze t models 0-1 for 1 iteration wait')
        bql_execute(bdb, 'analyze t models 0,1 for 1 iteration wait')
        bql_execute(bdb, 'analyze t for 1 iteration wait')
        bql_execute(bdb, 'select * from t')
        bql_execute(bdb, 'estimate pairwise row similarity from t')
