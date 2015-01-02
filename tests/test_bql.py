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

import bayeslite.ast as ast
import bayeslite.bql as bql
import bayeslite.parse as parse

import test_smoke

def bql2sql(string):
    with test_smoke.t1() as (bdb, _table_id):
        phrases = parse.parse_bql_string(string)
        out = StringIO.StringIO()
        for phrase in phrases:
            assert ast.is_query(phrase)
            bql.compile_query(bdb, phrase, out)
            out.write(';')
        return out.getvalue()

def test_select_trivial():
    assert bql2sql('select null;') == 'select null;'
    assert bql2sql("select 'x';") == "select 'x';"
    assert bql2sql("select 'x''y';") == "select 'x''y';"
    assert bql2sql('select "x";') == 'select "x";'
    assert bql2sql('select "x""y";') == 'select "x""y";'
    assert bql2sql('select 0;') == 'select 0;'
    assert bql2sql('select 0.;') == 'select 0.0;'
    assert bql2sql('select .0;') == 'select 0.0;'
    assert bql2sql('select 0.0;') == 'select 0.0;'
    assert bql2sql('select 1e0;') == 'select 1.0;'
    assert bql2sql('select 1e+1;') == 'select 10.0;'
    assert bql2sql('select 1e-1;') == 'select 0.1;'
    assert bql2sql('select .1e0;') == 'select 0.1;'
    assert bql2sql('select 1.e10;') == 'select 10000000000.0;'
    assert bql2sql('select all 0;') == 'select 0;'
    assert bql2sql('select distinct 0;') == 'select distinct 0;'
    assert bql2sql('select 0 as z;') == 'select 0 as "z";'
    assert bql2sql('select * from t;') == 'select * from "t";'
    assert bql2sql('select t.* from t;') == 'select "t".* from "t";'
    assert bql2sql('select c from t;') == 'select "c" from "t";'
    assert bql2sql('select c as d from t;') == 'select "c" as "d" from "t";'
    assert bql2sql('select t.c as d from t;') == \
        'select "t"."c" as "d" from "t";'
    assert bql2sql('select t.c as d, p as q, x from t;') == \
        'select "t"."c" as "d", "p" as "q", "x" from "t";'
    assert bql2sql('select * from t, u;') == 'select * from "t", "u";'
    assert bql2sql('select * from t as u;') == 'select * from "t" as "u";'
    assert bql2sql('select * where x;') == 'select * where "x";'
    assert bql2sql('select * from t where x;') == \
        'select * from "t" where "x";'
    assert bql2sql('select * group by x;') == 'select * group by "x";'
    assert bql2sql('select * from t where x group by y;') == \
        'select * from "t" where "x" group by "y";'
    assert bql2sql('select * from t where x group by y, z;') == \
        'select * from "t" where "x" group by "y", "z";'
    assert bql2sql('select * order by x;') == 'select * order by "x";'
    assert bql2sql('select * order by x asc;') == 'select * order by "x";'
    assert bql2sql('select * order by x desc;') == \
        'select * order by "x" desc;'
    assert bql2sql('select * order by x, y;') == 'select * order by "x", "y";'
    assert bql2sql('select * order by x desc, y;') == \
        'select * order by "x" desc, "y";'
    assert bql2sql('select * order by x, y asc;') == \
        'select * order by "x", "y";'
    assert bql2sql('select * limit 32;') == 'select * limit 32;'
    assert bql2sql('select * limit 32 offset 16;') == \
        'select * limit 32 offset 16;'
    assert bql2sql('select * limit 16, 32;') == 'select * limit 32 offset 16;'
    assert bql2sql('select (select0);') == 'select "select0";'
    assert bql2sql('select (select 0);') == 'select (select 0);'
    assert bql2sql('select f(f(), f(x), y);') == \
        'select "f"("f"(), "f"("x"), "y");'
    assert bql2sql('select a and b or c or not d is e is not f like j;') == \
        'select ((("a" AND "b") OR "c") OR' \
        + ' (NOT ((("d" IS "e") IS NOT "f") LIKE "j")));'
    assert bql2sql('select a like b not like c like d escape e;') == \
        'select ((("a" LIKE "b") NOT LIKE "c") LIKE "d" ESCAPE "e");'
    assert bql2sql('select a like b escape c glob d not glob e;') == \
        'select ((("a" LIKE "b" ESCAPE "c") GLOB "d") NOT GLOB "e");'
    assert bql2sql('select a not glob b glob c escape d;') == \
        'select (("a" NOT GLOB "b") GLOB "c" ESCAPE "d");'
    assert bql2sql('select a glob b escape c regexp e not regexp f;') == \
        'select ((("a" GLOB "b" ESCAPE "c") REGEXP "e") NOT REGEXP "f");'
    assert bql2sql('select a not regexp b regexp c escape d;') == \
        'select (("a" NOT REGEXP "b") REGEXP "c" ESCAPE "d");'
    assert bql2sql('select a regexp b escape c not regexp d escape e;') == \
        'select (("a" REGEXP "b" ESCAPE "c") NOT REGEXP "d" ESCAPE "e");'
    assert bql2sql('select a not regexp b escape c match e not match f;') == \
        'select ((("a" NOT REGEXP "b" ESCAPE "c") MATCH "e") NOT MATCH "f");'
    assert bql2sql('select a not match b match c escape d;') == \
        'select (("a" NOT MATCH "b") MATCH "c" ESCAPE "d");'
    assert bql2sql('select a match b escape c not match d escape e;') == \
        'select (("a" MATCH "b" ESCAPE "c") NOT MATCH "d" ESCAPE "e");'
    assert bql2sql('select a not match b escape c between d and e;') == \
        'select (("a" NOT MATCH "b" ESCAPE "c") BETWEEN "d" AND "e");'
    assert bql2sql('select a between b and c and d;') == \
        'select (("a" BETWEEN "b" AND "c") AND "d");'
    assert bql2sql('select a like b like c escape d between e and f;') == \
        'select ((("a" LIKE "b") LIKE "c" ESCAPE "d") BETWEEN "e" AND "f");'
    assert bql2sql('select a between b and c not between d and e;') == \
        'select (("a" BETWEEN "b" AND "c") NOT BETWEEN "d" AND "e");'
    assert bql2sql('select a not between b and c in (select f);') == \
        'select (("a" NOT BETWEEN "b" AND "c") IN (select "f"));'
    assert bql2sql('select a in (select b) and c not in (select d);') == \
        'select (("a" IN (select "b")) AND ("c" NOT IN (select "d")));'
    assert bql2sql('select a in (select b) isnull notnull!=c<>d<e<=f>g;') == \
        'select ((((("a" IN (select "b")) ISNULL) NOTNULL) != "c") !=' \
        + ' ((("d" < "e") <= "f") > "g"));'
    assert bql2sql('select a>b>=c<<d>>e&f|g+h-i*j/k;') == \
        'select (("a" > "b") >= (((("c" << "d") >> "e") & "f") |' \
        + ' (("g" + "h") - (("i" * "j") / "k"))));'
    assert bql2sql('select a/b||~~c collate d collate\'e\'||1;') == \
        'select ("a" / (("b" || (((~ (~ "c")) COLLATE "d") COLLATE "e"))' \
        + ' || 1));'
    assert bql2sql('select cast(f(x) as binary blob);') == \
        'select CAST("f"("x") AS "binary" "blob");'
    assert bql2sql('select cast(42 as varint(73));') == \
        'select CAST(42 AS "varint"(73));'
    assert bql2sql('select cast(f(x, y, z) as varchar(12 ,34));') == \
        'select CAST("f"("x", "y", "z") AS "varchar"(12, 34));'
    assert bql2sql('select exists (select a) and not exists (select b);') == \
        'select ((EXISTS (select "a")) AND (NOT (EXISTS (select "b"))));'

def test_select_bql():
    with pytest.raises(ValueError):
        bql2sql('select predictive probability of weight;')
    with pytest.raises(ValueError):
        bql2sql('select predictive probability of weight from t1, t1;')
    assert bql2sql('select predictive probability of weight from t1;') == \
        'select row_column_predictive_probability(1, rowid, 3) from "t1";'
    assert bql2sql('select label, predictive probability of weight from t1;') \
        == \
        'select "label", row_column_predictive_probability(1, rowid, 3)' \
        + ' from "t1";'
    assert bql2sql('select predictive probability of weight, label from t1;') \
        == \
        'select row_column_predictive_probability(1, rowid, 3), "label"' \
        + ' from "t1";'
    assert bql2sql('select probability of weight = 20 from t1;') == \
        'select column_value_probability(1, 3, 20) from "t1";'
    assert bql2sql('select typicality from t1;') == \
        'select row_typicality(1, rowid) from "t1";'
    assert bql2sql('select typicality of age from t1;') == \
        'select column_typicality(1, 2) from "t1";'
    assert bql2sql('select similarity to 5 with respect to (age, weight)' +
        ' from t1;') == \
        'select row_similarity(1, rowid, 5, 2, 3) from "t1";'
    assert bql2sql('select dependence probability of age with weight' +
        ' from t1;') == \
        'select column_dependence_probability(1, 2, 3) from "t1";'
