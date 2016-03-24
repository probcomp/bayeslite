# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2016, MIT Probabilistic Computing Project
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
import apsw
import pytest

import bayeslite
import bayeslite.ast as ast
import bayeslite.compiler as compiler
import bayeslite.core as core
import bayeslite.guess as guess
import bayeslite.parse as parse
import bayeslite.metamodels.troll_rng as troll

import test_core
import test_csv

from stochastic import stochastic

def bql2sql(string, setup=None):
    with test_core.t1() as (bdb, _table_id):
        if setup is not None:
            setup(bdb)
        phrases = parse.parse_bql_string(string)
        out = compiler.Output(0, {}, ())
        for phrase in phrases:
            assert ast.is_query(phrase)
            compiler.compile_query(bdb, phrase, out)
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
                out = compiler.Output(phrase.n_numpar, phrase.nampar_map,
                    bindings)
                phrase = phrase.phrase
            else:
                out = StringIO.StringIO()
            assert ast.is_query(phrase)
            compiler.compile_query(bdb, phrase, out)
            # XXX Do something about the parameters.
            out0.write(out.getvalue())
            out0.write(';')
        return out0.getvalue()

def bql_execute(bdb, string, bindings=()):
    return map(tuple, bdb.execute(string, bindings))

def empty(cursor):
    assert cursor is not None
    assert cursor.description is not None
    assert len(cursor.description) == 0
    with pytest.raises(StopIteration):
        cursor.next()

@stochastic(max_runs=2, min_passes=1)
def test_conditional_probability(seed):
    with test_core.t1(seed=seed) as (bdb, _generator_id):
        bdb.execute('''create generator t1_cond_prob_cc for t1 using
                       crosscat(age numerical, weight numerical,
                           dependent(age, weight));''')
        bdb.execute('initialize 1 model for t1_cond_prob_cc')
        bdb.execute('analyze t1_cond_prob_cc for 1 iteration wait')
        q0 = 'estimate probability of age = 8 by t1_cond_prob_cc'
        q1 = 'estimate probability of age = 8 given () by t1_cond_prob_cc'
        age_is_8 = bdb.execute(q0).fetchvalue()
        assert age_is_8 == bdb.execute(q1).fetchvalue()
        q2 = '''estimate probability of age = 8 given (weight = 16)
                by t1_cond_prob_cc'''
        age_is_8_given_weight_is_16 = bdb.execute(q2).fetchvalue()
        assert age_is_8 < age_is_8_given_weight_is_16

        probs = bdb.execute(
            'estimate probability of value 8 given (weight = 16)'
            ' from columns of t1_cond_prob_cc').fetchall()
        assert [(age_is_8_given_weight_is_16,), (0,)] == probs

@stochastic(max_runs=2, min_passes=1)
def test_joint_probability(seed):
    with test_core.t1(seed=seed) as (bdb, _generator_id):
        bdb.execute('initialize 10 models for t1_cc')
        bdb.execute('analyze t1_cc for 10 iterations wait')
        q0 = 'estimate probability of age = 8 by t1_cc'
        q1 = 'estimate probability of (age = 8) by t1_cc'
        assert bdb.execute(q0).fetchvalue() == bdb.execute(q1).fetchvalue()
        q1 = 'estimate probability of (age = 8) given () by t1_cc'
        assert bdb.execute(q0).fetchvalue() == bdb.execute(q1).fetchvalue()
        q2 = 'estimate probability of age = 8 given (weight = 16) by t1_cc'
        assert bdb.execute(q0).fetchvalue() < bdb.execute(q2).fetchvalue()
        q0 = 'estimate probability of age = 8 by t1_cc'
        q1 = 'estimate probability of (age = 8, weight = 16) by t1_cc'
        assert bdb.execute(q1).fetchvalue() < bdb.execute(q0).fetchvalue()
        q2 = 'estimate probability of (age = 8, weight = 16)' \
            " given (label = 'mumble') by t1_cc"
        assert bdb.execute(q1).fetchvalue() < bdb.execute(q2).fetchvalue()

def test_badbql():
    with test_core.t1() as (bdb, _generator_id):
        with pytest.raises(ValueError):
            bdb.execute('')
        with pytest.raises(ValueError):
            bdb.execute(';')
        with pytest.raises(ValueError):
            bdb.execute('select 0; select 1')

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
    assert bql2sql('select -1e+1;') == 'SELECT (- 10.0);'
    assert bql2sql('select +1e-1;') == 'SELECT (+ 0.1);'
    assert bql2sql('select SQRT(1-EXP(-2*value)) FROM bm_mi;') == \
        'SELECT "SQRT"((1 - "EXP"(((- 2) * "value")))) FROM "bm_mi";'
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
    assert bql2sql('select * from t where x group by y having sum(z) < 1') == \
        'SELECT * FROM "t" WHERE "x" GROUP BY "y" HAVING ("sum"("z") < 1);'
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

def test_estimate_bql():
    assert bql2sql('estimate predictive probability of weight'
            ' from t1_cc;') == \
        'SELECT bql_row_column_predictive_probability(1, NULL, _rowid_, 3)' \
            ' FROM "t1";'
    assert bql2sql('estimate label, predictive probability of weight'
            ' from t1_cc;') \
        == \
        'SELECT "label",' \
            ' bql_row_column_predictive_probability(1, NULL, _rowid_, 3)' \
            ' FROM "t1";'
    assert bql2sql('estimate predictive probability of weight, label'
            ' from t1_cc;') \
        == \
        'SELECT bql_row_column_predictive_probability(1, NULL, _rowid_, 3),' \
            ' "label"' \
            ' FROM "t1";'
    assert bql2sql('estimate predictive probability of weight + 1'
            ' from t1_cc;') == \
        'SELECT (bql_row_column_predictive_probability(1, NULL, _rowid_, 3)' \
            ' + 1)' \
            ' FROM "t1";'
    with pytest.raises(parse.BQLParseError):
        # Need a table.
        bql2sql('estimate predictive probability of weight;')
    with pytest.raises(parse.BQLParseError):
        # Need at most one generator.
        bql2sql('estimate predictive probability of weight from t1_cc, t1_cc;')
    with pytest.raises(parse.BQLParseError):
        # Need a generator name, not a subquery.
        bql2sql('estimate predictive probability of weight from (select 0);')
    with pytest.raises(parse.BQLParseError):
        # Need a column.
        bql2sql('estimate predictive probability from t1_cc;')
    assert bql2sql('estimate probability of weight = 20 from t1_cc;') == \
        'SELECT bql_pdf_joint(1, NULL, 3, 20) FROM "t1";'
    assert bql2sql('estimate probability of weight = 20 given (age = 8)'
            'from t1_cc;') == \
        'SELECT bql_pdf_joint(1, NULL, 3, 20, -1, 2, 8) FROM "t1";'
    assert bql2sql('estimate probability of (weight = 20, age = 8)'
            ' from t1_cc;') == \
        'SELECT bql_pdf_joint(1, NULL, 3, 20, 2, 8) FROM "t1";'
    assert bql2sql('estimate probability of (weight = 20, age = 8)'
            " given (label = 'mumble') from t1_cc;") == \
        "SELECT bql_pdf_joint(1, NULL, 3, 20, 2, 8, -1, 1, 'mumble')" \
            ' FROM "t1";'
    assert bql2sql('estimate probability of weight = (c + 1) from t1_cc;') == \
        'SELECT bql_pdf_joint(1, NULL, 3, ("c" + 1)) FROM "t1";'
    assert bql2sql('estimate probability of weight = f(c) from t1_cc;') == \
        'SELECT bql_pdf_joint(1, NULL, 3, "f"("c")) FROM "t1";'
    assert bql2sql('estimate similarity to (rowid = 5) from t1_cc;') == \
        'SELECT bql_row_similarity(1, NULL, _rowid_,' \
        ' (SELECT _rowid_ FROM "t1" WHERE ("rowid" = 5))) FROM "t1";'
    assert bql2sql('estimate similarity to (rowid = 5) with respect to age'
            ' from t1_cc') == \
        'SELECT bql_row_similarity(1, NULL, _rowid_,' \
        ' (SELECT _rowid_ FROM "t1" WHERE ("rowid" = 5)), 2) FROM "t1";'
    assert bql2sql('estimate similarity to (rowid = 5)'
            ' with respect to (age, weight) from t1_cc;') == \
        'SELECT bql_row_similarity(1, NULL, _rowid_,' \
        ' (SELECT _rowid_ FROM "t1" WHERE ("rowid" = 5)), 2, 3) FROM "t1";'
    assert bql2sql('estimate similarity to (rowid = 5) with respect to (*)'
            ' from t1_cc;') == \
        'SELECT bql_row_similarity(1, NULL, _rowid_,' \
        ' (SELECT _rowid_ FROM "t1" WHERE ("rowid" = 5))) FROM "t1";'
    assert bql2sql('estimate similarity to (rowid = 5)'
            ' with respect to (age, weight) from t1_cc;') == \
        'SELECT bql_row_similarity(1, NULL, _rowid_,' \
        ' (SELECT _rowid_ FROM "t1" WHERE ("rowid" = 5)), 2, 3) FROM "t1";'
    assert bql2sql('estimate dependence probability of age with weight' +
        ' from t1_cc;') == \
        'SELECT bql_column_dependence_probability(1, NULL, 2, 3) FROM "t1";'
    with pytest.raises(bayeslite.BQLError):
        # Need both columns fixed.
        bql2sql('estimate dependence probability with age from t1_cc;')
    with pytest.raises(bayeslite.BQLError):
        # Need both columns fixed.
        bql2sql('estimate dependence probability from t1_cc;')
    assert bql2sql('estimate mutual information of age with weight' +
        ' from t1_cc;') == \
        'SELECT bql_column_mutual_information(1, NULL, 2, 3, NULL) FROM "t1";'
    assert bql2sql('estimate mutual information of age with weight' +
        ' using 42 samples from t1_cc;') == \
        'SELECT bql_column_mutual_information(1, NULL, 2, 3, 42) FROM "t1";'
    with pytest.raises(bayeslite.BQLError):
        # Need both columns fixed.
        bql2sql('estimate mutual information with age from t1_cc;')
    with pytest.raises(bayeslite.BQLError):
        # Need both columns fixed.
        bql2sql('estimate mutual information from t1_cc;')
    with pytest.raises(bayeslite.BQLError):
        # Need both columns fixed.
        bql2sql('estimate mutual information with age using 42 samples'
            ' from t1_cc;')
    with pytest.raises(bayeslite.BQLError):
        # Need both columns fixed.
        bql2sql('estimate mutual information using 42 samples from t1_cc;')
    # XXX Should be SELECT, not ESTIMATE, here?
    assert bql2sql('estimate correlation of age with weight from t1_cc;') == \
        'SELECT bql_column_correlation(1, 2, 3) FROM "t1";'
    with pytest.raises(bayeslite.BQLError):
        # Need both columns fixed.
        bql2sql('estimate correlation with age from t1_cc;')
    with pytest.raises(bayeslite.BQLError):
        # Need both columns fixed.
        bql2sql('estimate correlation from t1_cc;')
    with pytest.raises(bayeslite.BQLError):
        # No PREDICT outside INFER.
        bql2sql('estimate predict age with confidence 0.9 from t1_cc;')
    assert bql2sql('infer explicit predict age with confidence 0.9'
            ' from t1_cc;') == \
        'SELECT bql_predict(1, NULL, 2, _rowid_, 0.9) FROM "t1";'
    assert bql2sql('infer explicit rowid, age,'
            ' predict age confidence age_conf from t1_cc') == \
        'SELECT c0 AS "rowid", c1 AS "age",' \
            ' bql_json_get(c2, \'value\') AS "age",' \
            ' bql_json_get(c2, \'confidence\') AS "age_conf"' \
            ' FROM (SELECT "rowid" AS c0, "age" AS c1,' \
                ' bql_predict_confidence(1, NULL, 2, _rowid_) AS c2' \
                ' FROM "t1");'
    assert bql2sql('infer explicit rowid, age,'
            ' predict age as age_inf confidence age_conf from t1_cc') == \
        'SELECT c0 AS "rowid", c1 AS "age",' \
            ' bql_json_get(c2, \'value\') AS "age_inf",' \
            ' bql_json_get(c2, \'confidence\') AS "age_conf"' \
            ' FROM (SELECT "rowid" AS c0, "age" AS c1,' \
                ' bql_predict_confidence(1, NULL, 2, _rowid_) AS c2' \
                ' FROM "t1");'
    assert bql2sql('infer rowid, age, weight from t1_cc') \
        == \
        'SELECT "rowid" AS "rowid",' \
        ' "IFNULL"("age", bql_predict(1, NULL, 2, _rowid_, 0)) AS "age",' \
        ' "IFNULL"("weight", bql_predict(1, NULL, 3, _rowid_, 0))' \
            ' AS "weight"' \
        ' FROM "t1";'
    assert bql2sql('infer rowid, age, weight with confidence 0.9 from t1_cc') \
        == \
        'SELECT "rowid" AS "rowid",' \
        ' "IFNULL"("age", bql_predict(1, NULL, 2, _rowid_, 0.9)) AS "age",' \
        ' "IFNULL"("weight", bql_predict(1, NULL, 3, _rowid_, 0.9))' \
            ' AS "weight"' \
        ' FROM "t1";'
    assert bql2sql('infer rowid, age, weight with confidence 0.9 from t1_cc'
            ' where label = \'foo\'') \
        == \
        'SELECT "rowid" AS "rowid",' \
        ' "IFNULL"("age", bql_predict(1, NULL, 2, _rowid_, 0.9)) AS "age",' \
        ' "IFNULL"("weight", bql_predict(1, NULL, 3, _rowid_, 0.9))' \
            ' AS "weight"' \
        ' FROM "t1"' \
        ' WHERE ("label" = \'foo\');'
    assert bql2sql('infer rowid, age, weight with confidence 0.9 from t1_cc'
            ' where ifnull(label, predict label with confidence 0.7)'
                ' = \'foo\'') \
        == \
        'SELECT "rowid" AS "rowid",' \
        ' "IFNULL"("age", bql_predict(1, NULL, 2, _rowid_, 0.9)) AS "age",' \
        ' "IFNULL"("weight", bql_predict(1, NULL, 3, _rowid_, 0.9))' \
            ' AS "weight"' \
        ' FROM "t1"' \
        ' WHERE ("ifnull"("label", bql_predict(1, NULL, 1, _rowid_, 0.7))' \
            ' = \'foo\');'
    assert bql2sql('infer rowid, * from t1_cc') == \
        'SELECT "rowid" AS "rowid", "id" AS "id",' \
        ' "IFNULL"("label", bql_predict(1, NULL, 1, _rowid_, 0)) AS "label",' \
        ' "IFNULL"("age", bql_predict(1, NULL, 2, _rowid_, 0)) AS "age",' \
        ' "IFNULL"("weight", bql_predict(1, NULL, 3, _rowid_, 0))' \
            ' AS "weight"' \
        ' FROM "t1";'

def test_estimate_columns_trivial():
    prefix0 = 'SELECT c.name AS name'
    prefix1 = ' FROM bayesdb_generator AS g,' \
        ' bayesdb_generator_column AS gc, bayesdb_column AS c' \
        ' WHERE g.id = 1 AND gc.generator_id = g.id' \
        ' AND c.tabname = g.tabname AND c.colno = gc.colno'
    prefix = prefix0 + prefix1
    assert bql2sql('estimate * from columns of t1_cc;') == \
        prefix + ';'
    assert bql2sql('estimate * from columns of t1_cc where' +
            ' (probability of value 42) > 0.5') == \
        prefix + \
        ' AND (bql_column_value_probability(1, NULL, c.colno, 42) > 0.5);'
    assert bql2sql('estimate * from columns of t1_cc'
            ' where (probability of value 8) > (probability of age = 16)') == \
        prefix + \
        ' AND (bql_column_value_probability(1, NULL, c.colno, 8) >' \
        ' bql_pdf_joint(1, NULL, 2, 16));'
    assert bql2sql('estimate *, probability of value 8 given (age = 8)'
            ' from columns of t1_cc;') == \
        prefix0 + \
        ', bql_column_value_probability(1, NULL, c.colno, 8, 2, 8)' + \
        prefix1 + ';'
    with pytest.raises(bayeslite.BQLError):
        # PREDICTIVE PROBABILITY makes no sense without row.
        bql2sql('estimate * from columns of t1_cc where' +
            ' predictive probability of x > 0;')
    with pytest.raises(bayeslite.BQLError):
        # SIMILARITY makes no sense without row.
        bql2sql('estimate * from columns of t1_cc where' +
            ' similarity to (rowid = x) with respect to c > 0;')
    assert bql2sql('estimate * from columns of t1_cc where' +
            ' dependence probability with age > 0.5;') == \
        prefix + \
        ' AND (bql_column_dependence_probability(1, NULL, 2, c.colno) > 0.5);'
    with pytest.raises(bayeslite.BQLError):
        # Must omit exactly one column.
        bql2sql('estimate * from columns of t1_cc where' +
            ' dependence probability of age with weight > 0.5;')
    with pytest.raises(bayeslite.BQLError):
        # Must omit exactly one column.
        bql2sql('estimate * from columns of t1_cc'
            ' where dependence probability > 0.5;')
    assert bql2sql('estimate * from columns of t1_cc order by' +
            ' mutual information with age;') == \
        prefix + \
        ' ORDER BY bql_column_mutual_information(1, NULL, 2, c.colno, NULL);'
    assert bql2sql('estimate * from columns of t1_cc order by' +
            ' mutual information with age using 42 samples;') == \
        prefix + \
        ' ORDER BY bql_column_mutual_information(1, NULL, 2, c.colno, 42);'
    with pytest.raises(bayeslite.BQLError):
        # Must omit exactly one column.
        bql2sql('estimate * from columns of t1_cc order by' +
            ' mutual information of age with weight;')
    with pytest.raises(bayeslite.BQLError):
        # Must omit exactly one column.
        bql2sql('estimate * from columns of t1_cc'
            ' where mutual information > 0.5;')
    with pytest.raises(bayeslite.BQLError):
        # Must omit exactly one column.
        bql2sql('estimate * from columns of t1_cc order by' +
            ' mutual information of age with weight using 42 samples;')
    with pytest.raises(bayeslite.BQLError):
        # Must omit exactly one column.
        bql2sql('estimate * from columns of t1_cc where' +
            ' mutual information using 42 samples > 0.5;')
    assert bql2sql('estimate * from columns of t1_cc order by' +
            ' correlation with age desc;') == \
        prefix + ' ORDER BY bql_column_correlation(1, 2, c.colno) DESC;'
    with pytest.raises(bayeslite.BQLError):
        # Must omit exactly one column.
        bql2sql('estimate * from columns of t1_cc order by' +
            ' correlation of age with weight;')
    with pytest.raises(bayeslite.BQLError):
        # Must omit exactly one column.
        bql2sql('estimate * from columns of t1_cc where correlation > 0.5;')
    with pytest.raises(bayeslite.BQLError):
        # Makes no sense.
        bql2sql('estimate * from columns of t1_cc'
            ' where predict age with confidence 0.9 > 30;')
    assert bql2sql('estimate'
            ' *, dependence probability with weight as depprob,'
            ' mutual information with weight as mutinf'
            ' from columns of t1_cc'
            ' where depprob > 0.5 order by mutinf desc') == \
        prefix0 + \
        ', bql_column_dependence_probability(1, NULL, 3, c.colno)' \
            ' AS "depprob"' \
        ', bql_column_mutual_information(1, NULL, 3, c.colno, NULL)' \
            ' AS "mutinf"' + \
        prefix1 + \
        ' AND ("depprob" > 0.5)' \
        ' ORDER BY "mutinf" DESC;'

def test_estimate_pairwise_trivial():
    prefix = 'SELECT 1 AS generator_id, c0.name AS name0, c1.name AS name1, '
    infix = ' AS value'
    infix0 = ' FROM bayesdb_generator AS g,'
    infix0 += ' bayesdb_generator_column AS gc0, bayesdb_column AS c0,'
    infix0 += ' bayesdb_generator_column AS gc1, bayesdb_column AS c1'
    infix0 += ' WHERE g.id = 1'
    infix0 += ' AND gc0.generator_id = g.id AND gc1.generator_id = g.id'
    infix0 += ' AND c0.tabname = g.tabname AND c0.colno = gc0.colno'
    infix0 += ' AND c1.tabname = g.tabname AND c1.colno = gc1.colno'
    infix += infix0
    assert bql2sql('estimate dependence probability'
            ' from pairwise columns of t1_cc;') == \
        prefix + \
        'bql_column_dependence_probability(1, NULL, c0.colno, c1.colno)' + \
        infix + ';'
    assert bql2sql('estimate mutual information'
            ' from pairwise columns of t1_cc where'
            ' (probability of age = 0) > 0.5;') == \
        prefix + \
        'bql_column_mutual_information(1, NULL, c0.colno, c1.colno, NULL)' + \
        infix + \
        ' AND (bql_pdf_joint(1, NULL, 2, 0) > 0.5);'
    with pytest.raises(bayeslite.BQLError):
        # PROBABILITY OF VALUE is 1-column.
        bql2sql('estimate correlation from pairwise columns of t1_cc where' +
            ' (probability of value 0) > 0.5;')
    with pytest.raises(bayeslite.BQLError):
        # PREDICTIVE PROBABILITY OF is a row function.
        bql2sql('estimate dependence probability'
            ' from pairwise columns of t1_cc' +
            ' where predictive probability of x > 0.5;')
    with pytest.raises(bayeslite.BQLError):
        # Must omit both columns.
        bql2sql('estimate dependence probability'
            ' from pairwise columns of t1_cc'
            ' where dependence probability of age with weight > 0.5;')
    with pytest.raises(bayeslite.BQLError):
        # Must omit both columns.
        bql2sql('estimate mutual information from pairwise columns of t1_cc'
            ' where dependence probability with weight > 0.5;')
    with pytest.raises(bayeslite.BQLError):
        # Must omit both columns.
        bql2sql('estimate mutual information using 42 samples'
            ' from pairwise columns of t1_cc'
            ' where dependence probability with weight > 0.5;')
    assert bql2sql('estimate correlation from pairwise columns of t1_cc'
            ' where dependence probability > 0.5;') == \
        prefix + 'bql_column_correlation(1, c0.colno, c1.colno)' + \
        infix + ' AND' \
        ' (bql_column_dependence_probability(1, NULL, c0.colno, c1.colno)' \
            ' > 0.5);'
    with pytest.raises(bayeslite.BQLError):
        # Must omit both columns.
        bql2sql('estimate dependence probability'
            ' from pairwise columns of t1_cc'
            ' where mutual information of age with weight > 0.5;')
    with pytest.raises(bayeslite.BQLError):
        # Must omit both columns.
        bql2sql('estimate dependence probability'
            ' from pairwise columns of t1_cc'
            ' where mutual information of age with weight using 42 samples'
                ' > 0.5;')
    with pytest.raises(bayeslite.BQLError):
        # Must omit both columns.
        bql2sql('estimate mutual information from pairwise columns of t1_cc'
            ' where mutual information with weight > 0.5;')
    with pytest.raises(bayeslite.BQLError):
        # Must omit both columns.
        bql2sql('estimate mutual information using 42 samples'
            ' from pairwise columns of t1_cc'
            ' where mutual information with weight using 42 samples > 0.5;')
    assert bql2sql('estimate correlation from pairwise columns of t1_cc' +
            ' where mutual information > 0.5;') == \
        prefix + 'bql_column_correlation(1, c0.colno, c1.colno)' + \
        infix + ' AND' + \
        ' (bql_column_mutual_information(1, NULL, c0.colno, c1.colno, NULL)' \
            ' > 0.5);'
    assert bql2sql('estimate correlation from pairwise columns of t1_cc' +
            ' where mutual information using 42 samples > 0.5;') == \
        prefix + 'bql_column_correlation(1, c0.colno, c1.colno)' + \
        infix + ' AND' + \
        ' (bql_column_mutual_information(1, NULL, c0.colno, c1.colno, 42)' \
            ' > 0.5);'
    with pytest.raises(bayeslite.BQLError):
        # Must omit both columns.
        bql2sql('estimate dependence probability'
            ' from pairwise columns of t1_cc'
            ' where correlation of age with weight > 0.5;')
    with pytest.raises(bayeslite.BQLError):
        # Must omit both columns.
        bql2sql('estimate mutual information from pairwise columns of t1_cc'
            ' where correlation with weight > 0.5;')
    with pytest.raises(bayeslite.BQLError):
        # Must omit both columns.
        bql2sql('estimate mutual information using 42 samples'
            ' from pairwise columns of t1_cc'
            ' where correlation with weight > 0.5;')
    assert bql2sql('estimate correlation from pairwise columns of t1_cc'
            ' where correlation > 0.5;') == \
        prefix + 'bql_column_correlation(1, c0.colno, c1.colno)' + \
        infix + ' AND' + \
        ' (bql_column_correlation(1, c0.colno, c1.colno) > 0.5);'
    with pytest.raises(bayeslite.BQLError):
        # Makes no sense.
        bql2sql('estimate dependence probability'
            ' from pairwise columns of t1_cc'
            ' where predict age with confidence 0.9 > 30;')
    assert bql2sql('estimate dependence probability as depprob,'
            ' mutual information as mutinf'
            ' from pairwise columns of t1_cc'
            ' where depprob > 0.5 order by mutinf desc') == \
        prefix + \
        'bql_column_dependence_probability(1, NULL, c0.colno, c1.colno)' \
        ' AS "depprob",' \
        ' bql_column_mutual_information(1, NULL, c0.colno, c1.colno, NULL)' \
        ' AS "mutinf"' + \
        infix0 + \
        ' AND ("depprob" > 0.5)' \
        ' ORDER BY "mutinf" DESC;'

def test_estimate_pairwise_row():
    prefix = 'SELECT r0._rowid_ AS rowid0, r1._rowid_ AS rowid1'
    infix = ' AS value FROM "t1" AS r0, "t1" AS r1'
    assert bql2sql('estimate similarity from pairwise t1_cc;') == \
        prefix + ', bql_row_similarity(1, NULL, r0._rowid_, r1._rowid_)' + \
        infix + ';'
    assert bql2sql('estimate similarity with respect to age' +
            ' from pairwise t1_cc;') == \
        prefix + ', bql_row_similarity(1, NULL, r0._rowid_, r1._rowid_, 2)' + \
        infix + ';'
    with pytest.raises(bayeslite.BQLError):
        # PREDICT is a 1-row function.
        bql2sql('estimate predict age with confidence 0.9 from pairwise t1;')

def test_estimate_pairwise_selected_columns():
    assert bql2sql('estimate dependence probability'
            ' from pairwise columns of t1_cc for label, age') == \
        'SELECT 1 AS generator_id, c0.name AS name0, c1.name AS name1,' \
        ' bql_column_dependence_probability(1, NULL, c0.colno, c1.colno)' \
            ' AS value' \
        ' FROM bayesdb_generator AS g,' \
        ' bayesdb_generator_column AS gc0, bayesdb_column AS c0,' \
        ' bayesdb_generator_column AS gc1, bayesdb_column AS c1' \
        ' WHERE g.id = 1' \
        ' AND gc0.generator_id = g.id AND gc1.generator_id = g.id' \
        ' AND c0.tabname = g.tabname AND c0.colno = gc0.colno' \
        ' AND c1.tabname = g.tabname AND c1.colno = gc1.colno' \
        ' AND c0.colno IN (1, 2) AND c1.colno IN (1, 2);'
    assert bql2sql('estimate dependence probability'
            ' from pairwise columns of t1_cc'
            ' for (ESTIMATE * FROM COLUMNS OF t1_cc'
                ' ORDER BY name DESC LIMIT 2)') == \
        'SELECT 1 AS generator_id, c0.name AS name0, c1.name AS name1,' \
        ' bql_column_dependence_probability(1, NULL, c0.colno, c1.colno)' \
            ' AS value' \
        ' FROM bayesdb_generator AS g,' \
        ' bayesdb_generator_column AS gc0, bayesdb_column AS c0,' \
        ' bayesdb_generator_column AS gc1, bayesdb_column AS c1' \
        ' WHERE g.id = 1' \
        ' AND gc0.generator_id = g.id AND gc1.generator_id = g.id' \
        ' AND c0.tabname = g.tabname AND c0.colno = gc0.colno' \
        ' AND c1.tabname = g.tabname AND c1.colno = gc1.colno' \
        ' AND c0.colno IN (3, 1) AND c1.colno IN (3, 1);'

def test_select_columns_subquery():
    assert bql2sql('select id, t1.(estimate * from columns of t1_cc'
            ' order by name asc limit 2) from t1') == \
        'SELECT "id", "t1"."age", "t1"."label" FROM "t1";'

def test_trivial_commands():
    with test_csv.bayesdb_csv_file(test_csv.csv_data) as (bdb, fname):
        # XXX Query parameters!
        with open(fname, 'rU') as f:
            bayeslite.bayesdb_read_csv(bdb, 't', f, header=True, create=True)
        with open(fname, 'rU') as f:
            with pytest.raises(ValueError):
                bayeslite.bayesdb_read_csv(bdb, 't', f, header=True,
                    create=True)
        with open(fname, 'rU') as f:
            bayeslite.bayesdb_read_csv(bdb, 't', f, header=True, create=True,
                ifnotexists=True)
        guess.bayesdb_guess_generator(bdb, 't_cc', 't', 'crosscat')
        with pytest.raises(ValueError):
            guess.bayesdb_guess_generator(bdb, 't_cc', 't', 'crosscat')
        guess.bayesdb_guess_generator(bdb, 't_cc', 't', 'crosscat',
            ifnotexists=True)
        bdb.execute('initialize 2 models for t_cc')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('initialize 2 models for t_cc')
        bdb.execute('drop models from t_cc')
        bdb.execute('drop models from t_cc')
        bdb.execute('initialize 2 models for t_cc')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('initialize 2 models for t_cc')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('drop models 0-2 from t_cc')
        bdb.execute('drop models 0-1 from t_cc')
        with bdb.savepoint():
            bdb.execute('initialize 2 models for t_cc')
            bdb.execute('drop models 0-1 from t_cc')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('drop models 0-1 from t_cc')
        bdb.execute('initialize 2 models for t_cc')
        bdb.execute('initialize 1 model if not exists for t_cc')
        bdb.execute('initialize 2 models if not exists for t_cc')
        generator_id = core.bayesdb_get_generator(bdb, 't_cc')
        assert core.bayesdb_generator_table(bdb, generator_id) == 't'
        bdb.execute('alter table t rename to t')
        assert core.bayesdb_generator_table(bdb, generator_id) == 't'
        bdb.execute('alter table t rename to T')
        assert core.bayesdb_generator_table(bdb, generator_id) == 'T'
        bdb.execute('estimate count(*) from t_cc').fetchall()
        bdb.execute('alter table t rename to t')
        assert core.bayesdb_generator_table(bdb, generator_id) == 't'
        bdb.execute('alter generator t_cc rename to t0_cc')
        assert core.bayesdb_generator_name(bdb, generator_id) == 't0_cc'
        bdb.execute('alter generator t0_cc rename to zot, rename to T0_CC')
        assert core.bayesdb_generator_name(bdb, generator_id) == 'T0_CC'
        bdb.execute('alter generator T0_cc rename to T0_cc')
        assert core.bayesdb_generator_name(bdb, generator_id) == 'T0_cc'
        bdb.execute('alter generator t0_CC rename to t0_cc')
        assert core.bayesdb_generator_name(bdb, generator_id) == 't0_cc'
        bdb.execute('estimate count(*) from t0_cc').fetchall()
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('estimate count(*) from t_cc')
        bdb.execute('alter generator t0_cc rename to T0_cc')
        bdb.execute('analyze t0_cc for 1 iteration wait')
        colno = core.bayesdb_generator_column_number(bdb, generator_id,
            'gender')
        with pytest.raises(parse.BQLParseError):
            # Rename the table's columns, not the generator's columns.
            bdb.execute('alter generator t0_cc rename gender to sex')
        with pytest.raises(NotImplementedError): # XXX
            bdb.execute('alter table t rename to t0, rename gender to sex')
            assert core.bayesdb_generator_column_number(bdb, generator_id,
                    'sex') \
                == colno
            bdb.execute('analyze t0_cc model 0 for 1 iteration wait')
            bdb.execute('alter generator t0_cc rename to t_cc')
            assert core.bayesdb_generator_column_number(bdb, generator_id,
                    'sex') \
                == colno
            bdb.execute('select sex from t0').fetchall()
            with pytest.raises(AssertionError): # XXX
                bdb.execute('select gender from t0')
                assert False, 'Need to fix quoting of unknown columns!'
            with pytest.raises(bayeslite.BQLError):
                bdb.execute('estimate predict sex with confidence 0.9'
                    ' from t_cc').fetchall()
            bdb.execute('infer explicit predict sex with confidence 0.9'
                ' from t_cc').fetchall()
            with pytest.raises(bayeslite.BQLError):
                bdb.execute('estimate predict gender with confidence 0.9'
                    ' from t_cc')
            with pytest.raises(bayeslite.BQLError):
                bdb.execute('infer explicit predict gender with confidence 0.9'
                    ' from t_cc')
            bdb.execute('alter table t0 rename sex to gender')
            assert core.bayesdb_generator_column_number(bdb, generator_id,
                    'gender') \
                == colno
        bdb.execute('alter generator t0_cc rename to t_cc')     # XXX
        bdb.execute('alter table t rename to T0')               # XXX
        bdb.sql_execute('create table t0_temp(x)')
        bdb.execute('alter table T0 rename to t0')
        assert bdb.execute('select count(*) from t0_temp').fetchvalue() == 0
        assert bdb.execute('select count(*) from t0').fetchvalue() > 0
        bdb.execute('drop table T0_TEMP')
        bdb.execute('analyze t_cc model 0 for 1 iteration wait')
        bdb.execute('analyze t_cc model 1 for 1 iteration wait')
        bdb.execute('analyze t_cc models 0-1 for 1 iteration wait')
        bdb.execute('analyze t_cc models 0,1 for 1 iteration wait')
        bdb.execute('analyze t_cc for 1 iteration wait')
        bdb.execute('select * from t0').fetchall()
        bdb.execute('select * from T0').fetchall()
        bdb.execute('estimate * from t_cc').fetchall()
        bdb.execute('estimate * from T_CC').fetchall()
        bdb.execute('estimate similarity from pairwise t_cc').fetchall()
        bdb.execute('select value from'
            ' (estimate correlation from pairwise columns of t_cc)').fetchall()
        bdb.execute('infer explicit predict age with confidence 0.9'
            ' from t_cc').fetchall()
        bdb.execute('infer explicit predict AGE with confidence 0.9'
            ' from T_cc').fetchall()
        bdb.execute('infer explicit predict aGe with confidence 0.9'
            ' from T_cC').fetchall()
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('estimate predict agee with confidence 0.9 from t_cc')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('infer explicit predict agee with confidence 0.9'
                ' from t_cc')
        # Make sure it works with the table too if we create a default
        # generator.
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('estimate * from t0')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('estimate * from columns of t0')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('estimate correlation from pairwise columns of t0')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('estimate similarity from pairwise t0')
        bdb.execute('''
            create default generator t_ccd for t0 using crosscat(
                age numerical,
                rank categorical
            )
        ''')
        bdb.execute('initialize 1 model if not exists for t_ccd')
        bdb.execute('analyze t_ccd for 1 iteration wait')
        bdb.execute('''
            create generator t_cce for t0 using crosscat(
                guess(*),
                age numerical,
                rank numerical
            )
        ''')
        with pytest.raises(bayeslite.BQLError):
            # No models to analyze.
            bdb.execute('analyze t_cce for 1 iteration wait')
        bdb.execute('initialize 1 model if not exists for t_cce')
        bdb.execute('analyze t_cce for 1 iteration wait')
        bdb.execute('estimate correlation'
            ' from pairwise columns of t_cce').fetchall()
        bdb.execute('initialize 2 models if not exists for t0')
        bdb.execute('analyze t0 for 1 iteration wait')
        bdb.execute('estimate * from t0').fetchall()
        bdb.execute('estimate * from columns of t0').fetchall()
        bdb.execute('estimate * from columns of t0'
            ' order by dependence probability with age').fetchall()
        bdb.execute('estimate correlation'
            ' from pairwise columns of t0').fetchall()
        bdb.execute('estimate similarity from pairwise t0').fetchall()
        # XXX Distinguish the two generators somehow.
        bdb.execute('alter table t0 set default generator to t_cc')
        bdb.execute('estimate * from t0').fetchall()
        bdb.execute('estimate * from columns of t0').fetchall()
        bdb.execute('estimate correlation'
            ' from pairwise columns of t0').fetchall()
        bdb.execute('estimate similarity from pairwise t0').fetchall()
        bdb.execute('alter table t0 unset default generator')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('estimate * from t0')
        bdb.execute('alter table t0 rename to t')
        bdb.execute('alter table t set default generator to t_ccd')
        bdb.execute('estimate * from t').fetchall()
        bdb.execute('estimate * from columns of t').fetchall()
        bdb.execute('estimate correlation'
            ' from pairwise columns of t').fetchall()
        bdb.execute('estimate similarity from pairwise t').fetchall()
        bdb.execute('drop generator t_ccd')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('initialize 3 models if not exists for t_ccd')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('initialize 4 models if not exists for t')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('analyze t_ccd for 1 iteration wait')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('analyze t0 for 1 iteration wait')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('estimate * from t_ccd')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('estimate * from columns of t_ccd')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('estimate correlation from pairwise columns of t_ccd')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('estimate similarity from pairwise t_ccd')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('estimate * from t')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('estimate * from columns of t')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('estimate correlation from pairwise columns of t')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('estimate similarity from pairwise t')
        bdb.execute('alter table t set default generator to t_cc')
        bdb.execute('initialize 6 models if not exists for t_cc')
        bdb.execute('initialize 7 models if not exists for t')
        bdb.execute('analyze t_cc for 1 iteration wait')
        bdb.execute('analyze t for 1 iteration wait')
        bdb.execute('estimate * from t').fetchall()
        bdb.execute('estimate * from columns of t').fetchall()
        bdb.execute('estimate correlation'
            ' from pairwise columns of t').fetchall()
        bdb.execute('estimate similarity from pairwise t').fetchall()

def test_trivial_deadline():
    with test_core.t1() as (bdb, _table_id):
        bdb.execute('initialize 1 model for t1_cc')
        bdb.execute('analyze t1_cc for 1 second wait')

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
        with open(fname, 'rU') as f:
            bayeslite.bayesdb_read_csv(bdb, 't', f, header=True, create=True)
        assert bql_execute(bdb, 'select count(*) from t') == [(7,)]
        assert bql_execute(bdb, 'select count(distinct division) from t') == \
            [(6,)]
        assert bql_execute(bdb, 'select * from t where height > ?', (70,)) == \
            [
                (41, 'M', 65600, 72, 'marketing', 4),
                (30, 'M', 70000, 73, 'sales', 4),
                (30, 'F', 81000, 73, 'engineering', 3),
            ]
        assert bql_execute(bdb, 'select * from t where height > ?123',
                (0,)*122 + (70,)) == \
            [
                (41, 'M', 65600, 72, 'marketing', 4),
                (30, 'M', 70000, 73, 'sales', 4),
                (30, 'F', 81000, 73, 'engineering', 3),
            ]
        assert bql_execute(bdb, 'select age from t where division = :division',
                {':division': 'sales'}) == \
            [(34,), (30,)]
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
        def traced_execute(query, *args):
            bql = []
            def trace(string, _bindings):
                bql.append(' '.join(string.split()))
            bdb.trace(trace)
            with bdb.savepoint():
                bdb.execute(query, *args)
            bdb.untrace(trace)
            return bql
        def sqltraced_execute(query, *args):
            sql = []
            def trace(string, _bindings):
                sql.append(' '.join(string.split()))
            bdb.sql_trace(trace)
            with bdb.savepoint():
                bdb.execute(query, *args)
            bdb.sql_untrace(trace)
            return sql
        bdb.execute('create generator t_cc for t using crosscat(guess(*))')
        bdb.execute('initialize 1 model for t_cc;')
        iters0 = bdb.sql_execute('select *'
            ' from bayesdb_generator_model').fetchall()
        thetas0 = bdb.sql_execute('select *'
            ' from bayesdb_crosscat_theta').fetchall()
        bdb.execute('analyze t_cc for 1 iteration wait;')
        iters1 = bdb.sql_execute('select *'
            ' from bayesdb_generator_model').fetchall()
        thetas1 = bdb.sql_execute('select *'
            ' from bayesdb_crosscat_theta').fetchall()
        assert iters0 != iters1
        assert thetas0 != thetas1
        assert traced_execute('estimate similarity to (rowid = 1)'
                ' with respect to (estimate * from columns of t_cc limit 1)'
                ' from t_cc;') == [
            'estimate similarity to (rowid = 1)' \
                ' with respect to (estimate * from columns of t_cc limit 1)' \
                ' from t_cc;',
        ]
        assert sqltraced_execute('estimate similarity to (rowid = 1)'
                ' with respect to (estimate * from columns of t_cc limit 1)'
                ' from t_cc;') == [
            'SELECT COUNT(*) FROM bayesdb_generator'
                ' WHERE name = :name OR (defaultp AND tabname = :name)',
            'SELECT id FROM bayesdb_generator'
                ' WHERE name = :name OR (defaultp AND tabname = :name)',
            'SELECT tabname FROM bayesdb_generator WHERE id = ?',
            'SELECT COUNT(*) FROM bayesdb_generator'
                ' WHERE name = :name OR (defaultp AND tabname = :name)',
            'SELECT id FROM bayesdb_generator'
                ' WHERE name = :name OR (defaultp AND tabname = :name)',
            # ESTIMATE * FROM COLUMNS OF:
            'SELECT c.name AS name'
                ' FROM bayesdb_generator AS g,'
                    ' bayesdb_generator_column AS gc,'
                    ' bayesdb_column AS c'
                ' WHERE g.id = 1 AND gc.generator_id = g.id'
                    ' AND c.tabname = g.tabname AND c.colno = gc.colno'
                ' LIMIT 1',
            'SELECT c.colno'
                ' FROM bayesdb_generator AS g,'
                    ' bayesdb_generator_column AS gc,'
                    ' bayesdb_column AS c'
                ' WHERE g.id = :generator_id AND c.name = :column_name'
                    ' AND g.id = gc.generator_id'
                    ' AND g.tabname = c.tabname AND gc.colno = c.colno',
            # ESTIMATE SIMILARITY TO (rowid=1):
            'SELECT tabname FROM bayesdb_generator WHERE id = ?',
            'SELECT bql_row_similarity(1, NULL, _rowid_,'
                ' (SELECT _rowid_ FROM "t" WHERE ("rowid" = 1)), 0) FROM "t"',
            'SELECT metamodel FROM bayesdb_generator WHERE id = ?',
            'SELECT modelno FROM bayesdb_crosscat_theta'
                ' WHERE generator_id = ?',
            'SELECT theta_json FROM bayesdb_crosscat_theta'
                ' WHERE generator_id = ? AND modelno = ?',
            'SELECT modelno FROM bayesdb_crosscat_theta'
                ' WHERE generator_id = ?',
            'SELECT sql_rowid, cc_row_id FROM bayesdb_crosscat_subsample'
                ' WHERE generator_id = ? AND sql_rowid IN (1)',
            'SELECT metadata_json FROM bayesdb_crosscat_metadata'
                ' WHERE generator_id = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
        ]
        assert sqltraced_execute('estimate similarity to (rowid = 1)'
                ' with respect to (estimate * from columns of t_cc limit ?)'
                ' from t_cc;',
                (1,)) == [
            'SELECT COUNT(*) FROM bayesdb_generator'
                ' WHERE name = :name OR (defaultp AND tabname = :name)',
            'SELECT id FROM bayesdb_generator'
                ' WHERE name = :name OR (defaultp AND tabname = :name)',
            'SELECT tabname FROM bayesdb_generator WHERE id = ?',
            'SELECT COUNT(*) FROM bayesdb_generator'
                ' WHERE name = :name OR (defaultp AND tabname = :name)',
            'SELECT id FROM bayesdb_generator'
                ' WHERE name = :name OR (defaultp AND tabname = :name)',
            # ESTIMATE * FROM COLUMNS OF:
            'SELECT c.name AS name'
                ' FROM bayesdb_generator AS g,'
                    ' bayesdb_generator_column AS gc,'
                    ' bayesdb_column AS c'
                ' WHERE g.id = 1 AND gc.generator_id = g.id'
                    ' AND c.tabname = g.tabname AND c.colno = gc.colno'
                ' LIMIT ?1',
            'SELECT c.colno'
                ' FROM bayesdb_generator AS g,'
                    ' bayesdb_generator_column AS gc,'
                    ' bayesdb_column AS c'
                ' WHERE g.id = :generator_id AND c.name = :column_name'
                    ' AND g.id = gc.generator_id'
                    ' AND g.tabname = c.tabname AND gc.colno = c.colno',
            'SELECT tabname FROM bayesdb_generator WHERE id = ?',
            # ESTIMATE SIMILARITY TO (rowid=1):
            'SELECT bql_row_similarity(1, NULL, _rowid_,'
                ' (SELECT _rowid_ FROM "t" WHERE ("rowid" = 1)), 0) FROM "t"',
            'SELECT metamodel FROM bayesdb_generator WHERE id = ?',
            'SELECT modelno FROM bayesdb_crosscat_theta'
                ' WHERE generator_id = ?',
            'SELECT theta_json FROM bayesdb_crosscat_theta'
                ' WHERE generator_id = ? AND modelno = ?',
            'SELECT modelno FROM bayesdb_crosscat_theta'
                ' WHERE generator_id = ?',
            'SELECT sql_rowid, cc_row_id FROM bayesdb_crosscat_subsample'
                ' WHERE generator_id = ? AND sql_rowid IN (1)',
            'SELECT metadata_json FROM bayesdb_crosscat_metadata'
                ' WHERE generator_id = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
        ]
        assert sqltraced_execute('create temp table if not exists sim as'
                    ' simulate age, RANK, division'
                    " from t_cc given gender = 'F' limit 4") == [
            'SELECT COUNT(*) FROM bayesdb_generator WHERE name = ?',
            'PRAGMA table_info("sim")',
            'SELECT COUNT(*) FROM bayesdb_generator'
                ' WHERE name = :name OR (defaultp AND tabname = :name)',
            'SELECT id FROM bayesdb_generator'
                ' WHERE name = :name OR (defaultp AND tabname = :name)',
            'SELECT metamodel FROM bayesdb_generator WHERE id = ?',
            'SELECT tabname FROM bayesdb_generator WHERE id = ?',
            'PRAGMA table_info("t")',
            "SELECT CAST(4 AS INTEGER), CAST(NULL AS INTEGER), 'F'",
            'SELECT c.colno'
                ' FROM bayesdb_generator AS g,'
                    ' bayesdb_generator_column AS gc,'
                    ' bayesdb_column AS c'
                ' WHERE g.id = :generator_id AND c.name = :column_name'
                    ' AND g.id = gc.generator_id'
                    ' AND g.tabname = c.tabname AND gc.colno = c.colno',
            'SELECT c.colno'
                ' FROM bayesdb_generator AS g,'
                    ' bayesdb_generator_column AS gc,'
                    ' bayesdb_column AS c'
                ' WHERE g.id = :generator_id AND c.name = :column_name'
                    ' AND g.id = gc.generator_id'
                    ' AND g.tabname = c.tabname AND gc.colno = c.colno',
            'SELECT c.colno'
                ' FROM bayesdb_generator AS g,'
                    ' bayesdb_generator_column AS gc,'
                    ' bayesdb_column AS c'
                ' WHERE g.id = :generator_id AND c.name = :column_name'
                    ' AND g.id = gc.generator_id'
                    ' AND g.tabname = c.tabname AND gc.colno = c.colno',
            'SELECT c.colno'
                ' FROM bayesdb_generator AS g,'
                    ' bayesdb_generator_column AS gc,'
                    ' bayesdb_column AS c'
                ' WHERE g.id = :generator_id AND c.name = :column_name'
                    ' AND g.id = gc.generator_id'
                    ' AND g.tabname = c.tabname AND gc.colno = c.colno',
            'CREATE TEMP TABLE IF NOT EXISTS "sim"'
                ' ("age" NUMERIC,"RANK" NUMERIC,"division" NUMERIC)',
            'SELECT metamodel FROM bayesdb_generator WHERE id = ?',
            'SELECT tabname FROM bayesdb_generator WHERE id = ?',
            'SELECT MAX(_rowid_) FROM "t"',
            'SELECT metadata_json FROM bayesdb_crosscat_metadata'
                ' WHERE generator_id = ?',
            'SELECT modelno FROM bayesdb_crosscat_theta'
                ' WHERE generator_id = ?',
            'SELECT theta_json FROM bayesdb_crosscat_theta'
                ' WHERE generator_id = ? AND modelno = ?',
            'SELECT modelno FROM bayesdb_crosscat_theta'
                ' WHERE generator_id = ?',
            'SELECT sql_rowid, cc_row_id FROM bayesdb_crosscat_subsample'
                ' WHERE generator_id = ? AND sql_rowid IN (8)',
            'SELECT tabname FROM bayesdb_generator WHERE id = ?',
            'SELECT c.name, c.colno, gc.stattype'
                ' FROM bayesdb_column AS c,'
                    ' bayesdb_generator AS g,'
                    ' bayesdb_generator_column AS gc'
                ' WHERE g.id = ? AND c.tabname = g.tabname'
                    ' AND c.colno = gc.colno'
                    ' AND gc.generator_id = g.id'
                ' ORDER BY c.colno ASC',
            'SELECT CAST("age" AS "text"),CAST("gender" AS "text"),'
                'CAST("salary" AS "text"),CAST("height" AS "text"),'
                'CAST("division" AS "text"),CAST("rank" AS "text")'
                ' FROM "t" WHERE _rowid_ IN (8) ORDER BY _rowid_ ASC',
            'SELECT MAX(cc_row_id) + 1 FROM bayesdb_crosscat_subsample'
                ' WHERE generator_id = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT stattype FROM bayesdb_generator_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT stattype FROM bayesdb_generator_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT stattype FROM bayesdb_generator_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT stattype FROM bayesdb_generator_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT stattype FROM bayesdb_generator_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT stattype FROM bayesdb_generator_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT stattype FROM bayesdb_generator_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT stattype FROM bayesdb_generator_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT stattype FROM bayesdb_generator_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT stattype FROM bayesdb_generator_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT stattype FROM bayesdb_generator_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT stattype FROM bayesdb_generator_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT stattype FROM bayesdb_generator_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
            'INSERT INTO "sim" ("age","RANK","division") VALUES (?,?,?)',
            'INSERT INTO "sim" ("age","RANK","division") VALUES (?,?,?)',
            'INSERT INTO "sim" ("age","RANK","division") VALUES (?,?,?)',
            'INSERT INTO "sim" ("age","RANK","division") VALUES (?,?,?)',
        ]
        assert sqltraced_execute('select * from (simulate age from t_cc'
                    " given gender = 'F' limit 4)") == [
            'PRAGMA table_info("bayesdb_temp_0")',
            'SELECT id FROM bayesdb_generator WHERE name = :name' \
                ' OR (defaultp AND tabname = :name)',
            'SELECT tabname FROM bayesdb_generator WHERE id = ?',
            'PRAGMA table_info("t")',
            "SELECT CAST(4 AS INTEGER), CAST(NULL AS INTEGER), 'F'",
            'SELECT c.colno' \
                ' FROM bayesdb_generator AS g,' \
                    ' bayesdb_generator_column AS gc,' \
                    ' bayesdb_column AS c' \
                ' WHERE g.id = :generator_id' \
                    ' AND c.name = :column_name' \
                    ' AND g.id = gc.generator_id' \
                    ' AND g.tabname = c.tabname' \
                    ' AND gc.colno = c.colno',
            'SELECT c.colno' \
                ' FROM bayesdb_generator AS g,' \
                    ' bayesdb_generator_column AS gc,' \
                    ' bayesdb_column AS c' \
                ' WHERE g.id = :generator_id' \
                    ' AND c.name = :column_name' \
                    ' AND g.id = gc.generator_id' \
                    ' AND g.tabname = c.tabname' \
                    ' AND gc.colno = c.colno',
            'SELECT metamodel FROM bayesdb_generator WHERE id = ?',
            'SELECT tabname FROM bayesdb_generator WHERE id = ?',
            'SELECT MAX(_rowid_) FROM "t"',
            'SELECT metadata_json FROM bayesdb_crosscat_metadata' \
                ' WHERE generator_id = ?',
            'SELECT modelno FROM bayesdb_crosscat_theta' \
                ' WHERE generator_id = ?',
            'SELECT theta_json FROM bayesdb_crosscat_theta'
                ' WHERE generator_id = ? AND modelno = ?',
            'SELECT modelno FROM bayesdb_crosscat_theta' \
                ' WHERE generator_id = ?',
            'SELECT sql_rowid, cc_row_id FROM bayesdb_crosscat_subsample'
                ' WHERE generator_id = ? AND sql_rowid IN (8)',
            'SELECT tabname FROM bayesdb_generator WHERE id = ?',
            'SELECT c.name, c.colno, gc.stattype'
                ' FROM bayesdb_column AS c,'
                    ' bayesdb_generator AS g,'
                    ' bayesdb_generator_column AS gc'
                ' WHERE g.id = ? AND c.tabname = g.tabname'
                    ' AND c.colno = gc.colno'
                    ' AND gc.generator_id = g.id'
                ' ORDER BY c.colno ASC',
            'SELECT CAST("age" AS "text"),CAST("gender" AS "text"),'
                'CAST("salary" AS "text"),CAST("height" AS "text"),'
                'CAST("division" AS "text"),CAST("rank" AS "text")'
                ' FROM "t" WHERE _rowid_ IN (8) ORDER BY _rowid_ ASC',
            'SELECT MAX(cc_row_id) + 1 FROM bayesdb_crosscat_subsample'
                ' WHERE generator_id = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column' \
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column' \
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT stattype FROM bayesdb_generator_column' \
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column' \
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT stattype FROM bayesdb_generator_column' \
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column' \
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT stattype FROM bayesdb_generator_column' \
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column' \
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT stattype FROM bayesdb_generator_column' \
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column' \
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT stattype FROM bayesdb_generator_column' \
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column' \
                ' WHERE generator_id = ? AND colno = ?',
            'CREATE TEMP TABLE "bayesdb_temp_0" ("age" NUMERIC)',
            'INSERT INTO "bayesdb_temp_0" ("age") VALUES (?)',
            'INSERT INTO "bayesdb_temp_0" ("age") VALUES (?)',
            'INSERT INTO "bayesdb_temp_0" ("age") VALUES (?)',
            'INSERT INTO "bayesdb_temp_0" ("age") VALUES (?)',
            'SELECT * FROM (SELECT * FROM "bayesdb_temp_0")',
            'DROP TABLE "bayesdb_temp_0"',
        ]
        bdb.execute('''
            create generator tu_cc for t using crosscat(
                age NUMERICAL,
                gender CATEGORICAL,   -- Not binary!
                salary NUMERICAL,
                height NUMERICAL,
                division CATEGORICAL,
                rank CATEGORICAL
            )
        ''')
        bdb.execute('initialize 1 model for tu_cc;')
        assert sqltraced_execute('analyze tu_cc for 1 iteration wait;') == [
            'SELECT COUNT(*) FROM bayesdb_generator'
                ' WHERE name = :name OR (defaultp AND tabname = :name)',
            'SELECT id FROM bayesdb_generator'
                ' WHERE name = :name OR (defaultp AND tabname = :name)',
            'SELECT metamodel FROM bayesdb_generator WHERE id = ?',
            'SELECT metadata_json FROM bayesdb_crosscat_metadata'
                ' WHERE generator_id = ?',
            'SELECT tabname FROM bayesdb_generator WHERE id = ?',
            'SELECT c.name, c.colno, gc.stattype'
                ' FROM bayesdb_column AS c,'
                    ' bayesdb_generator AS g,'
                    ' bayesdb_generator_column AS gc'
                ' WHERE g.id = ?'
                    ' AND c.tabname = g.tabname'
                    ' AND c.colno = gc.colno'
                    ' AND gc.generator_id = g.id'
                ' ORDER BY c.colno ASC',
            'SELECT CAST(t."age" AS "real"),CAST(t."gender" AS "text"),'
                    'CAST(t."salary" AS "real"),CAST(t."height" AS "real"),'
                    'CAST(t."division" AS "text"),CAST(t."rank" AS "text")'
                ' FROM "t" AS t,'
                    ' bayesdb_crosscat_subsample AS s'
                ' WHERE s.generator_id = ? AND s.sql_rowid = t._rowid_',
        ] + 7*[                 # one per row of table
            # age NUMERICAL
            'SELECT stattype FROM bayesdb_generator_column'
                ' WHERE generator_id = ? AND colno = ?',
            # gender CATEGORICAL
            'SELECT stattype FROM bayesdb_generator_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
            # salary NUMERICAL
            'SELECT stattype FROM bayesdb_generator_column'
                ' WHERE generator_id = ? AND colno = ?',
            # height NUMERICAL
            'SELECT stattype FROM bayesdb_generator_column'
                ' WHERE generator_id = ? AND colno = ?',
            # division CATEGORICAL
            'SELECT stattype FROM bayesdb_generator_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
            # rank CATEGORICAL
            'SELECT stattype FROM bayesdb_generator_column'
                ' WHERE generator_id = ? AND colno = ?',
            'SELECT cc_colno FROM bayesdb_crosscat_column'
                ' WHERE generator_id = ? AND colno = ?',
        ] + [
            'SELECT modelno FROM bayesdb_crosscat_theta'
                ' WHERE generator_id = ?',
            'SELECT theta_json FROM bayesdb_crosscat_theta'
                ' WHERE generator_id = ? AND modelno = ?',
            'UPDATE bayesdb_generator_model'
                ' SET iterations = iterations + :iterations'
                ' WHERE generator_id = :generator_id AND modelno = :modelno',
            'UPDATE bayesdb_crosscat_theta'
                ' SET theta_json = :theta_json'
                ' WHERE generator_id = :generator_id AND modelno = :modelno',
            'SELECT 1 + MAX(checkpoint) FROM bayesdb_crosscat_diagnostics'
                ' WHERE generator_id = :generator_id AND modelno = :modelno',
            'INSERT INTO bayesdb_crosscat_diagnostics'
                ' (generator_id, modelno, checkpoint, logscore,'
                    ' num_views, column_crp_alpha, iterations)'
                ' VALUES (:generator_id, :modelno, :checkpoint, :logscore,'
                    ' :num_views, :column_crp_alpha, :iterations)',
        ]

def test_createtab():
    with test_csv.bayesdb_csv_file(test_csv.csv_data) as (bdb, fname):
        with pytest.raises(apsw.SQLError):
            bdb.execute('drop table t')
        bdb.execute('drop table if exists t')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('drop generator t_cc')
        bdb.execute('drop generator if exists t_cc')
        with open(fname, 'rU') as f:
            bayeslite.bayesdb_read_csv(bdb, 't', f, header=True, create=True)
        with bdb.savepoint():
            # Savepoint because we don't actually want the new data to
            # be inserted.
            with open(fname, 'rU') as f:
                bayeslite.bayesdb_read_csv(bdb, 't', f, header=True,
                    create=True, ifnotexists=True)
        bdb.execute('''
            create generator t_cc for t using crosscat (
                guess(*),
                age numerical
            )
        ''')
        with pytest.raises(bayeslite.BQLError):
            # Redefining generator.
            bdb.execute('''
                create generator t_cc for t using crosscat (
                    guess(*),
                    age ignore
                )
            ''')
        # Make sure ignore columns work.
        #
        # XXX Also check key columns.
        bdb.execute('''
            create generator t_cc0 for t using crosscat (
                guess(*),
                age ignore
            )
        ''')
        bdb.execute('drop generator t_cc0')
        generator_id = core.bayesdb_get_generator(bdb, 't_cc')
        colno = core.bayesdb_generator_column_number(bdb, generator_id, 'age')
        assert core.bayesdb_generator_column_stattype(bdb, generator_id,
                colno) == 'numerical'
        bdb.execute('initialize 1 model for t_cc')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('drop table t')
        bdb.execute('drop generator t_cc')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('drop generator t_cc')
        bdb.execute('drop generator if exists t_cc')
        bdb.execute('drop table t')
        with open(fname, 'rU') as f:
            bayeslite.bayesdb_read_csv(bdb, 't', f, header=True, create=True)
        guess.bayesdb_guess_generator(bdb, 't_cc', 't', 'crosscat')
        bdb.execute("create table u as select * from t where gender = 'F'")
        assert bql_execute(bdb, 'select * from u') == [
            (23, 'F', 81000, 67, 'data science', 3),
            (36, 'F', 96000, 70, 'management', 2),
            (30, 'F', 81000, 73, 'engineering', 3),
        ]
        with pytest.raises(apsw.SQLError):
            bdb.execute("create table u as select * from t where gender = 'F'")
        bdb.execute('drop table u')
        with pytest.raises(apsw.SQLError):
            bql_execute(bdb, 'select * from u')
        bdb.execute("create temp table u as"
            " select * from t where gender = 'F'")
        assert bql_execute(bdb, 'select * from u') == [
            (23, 'F', 81000, 67, 'data science', 3),
            (36, 'F', 96000, 70, 'management', 2),
            (30, 'F', 81000, 73, 'engineering', 3),
        ]
        # XXX Test to make sure TEMP is passed through, and the table
        # doesn't persist on disk.

def test_txn():
    with test_csv.bayesdb_csv_file(test_csv.csv_data) as (bdb, fname):
        # Make sure rollback and commit fail outside a transaction.
        with pytest.raises(bayeslite.BayesDBTxnError):
            bdb.execute('ROLLBACK')
        with pytest.raises(bayeslite.BayesDBTxnError):
            bdb.execute('COMMIT')

        # Open a transaction which we'll roll back.
        bdb.execute('BEGIN')
        try:
            # Make sure transactions don't nest.  (Use savepoints.)
            with pytest.raises(bayeslite.BayesDBTxnError):
                bdb.execute('BEGIN')
        finally:
            bdb.execute('ROLLBACK')

        # Make sure rollback and commit still fail outside a transaction.
        with pytest.raises(bayeslite.BayesDBTxnError):
            bdb.execute('ROLLBACK')
        with pytest.raises(bayeslite.BayesDBTxnError):
            bdb.execute('COMMIT')

        # Open a transaction which we'll commit.
        bdb.execute('BEGIN')
        try:
            with pytest.raises(bayeslite.BayesDBTxnError):
                bdb.execute('BEGIN')
        finally:
            bdb.execute('COMMIT')

        with pytest.raises(bayeslite.BayesDBTxnError):
            bdb.execute('ROLLBACK')
        with pytest.raises(bayeslite.BayesDBTxnError):
            bdb.execute('COMMIT')

        # Make sure ROLLBACK undoes the effects of the transaction.
        bdb.execute('BEGIN')
        try:
            with open(fname, 'rU') as f:
                bayeslite.bayesdb_read_csv(bdb, 't', f, header=True,
                    create=True)
            bdb.execute('SELECT * FROM t').fetchall()
            guess.bayesdb_guess_generator(bdb, 't_cc', 't', 'crosscat')
            bdb.execute('ESTIMATE * FROM t_cc').fetchall()
        finally:
            bdb.execute('ROLLBACK')
        with pytest.raises(apsw.SQLError):
            bdb.execute('SELECT * FROM t')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('ESTIMATE * FROM t_cc')

        # Make sure CREATE and DROP both work in the transaction.
        bdb.execute('BEGIN')
        try:
            with open(fname, 'rU') as f:
                bayeslite.bayesdb_read_csv(bdb, 't', f, header=True,
                    create=True)
            bdb.execute('SELECT * FROM t').fetchall()
            guess.bayesdb_guess_generator(bdb, 't_cc', 't', 'crosscat')
            bdb.execute('ESTIMATE * FROM t_cc').fetchall()
            with pytest.raises(bayeslite.BQLError):
                bdb.execute('DROP TABLE t')
            bdb.execute('DROP GENERATOR t_cc')
            with pytest.raises(bayeslite.BQLError):
                bdb.execute('ESTIMATE * FROM t_cc')
            bdb.execute('DROP TABLE t')
            with pytest.raises(apsw.SQLError):
                bdb.execute('SELECT * FROM t')
        finally:
            bdb.execute('ROLLBACK')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('ESTIMATE * FROM t_cc')
        with pytest.raises(apsw.SQLError):
            bdb.execute('SELECT * FROM t')

        # Make sure CREATE and DROP work even if we commit.
        bdb.execute('BEGIN')
        try:
            with open(fname, 'rU') as f:
                bayeslite.bayesdb_read_csv(bdb, 't', f, header=True,
                    create=True)
            bdb.execute('SELECT * FROM t').fetchall()
            guess.bayesdb_guess_generator(bdb, 't_cc', 't', 'crosscat')
            bdb.execute('ESTIMATE * FROM t_cc').fetchall()
            with pytest.raises(bayeslite.BQLError):
                bdb.execute('DROP TABLE t')
            bdb.execute('DROP GENERATOR t_cc')
            with pytest.raises(bayeslite.BQLError):
                bdb.execute('ESTIMATE * FROM t_cc')
            bdb.execute('DROP TABLE t')
            with pytest.raises(apsw.SQLError):
                bdb.execute('SELECT * FROM t')
        finally:
            bdb.execute('COMMIT')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('ESTIMATE * FROM t_cc')
        with pytest.raises(apsw.SQLError):
            bdb.execute('SELECT * FROM t')

        # Make sure CREATE persists if we commit.
        bdb.execute('BEGIN')
        try:
            with open(fname, 'rU') as f:
                bayeslite.bayesdb_read_csv(bdb, 't', f, header=True,
                    create=True)
            bdb.execute('SELECT * FROM t').fetchall()
            guess.bayesdb_guess_generator(bdb, 't_cc', 't', 'crosscat')
            bdb.execute('ESTIMATE * FROM t_cc').fetchall()
        finally:
            bdb.execute('COMMIT')
        bdb.execute('SELECT * FROM t').fetchall()
        bdb.execute('ESTIMATE * FROM t_cc').fetchall()

        # Make sure bdb.transaction works, rolls back on exception,
        # and handles nesting correctly with respect to savepoints.
        try:
            with bdb.transaction():
                bdb.sql_execute('create table quagga(x)')
                raise StopIteration
        except StopIteration:
            pass
        with pytest.raises(apsw.SQLError):
            bdb.execute('select * from quagga')
        with bdb.transaction():
            with bdb.savepoint():
                with bdb.savepoint():
                    pass
        with bdb.savepoint():
            with pytest.raises(bayeslite.BayesDBTxnError):
                with bdb.transaction():
                    pass

        # XXX To do: Make sure other effects (e.g., analysis) get
        # rolled back by ROLLBACK.

def test_predprob_null():
    with test_core.bayesdb() as bdb:
        bdb.sql_execute('''
            create table foo (
                id integer primary key not null,
                x numeric,
                y numeric
            )
        ''')
        bdb.sql_execute("insert into foo values (1, 1, 'strange')")
        bdb.sql_execute("insert into foo values (2, 1.2, 'strange')")
        bdb.sql_execute("insert into foo values (3, 0.8, 'strange')")
        bdb.sql_execute("insert into foo values (4, NULL, 'strange')")
        bdb.sql_execute("insert into foo values (5, 73, 'up')")
        bdb.sql_execute("insert into foo values (6, 80, 'up')")
        bdb.sql_execute("insert into foo values (7, 60, 'up')")
        bdb.sql_execute("insert into foo values (8, 67, NULL)")
        bdb.sql_execute("insert into foo values (9, 3.1415926, 'down')")
        bdb.sql_execute("insert into foo values (10, 1.4142135, 'down')")
        bdb.sql_execute("insert into foo values (11, 2.7182818, 'down')")
        bdb.sql_execute("insert into foo values (12, NULL, 'down')")
        bdb.execute('''
            create generator foo_cc for foo using crosscat (
                x numerical,
                y categorical
            )
        ''')
        bdb.execute('initialize 1 model for foo_cc')
        bdb.execute('analyze foo_cc for 1 iteration wait')
        # Null value => null predictive probability.
        assert bdb.execute('estimate predictive probability of x'
                ' from foo_cc where id = 4;').fetchall() == \
            [(None,)]
        # Nonnull value => nonnull predictive probability.
        x = bdb.execute('estimate predictive probability of x'
            ' from foo_cc where id = 5').fetchall()
        assert len(x) == 1
        assert len(x[0]) == 1
        assert isinstance(x[0][0], (int, float))

def test_guess_all():
    with test_core.bayesdb() as bdb:
        bdb.sql_execute('create table foo (x numeric, y numeric, z numeric)')
        bdb.sql_execute('insert into foo values (1, 2, 3)')
        bdb.execute('create generator foo_cc for foo using crosscat(guess(*))')

def test_misc_errors():
    with test_core.t1() as (bdb, _generator_id):
        with pytest.raises(bayeslite.BQLError):
            # t1_cc already exists as a generator.
            bdb.execute('create table t1_cc as simulate weight from t1_cc'
                ' limit 1')
        with pytest.raises(bayeslite.BQLError):
            # t1 already exists as a table.
            bdb.execute('create table t1 as simulate weight from t1_cc'
                ' limit 1')
        with pytest.raises(bayeslite.BQLError):
            # t1x does not exist as a generator or table.
            bdb.execute('create table t1_sim as simulate weight from t1x'
                ' limit 1')
        with pytest.raises(bayeslite.BQLError):
            # t1_cc does not have a column waught.
            bdb.execute('create table t1_sim as simulate waught from t1_cc'
                ' limit 1')
        with pytest.raises(bayeslite.BQLError):
            # t1_cc does not have a column agee.
            bdb.execute('create table t1_sim as simulate weight from t1_cc'
                ' given agee = 42 limit 1')
        with pytest.raises(bayeslite.BQLError):
            # t2 does not exist as a table.
            bdb.execute('alter table t2 set default generator to t1_cc')
        with bdb.savepoint():
            bdb.sql_execute('create table t2(x)')
            with pytest.raises(bayeslite.BQLError):
                # t1 already exists as a table.
                bdb.execute('alter table t2 rename to t1')
            with pytest.raises(bayeslite.BQLError):
                # t1_cc already exists as a generator.
                bdb.execute('alter table t2 rename to t1_cc')
        with pytest.raises(NotImplementedError):
            # Renaming columns is not yet implemented.
            bdb.execute('alter table t1 rename weight to mass')
        with pytest.raises(bayeslite.BQLError):
            # t1_xc does not exist as a generator.
            bdb.execute('alter table t1 set default generator to t1_xc')
        with pytest.raises(bayeslite.BQLError):
            # xcat does not exist as a metamodel.
            bdb.execute('create generator t1_xc for t1 using xcat(guess(*))')
        with pytest.raises(bayeslite.BQLError):
            # t1 already exists as a table.
            bdb.execute('create generator t1 for t1 using crosscat(guess(*))')
        with pytest.raises(bayeslite.BQLError):
            # t1_cc already exists as a generator.
            bdb.execute('create generator t1_cc for t1'
                ' using crosscat(guess(*))')
        with pytest.raises(bayeslite.BQLError):
            # multinomial is not a known statistical type.
            bdb.execute('create generator t1_xc for t1'
                ' using crosscat(weight multinomial)')
        with pytest.raises(bayeslite.BQLError):
            # t1_xc does not exist as a generator.
            bdb.execute('alter generator t1_xc rename to t1_xcat')
        with pytest.raises(bayeslite.BQLError):
            # t1 already exists as a table.
            bdb.execute('alter generator t1_cc rename to t1')
        with bdb.savepoint():
            bdb.execute('create generator t1_xc for t1'
                ' using crosscat(guess(*))')
            with pytest.raises(bayeslite.BQLError):
                # t1_xc already exists as a generator.
                bdb.execute('alter generator t1_cc rename to t1_xc')
        with pytest.raises(NotImplementedError):
            # Need WAIT.
            bdb.execute('analyze t1_cc for 1 iteration')
        with bdb.savepoint():
            bdb.execute('initialize 1 model for t1_cc')
            bdb.execute('analyze t1_cc for 1 iteration wait')
            with pytest.raises(apsw.SQLError):
                bdb.execute('select'
                    ' nonexistent((simulate age from t1_cc limit 1));')
        with pytest.raises(ValueError):
            bdb.execute('select :x', {'y': 42})
        with pytest.raises(ValueError):
            bdb.execute('select :x', {'x': 53, 'y': 42})
        with pytest.raises(ValueError):
            bdb.execute('select ?, ?', (1,))
        with pytest.raises(ValueError):
            bdb.execute('select ?', (1, 2))
        with pytest.raises(TypeError):
            bdb.execute('select ?', 42)
        with pytest.raises(NotImplementedError):
            bdb.execute('infer explicit predict age confidence ac, *'
                ' from t1_cc')
        with pytest.raises(NotImplementedError):
            bdb.execute('infer explicit predict age confidence ac,'
                ' t1.(select age from t1 limit 1) from t1_cc')
        with pytest.raises(bayeslite.BQLError):
            try:
                bdb.execute('estimate similarity to (rowid=1)'
                    ' with respect to (agee) from t1_cc')
            except bayeslite.BQLError as e:
                assert 'No such columns in generator:' in str(e)
                raise

def test_nested_simulate():
    with test_core.t1() as (bdb, _table_id):
        bdb.execute('initialize 1 model for t1_cc')
        bdb.execute('analyze t1_cc for 1 iteration wait')
        bdb.execute('select (simulate age from t1_cc limit 1),'
            ' (simulate weight from t1_cc limit 1)').fetchall()
        assert bdb.temp_table_name() == 'bayesdb_temp_2'
        assert not core.bayesdb_has_table(bdb, 'bayesdb_temp_0')
        assert not core.bayesdb_has_table(bdb, 'bayesdb_temp_1')
        bdb.execute('simulate weight from t1_cc'
            ' given age = (simulate age from t1_cc limit 1)'
            ' limit 1').fetchall()
        # Make sure unwinding doesn't raise an exception.  Calling
        # __del__ directly, rather than via del(), has two effects:
        #
        # (a) It actually raises any exceptions in the method, unlike
        # del(), which suppresses them.
        #
        # (b) It may cause a subsequent __del__ to fail and raise an
        # exception, so that a subsequent del(), including an implicit
        # one at the end of a scope, may print a message to stderr.
        #
        # Effect (a) is what we are actually trying to test.  Effect
        # (b) is a harmless consequence as far as pytest is concerned,
        # as long as the test otherwise passes.
        bdb.execute('simulate weight from t1_cc'
            ' given age = (simulate age from t1_cc limit 1)'
            ' limit 1').__del__()

def test_using_models():
    def setup(bdb):
        bdb.execute('initialize 1 model for t1_cc')
        bdb.execute('analyze t1_cc for 1 iteration wait')
    assert bql2sql('simulate age, weight from t1_cc using model 0'
            ' limit 1', setup=setup) == \
        'SELECT * FROM "bayesdb_temp_0";'
    assert bql2sql('estimate predictive probability of weight from t1_cc'
            ' using model 42') == \
        'SELECT bql_row_column_predictive_probability(1, 42, _rowid_, 3)' \
            ' FROM "t1";'
    assert bql2sql('estimate *, mutual information with weight as mi'
            ' from columns of t1_cc using model 42') == \
        'SELECT c.name AS name,' \
            ' bql_column_mutual_information(1, 42, 3, c.colno, NULL) AS "mi"' \
        ' FROM bayesdb_generator AS g,' \
            ' bayesdb_generator_column AS gc, bayesdb_column AS c' \
        ' WHERE g.id = 1 AND gc.generator_id = g.id' \
        ' AND c.tabname = g.tabname AND c.colno = gc.colno;'
    assert bql2sql('estimate mutual information from pairwise columns of t1_cc'
            ' using model 42') == \
        'SELECT 1 AS generator_id, c0.name AS name0, c1.name AS name1,' \
            ' bql_column_mutual_information(1, 42, c0.colno, c1.colno,' \
                ' NULL) AS value' \
        ' FROM bayesdb_generator AS g,' \
            ' bayesdb_generator_column AS gc0, bayesdb_column AS c0,' \
            ' bayesdb_generator_column AS gc1, bayesdb_column AS c1' \
        ' WHERE g.id = 1' \
            ' AND gc0.generator_id = g.id AND gc1.generator_id = g.id' \
            ' AND c0.tabname = g.tabname AND c0.colno = gc0.colno' \
            ' AND c1.tabname = g.tabname AND c1.colno = gc1.colno;'
    assert bql2sql('estimate similarity from pairwise t1_cc'
            ' using model 42') == \
        'SELECT r0._rowid_ AS rowid0, r1._rowid_ AS rowid1,' \
            ' bql_row_similarity(1, 42, r0._rowid_, r1._rowid_) AS value' \
        ' FROM "t1" AS r0, "t1" AS r1;'
    assert bql2sql('infer id, age, weight from t1_cc using model 42') == \
        'SELECT "id" AS "id",' \
            ' "IFNULL"("age", bql_predict(1, 42, 2, _rowid_, 0)) AS "age",' \
            ' "IFNULL"("weight", bql_predict(1, 42, 3, _rowid_, 0))' \
                ' AS "weight"' \
        ' FROM "t1";'
    assert bql2sql('infer explicit id, age,'
            ' ifnull(weight, predict weight with confidence 0.9)'
            ' from t1_cc using model 42') == \
        'SELECT "id", "age",' \
            ' "ifnull"("weight", bql_predict(1, 42, 3, _rowid_, 0.9))' \
        ' FROM "t1";'

def test_checkpoint__ci_slow():
    with test_core.t1() as (bdb, generator_id):
        bdb.execute('initialize 1 model for t1_cc')
        bdb.execute('analyze t1_cc for 10 iterations checkpoint 1 iteration'
            ' wait')
        bdb.execute('analyze t1_cc for 5 seconds checkpoint 1 second wait')
        bdb.execute('drop models from t1_cc')
        bdb.execute('initialize 1 model for t1_cc')
        bdb.execute('analyze t1_cc for 5 iterations checkpoint 1 second wait')
        sql = '''
            select iterations from bayesdb_generator_model
                where generator_id = ?
        '''
        assert bdb.execute(sql, (generator_id,)).fetchvalue() == 5
        bdb.execute('drop models from t1_cc')
        bdb.execute('initialize 1 model for t1_cc')
        bdb.execute('analyze t1_cc for 1 iteration checkpoint 2 iterations'
            ' wait')
        sql = '''
            select iterations from bayesdb_generator_model
                where generator_id = ?
        '''
        assert bdb.execute(sql, (generator_id,)).fetchvalue() == 1
        bdb.execute('analyze t1_cc for 1 iteration checkpoint 0 seconds wait')

def test_infer_confidence__ci_slow():
    with test_core.t1() as (bdb, _generator_id):
        bdb.execute('initialize 1 model for t1_cc')
        bdb.execute('analyze t1_cc for 1 iteration wait')
        bdb.execute('infer explicit rowid, rowid as another_rowid, 4,'
            ' age, predict age as age_inf confidence age_conf'
            ' from t1_cc').fetchall()

def test_infer_as_estimate():
    with test_core.t1() as (bdb, _generator_id):
        bdb.execute('initialize 1 model for t1_cc')
        bdb.execute('analyze t1_cc for 1 iteration wait')
        bdb.execute('infer explicit predictive probability of age'
            ' from t1_cc').fetchall()

def test_estimate_by():
    with test_core.t1() as (bdb, _generator_id):
        bdb.execute('initialize 1 model for t1_cc')
        bdb.execute('analyze t1_cc for 1 iteration wait')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('estimate predictive probability of age'
                ' by t1_cc')
        with pytest.raises(bayeslite.BQLError):
            bdb.execute('estimate similarity to (rowid=1) by t1_cc')
        def check(x):
            assert len(bdb.execute(x).fetchall()) == 1
        check('estimate probability of age = 42 by t1_cc')
        check('estimate dependence probability of age with weight by t1_cc')
        check('estimate mutual information of age with weight by t1_cc')
        check('estimate correlation of age with weight by t1_cc')
        check('estimate correlation pvalue of age with weight by t1_cc')

def test_empty_cursor():
    with bayeslite.bayesdb_open() as bdb:
        assert bdb.execute('SELECT 0').connection == bdb
        empty(bdb.execute('BEGIN'))
        empty(bdb.execute('COMMIT'))
        empty(bdb.sql_execute('CREATE TABLE t(x, y, z)'))
        empty(bdb.sql_execute('INSERT INTO t VALUES(1,2,3)'))
        empty(bdb.sql_execute('INSERT INTO t VALUES(4,5,6)'))
        empty(bdb.sql_execute('INSERT INTO t VALUES(7,8,9)'))
        empty(bdb.execute('''
            CREATE GENERATOR t_cc FOR t USING crosscat(GUESS(*))
        '''))
        empty(bdb.execute('INITIALIZE 1 MODEL FOR t_cc'))
        empty(bdb.execute('DROP GENERATOR t_cc'))
        empty(bdb.execute('DROP TABLE t'))

def test_create_generator_ifnotexists():
    # XXX Test other metamodels too, because they have a role in ensuring that
    # this works. Their create_generator will still be called.
    for using_clause in ('crosscat(GUESS(*))',):
        with bayeslite.bayesdb_open() as bdb:
            bdb.sql_execute('CREATE TABLE t(x, y, z)')
            bdb.sql_execute('INSERT INTO t VALUES(1,2,3)')
            for _i in (0, 1):
                bdb.execute('CREATE GENERATOR t_cc IF NOT EXISTS FOR t USING '
                            + using_clause)
            try:
                bdb.execute('CREATE GENERATOR t_cc FOR t USING ' + using_clause)
                assert False  # Should have said it exists.
            except bayeslite.BQLError:
                pass

class MockTracerOneQuery(bayeslite.IBayesDBTracer):
    def __init__(self, q, qid):
        self.q = q
        self.qid = qid
        self.start_calls = 0
        self.ready_calls = 0
        self.error_calls = 0
        self.finished_calls = 0
        self.abandoned_calls = 0
    def start(self, qid, query, bindings):
        assert qid == self.qid
        assert query == self.q
        assert bindings == ()
        self.start_calls += 1
    def ready(self, qid, _cursor):
        assert qid == self.qid
        self.ready_calls += 1
    def error(self, qid, _e):
        assert qid == self.qid
        self.error_calls += 1
    def finished(self, qid):
        assert qid == self.qid
        self.finished_calls += 1
    def abandoned(self, qid):
        assert qid == self.qid
        self.abandoned_calls += 1

def test_tracing_smoke():
    with test_core.t1() as (bdb, _generator_id):
        q = 'SELECT * FROM t1'
        tracer = MockTracerOneQuery(q, 1)
        bdb.trace(tracer)
        cursor = bdb.execute(q)
        assert tracer.start_calls == 1
        assert tracer.ready_calls == 1
        assert tracer.error_calls == 0
        assert tracer.finished_calls == 0
        assert tracer.abandoned_calls == 0
        cursor.fetchall()
        assert tracer.start_calls == 1
        assert tracer.ready_calls == 1
        assert tracer.error_calls == 0
        assert tracer.finished_calls == 1
        assert tracer.abandoned_calls == 0
        del cursor
        assert tracer.start_calls == 1
        assert tracer.ready_calls == 1
        assert tracer.error_calls == 0
        assert tracer.finished_calls == 1
        assert tracer.abandoned_calls == 1
        bdb.untrace(tracer)
        # XXX Make sure the whole cursor API works.
        q = 'SELECT 42'
        tracer = MockTracerOneQuery(q, 2)
        bdb.trace(tracer)
        cursor = bdb.execute(q)
        assert tracer.start_calls == 1
        assert tracer.ready_calls == 1
        assert tracer.error_calls == 0
        assert tracer.finished_calls == 0
        assert tracer.abandoned_calls == 0
        assert cursor.fetchvalue() == 42
        assert tracer.start_calls == 1
        assert tracer.ready_calls == 1
        assert tracer.error_calls == 0
        assert tracer.finished_calls == 1
        assert tracer.abandoned_calls == 0
        del cursor
        assert tracer.start_calls == 1
        assert tracer.ready_calls == 1
        assert tracer.error_calls == 0
        assert tracer.finished_calls == 1
        assert tracer.abandoned_calls == 1

def test_tracing_error_smoke():
    with test_core.t1() as (bdb, _generator_id):
        q = 'SELECT * FROM wrong'
        tracer = MockTracerOneQuery(q, 1)
        bdb.trace(tracer)
        with pytest.raises(apsw.SQLError):
            bdb.execute(q)
        assert tracer.start_calls == 1
        assert tracer.ready_calls == 0
        assert tracer.error_calls == 1
        assert tracer.finished_calls == 0
        assert tracer.abandoned_calls == 0

class Boom(Exception): pass
class ErroneousMetamodel(troll.TrollMetamodel):
    def __init__(self):
        self.call_ct = 0
    def name(self): return 'erroneous'
    def logpdf_joint(self, *_args, **_kwargs):
        if self.call_ct > 10: # Wait to avoid raising during sqlite's prefetch
            raise Boom()
        self.call_ct += 1
        return 0

def test_tracing_execution_error_smoke():
    with test_core.t1() as (bdb, _generator_id):
        bayeslite.bayesdb_register_metamodel(bdb, ErroneousMetamodel())
        bdb.execute('''
            CREATE GENERATOR t1_err FOR t1 USING erroneous(age NUMERICAL)''')
        q = 'ESTIMATE PREDICTIVE PROBABILITY OF age FROM t1_err'
        tracer = MockTracerOneQuery(q, 1)
        bdb.trace(tracer)
        cursor = bdb.execute(q)
        assert tracer.start_calls == 1
        assert tracer.ready_calls == 1
        assert tracer.error_calls == 0
        assert tracer.finished_calls == 0
        assert tracer.abandoned_calls == 0
        with pytest.raises(Boom):
            cursor.fetchall()
        assert tracer.start_calls == 1
        assert tracer.ready_calls == 1
        assert tracer.error_calls == 1
        assert tracer.finished_calls == 0
        assert tracer.abandoned_calls == 0
