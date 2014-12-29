/*
 *  Copyright (c) 2010-2014, MIT Probabilistic Computing Project
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

bql(start)		::= phrases.

phrases(one)		::= phrasesemi(phrase).
phrases(many)		::= phrases(phrases) T_SEMI phrasesemi(phrase).
phrasesemi(empty)	::= T_SEMI.
phrasesemi(nonempty)	::= phrase(phrase) T_SEMI.
phrase(query)		::= query(q).
/*
phrase(command)		::= command(c).
*/

query(q)		::= query_action(action) query_body(body).

query_action(none)	::= .
query_action(freq)	::= K_FREQ.
query_action(hist)	::= K_HIST.
query_action(summarize)	::= K_SUMMARIZE.
query_action(plot)	::= K_PLOT.
/* XXX EXPLAIN (QUERY PLAN)?  */

query_body(select)	::= select(q).
/*
query_body(infer)	::= infer(q).
query_body(simulate)	::= simulate(q).
query_body(estimate_pairwise_row)
			::= estimate_pairwise_row(q).
query_body(create_column_list)
			::= create_column_list(q).
*/

/* XXX Support WITH ... SELECT ... (i.e., common table expressions).  */
select(one)		::= select1(select).

/* XXX Support compound selects.
select(compound)	::= select(left) select_op(op) select1(right).

select_op(union)	::= K_UNION.
select_op(union_all)	::= K_UNION K_ALL.
select_op(except)	::= K_EXCEPT.
select_op(intersect)	::= K_INTERSECT.
*/

select1(s)		::= K_SELECT distinct(distinct) select_columns(columns)
				from(tables) where(conditions)
				group_by(grouping) order_by(ordering)
				limit(limit).

/* XXX Support SELECT DISTINCT/ALL.
distinct(distinct)	::= K_DISTINCT.
distinct(all)		::= K_ALL.
*/
distinct(default)	::= .

/* XXX Allow mixing BQL functions and SQL columns?  */
select_columns(sql)	::= select_columns1(cs).
select_columns(bqlfn)	::= select_bqlfn(bqlfn).

select_columns1(one)	::= select_column(c).
select_columns1(many)	::= select_columns1(cs) T_COMMA select_column(c).

select_column(star)	::= T_STAR.
select_column(qstar)	::= table_name(table) T_DOT T_STAR.
select_column(exp)	::= expression(e) as(as).

/*
 * XXX Why are these allowed only in select, rather than generally
 * anywhere that an expression is allowed?  I'm parroting the old
 * grammar here, but it seems to me this should be changed.
 */
select_bqlfn(predprob)	::= K_PREDICTIVE K_PROBABILITY K_OF L_NAME(col).
select_bqlfn(prob)	::= K_PROBABILITY K_OF L_NAME(col) T_EQ expression(e).
select_bqlfn(typ_row)	::= K_TYPICALITY.
select_bqlfn(typ_col)	::= K_TYPICALITY K_OF L_NAME(col).
select_bqlfn(sim)	::= K_SIMILARITY K_TO expression(row) wrt(wrt).
select_bqlfn(depprob)	::= K_DEPENDENCE K_PROBABILITY ofwith(ofwith).
select_bqlfn(mutinf)	::= K_MUTUAL K_INFORMATION ofwith(ofwith).
select_bqlfn(correl)	::= K_CORRELATION ofwith(ofwith).

wrt(none)		::= .
wrt(some)		::= K_WITH K_RESPECT K_TO column_lists(columns).

ofwith(with)		::= K_WITH L_NAME(col).
ofwith(ofwith)		::= K_OF L_NAME(col1) K_WITH L_NAME(col2).

column_lists(one)	::= column_list(collist).
column_lists(many)	::= column_lists(collists)
				T_COMMA|K_AND column_list(collist).
column_list(all)	::= T_STAR.
column_list(column)	::= L_NAME(column).

as(none)		::= .
as(some)		::= AS L_NAME(name).

from(empty)		::= .
from(nonempty)		::= K_FROM select_tables(tables).

select_tables(one)	::= select_table(t).
select_tables(many)	::= select_tables(ts) T_COMMA select_table(t).

select_table(named)	::= table_name(table).
select_table(subquery)	::= T_LROUND query_body(q) T_RROUND.

where(unconditional)	::= .
where(conditional)	::= K_WHERE expression(condition).

/* XXX Allow database-qualified name.  */
table_name(unqualified)	::= L_NAME(name).

group_by(none)		::= .
group_by(some)		::= K_GROUP K_BY group_keys(k).
group_keys(one)		::= expression(e).
group_keys(many)	::= group_keys(k) T_COMMA expression(e).

order_by(none)		::= .
order_by(some)		::= K_ORDER K_BY order_keys(k).
order_keys(one)		::= expression(e) order_sense(s).
order_keys(many)	::= order_keys(k) T_COMMA expression(e) order_sense(s).
order_sense(none)	::= .
order_sense(asc)	::= K_ASC.
order_sense(desc)	::= K_DESC.

limit(none)		::= .
limit(some)		::= K_LIMIT expression(limit).
limit(offset)		::= K_LIMIT expression(limit)
				K_OFFSET expression(offset).
limit(comma)		::= K_LIMIT expression(offset)
				T_COMMA expression(limit).

expression(literal)	::= literal(v).
expression(paren)	::= T_LROUND expression(e) T_RROUND.
expression(column)	::= L_NAME(col).
expression(tabcol)	::= table_name(tab) T_DOT L_NAME(col).

literal(null)		::= K_NULL.
literal(integer)	::= L_INTEGER(i).
literal(float)		::= L_FLOAT(f).
literal(string)		::= L_STRING(s).
