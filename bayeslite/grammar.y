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
select_columns1(many)	::= select_column_list(cs) T_COMMA select_column(c).

select_column(star)	::= T_STAR.
select_column(qstar)	::= name(table) T_DOT T_STAR.
select_column(exp)	::= expression(e) as(as).

/*
 * XXX Why are these allowed only in select, rather than generally
 * anywhere that an expression is allowed?  I'm Parroting the old
 * grammar here, but it seems to me this should be changed.
 */
select_bqlfn(predprob)	::= K_PREDICTIVE K_PROBABILITY K_OF name(col).
select_bqlfn(prob)	::= K_PROBABILITY K_OF name(col) T_EQ expression(e).
select_bqlfn(typ_row)	::= K_TYPICALITY.
select_bqlfn(typ_col)	::= K_TYPICALITY K_OF name(col).
select_bqlfn(sim)	::= K_SIMILARITY to(to) wrt(wrt).
select_bqlfn(depprob)	::= K_DEPENDENCE K_PROBABILITY ofwith(ofwith).
select_bqlfn(mutinf)	::= K_MUTUAL K_INFORMATION ofwith(ofwith).
select_bqlfn(correl)	::= K_CORRELATION ofwith(ofwith).

as(none)		::= .
as(some)		::= AS L_NAME(name).

from(empty)		::= .
from(nonempty)		::= K_FROM select_tables(tables).

select_tables(one)	::= select_table(t).
select_tables(many)	::= select_tables(ts) T_COMMA select_table(t).

select_table(named)	::= name(n).
select_table(subquery)	::= T_LROUND query_body(q) T_RROUND.

/* XXX Allow database-qualified name.  */
name(unqualified)	::= L_NAME.

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
