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

bql(start)		::= phrases(phrases).

phrases(none)		::= .
phrases(some)		::= phrases(phrases) phrasesemi(phrase).
phrasesemi(empty)	::= T_SEMI.
phrasesemi(nonempty)	::= phrase(phrase) T_SEMI.
phrase(query)		::= query_action(action) query(q).
/*
phrase(command)		::= command(c).
*/

query_action(none)	::= .
query_action(freq)	::= K_FREQ.
query_action(hist)	::= K_HIST.
query_action(summarize)	::= K_SUMMARIZE.
query_action(plot)	::= K_PLOT.
/* XXX EXPLAIN (QUERY PLAN)?  */

query(select)		::= select(q).
/*
query(infer)		::= infer(q).
query(simulate)		::= simulate(q).
query(estimate_pairwise_row)
			::= estimate_pairwise_row(q).
query(create_column_list)
			::= create_column_list(q).
*/

/* XXX Support WITH ... SELECT ... (i.e., common table expressions).  */
select(s)		::= K_SELECT select_quant(quant) select_columns(cols)
				from(tabs)
				where(cond)
				group_by(group)
				order_by(ord)
				limit(lim).

select_quant(distinct)	::= K_DISTINCT.
select_quant(all)	::= K_ALL.
select_quant(default)	::= .

select_columns(one)	::= select_column(c).
select_columns(many)	::= select_columns(cs) T_COMMA select_column(c).

select_column(star)	::= T_STAR.
select_column(qstar)	::= table_name(table) T_DOT T_STAR.
select_column(exp)	::= expression(e) as(name).
select_column(bql)	::= select_bql(bql).

/*
 * XXX Why are these allowed only in select, rather than generally
 * anywhere that an expression is allowed?  I'm parroting the old
 * grammar here, but it seems to me this should be changed.
 */
select_bql(predprob)	::= K_PREDICTIVE K_PROBABILITY K_OF L_NAME(col).
select_bql(prob)	::= K_PROBABILITY K_OF L_NAME(col) T_EQ expression(e).
select_bql(typ_row)	::= K_TYPICALITY.
select_bql(typ_col)	::= K_TYPICALITY K_OF L_NAME(col).
select_bql(sim)		::= K_SIMILARITY K_TO expression(row) wrt(cols).
select_bql(depprob)	::= K_DEPENDENCE K_PROBABILITY ofwith(cols).
select_bql(mutinf)	::= K_MUTUAL K_INFORMATION ofwith(cols).
select_bql(correl)	::= K_CORRELATION ofwith(cols).

/*
 * Parenthesizing the column lists is not what we did before, but is
 * necessary to avoid ambiguity at the comma: is it another select
 * column, or is it another wrt column?
 */
wrt(none)		::= .
wrt(one)		::= K_WITH K_RESPECT K_TO column_list(collist).
wrt(some)		::= K_WITH K_RESPECT K_TO
				T_LROUND column_lists(collists) T_RROUND.

ofwith(with)		::= K_WITH L_NAME(col).
ofwith(ofwith)		::= K_OF L_NAME(col1) K_WITH L_NAME(col2).

column_lists(one)	::= column_list(collist).
column_lists(many)	::= column_lists(collists)
				T_COMMA|K_AND column_list(collist).
column_list(all)	::= T_STAR.
column_list(column)	::= L_NAME(col).

as(none)		::= .
as(some)		::= K_AS L_NAME(name).

from(empty)		::= .
from(nonempty)		::= K_FROM select_tables(tables).

select_tables(one)	::= select_table(t).
select_tables(many)	::= select_tables(ts) T_COMMA select_table(t).

select_table(named)	::= table_name(table) as(name).
select_table(subquery)	::= T_LROUND query(q) T_RROUND as(name).

where(unconditional)	::= .
where(conditional)	::= K_WHERE expression(condition).

/* XXX Allow database-qualified name.  */
table_name(unqualified)	::= L_NAME(name).

group_by(none)		::= .
group_by(some)		::= K_GROUP K_BY expressions(keys).

order_by(none)		::= .
order_by(some)		::= K_ORDER K_BY order_keys(keys).
order_keys(one)		::= order_key(key).
order_keys(many)	::= order_keys(keys) T_COMMA order_key(key).
order_key(k)		::= expression(e) order_sense(s).
order_sense(none)	::= .
order_sense(asc)	::= K_ASC.
order_sense(desc)	::= K_DESC.

limit(none)		::= .
limit(some)		::= K_LIMIT expression(limit).
limit(offset)		::= K_LIMIT expression(limit)
				K_OFFSET expression(offset).
limit(comma)		::= K_LIMIT expression(offset)
				T_COMMA expression(limit).

opt_expressions(none)	::= .
opt_expressions(some)	::= expressions(es).

expressions(one)	::= expression(e).
expressions(many)	::= expressions(es) T_COMMA expression(e).

expression(or)		::= boolean_or(e).

boolean_or(or)		::= boolean_or(l) K_OR boolean_and(r).
boolean_or(and)		::= boolean_and(a).

boolean_and(and)	::= boolean_and(l) K_AND boolean_not(r).
boolean_and(not)	::= boolean_not(n).

boolean_not(not)	::= K_NOT boolean_not(n).
boolean_not(equality)	::= equality(c).

equality(is)		::= equality(l) K_IS ordering(r).
equality(match)		::= equality(l) K_MATCH ordering(r).
equality(like)		::= equality(l) K_LIKE ordering(r).
equality(like_esc)	::= equality(l) K_LIKE ordering(r)
				K_ESCAPE ordering(e).
equality(between)	::= equality(m) K_BETWEEN ordering(l)
				K_AND ordering(r).
equality(notbetween)	::= equality(m) K_NOT K_BETWEEN ordering(l)
				K_AND ordering(r).
equality(in)		::= equality(l) K_IN ordering(r).
equality(isnull)	::= equality(e) K_ISNULL.
equality(notnull)	::= equality(e) K_NOTNULL.
equality(neq)		::= equality(l) T_NEQ ordering(r).
equality(eq)		::= equality(l) T_EQ ordering(r).
equality(ordering)	::= ordering(o).

ordering(lt)		::= ordering(l) T_LT bitwise(r).
ordering(leq)		::= ordering(l) T_LEQ bitwise(r).
ordering(geq)		::= ordering(l) T_GEQ bitwise(r).
ordering(gt)		::= ordering(l) T_GT bitwise(r).
ordering(bitwise)	::= bitwise(b).

bitwise(and)		::= bitwise(l) T_BITAND additive(r).
bitwise(ior)		::= bitwise(l) T_BITIOR additive(r).
bitwise(lshift)		::= bitwise(l) T_LSHIFT additive(r).
bitwise(rshift)		::= bitwise(l) T_RSHIFT additive(r).
bitwise(additive)	::= additive(a).

additive(add)		::= additive(l) T_PLUS multiplicative(r).
additive(sub)		::= additive(l) T_MINUS multiplicative(r).
additive(mult)		::= multiplicative(m).

multiplicative(mul)	::= multiplicative(l) T_STAR concatenative(r).
multiplicative(div)	::= multiplicative(l) T_SLASH concatenative(r).
multiplicative(rem)	::= multiplicative(l) T_PERCENT concatenative(r).
multiplicative(conc)	::= concatenative(c).

concatenative(concat)	::= concatenative(l) T_CONCAT collating(r).
concatenative(collate)	::= collating(c).

collating(collate)	::= collating(e) K_COLLATE L_NAME|L_STRING(c).
collating(bitwise_not)	::= bitwise_not(n).

bitwise_not(not)	::= T_BITNOT bitwise_not(n).
bitwise_not(primary)	::= primary(p).

primary(literal)	::= literal(v).
primary(apply)		::= L_NAME(fn) T_LROUND opt_expressions(es) T_RROUND.
primary(paren)		::= T_LROUND expression(e) T_RROUND.
primary(subquery)	::= T_LROUND query(q) T_RROUND.
primary(cast)		::= K_CAST T_LROUND expression(e)
				K_AS type(t) T_RROUND.
primary(column)		::= L_NAME(col).
primary(tabcol)		::= table_name(tab) T_DOT L_NAME(col).

literal(null)		::= K_NULL.
literal(integer)	::= L_INTEGER(i).
literal(float)		::= L_FLOAT(f).
literal(string)		::= L_STRING(s).

type(name)		::= typename(n).
type(onearg)		::= typename(n) T_LROUND typearg(a) T_RROUND.
type(twoarg)		::= typename(n) T_LROUND typearg(a) T_COMMA
				typearg(b) T_RROUND.
typename(one)		::= L_NAME(n).
typename(many)		::= typename(tn) L_NAME(n).
typearg(unsigned)	::= L_INTEGER(i).
typearg(positive)	::= T_PLUS L_INTEGER(i).
typearg(negative)	::= T_MINUS L_INTEGER(i).
