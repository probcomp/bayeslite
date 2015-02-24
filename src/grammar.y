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
phrases(some)		::= phrases(phrases) phrase1(phrase) T_SEMI.
phrase1(empty)		::= .
phrase1(nonempty)	::= phrase(phrase).
phrase(command)		::= command(c).
phrase(query)		::= query(q).

/* XXX Need database names.  */
command(droptable)	::= K_DROP K_TABLE ifexists(ifexists) L_NAME(name).
command(createtab_as)	::= K_CREATE K_TABLE ifnotexists(ifnotexists)
				L_NAME(name) K_AS query(query).
command(createbtab_csv)	::= K_CREATE K_BTABLE ifnotexists(ifnotexists)
				L_NAME(name) K_FROM L_STRING(file).
command(init_models)	::= K_INITIALIZE L_INTEGER(n) K_MODEL|K_MODELS
				ifnotexists(ifnotexists)
				K_FOR table_name(btable).
command(analyze_models)	::= K_ANALYZE table_name(btable) opt_modelset(models)
				anlimit(anlimit) opt_wait(wait).

ifexists(none)		::= .
ifexists(some)		::= K_IF K_EXISTS.
ifnotexists(none)	::= .
ifnotexists(some)	::= K_IF K_NOT K_EXISTS.

opt_modelset(none)	::= .
opt_modelset(some)	::= K_MODEL|K_MODELS modelset(m).

modelset(one)		::= modelrange(r).
modelset(many)		::= modelset(m) T_COMMA modelrange(r).

modelrange(single)	::= L_INTEGER(modelno).
modelrange(multi)	::= L_INTEGER(minno) T_MINUS L_INTEGER(maxno).

anlimit(iterations)	::= K_FOR L_INTEGER(n) K_ITERATION|K_ITERATIONS.
anlimit(minutes)	::= K_FOR L_INTEGER(n) K_MINUTE|K_MINUTES.
anlimit(seconds)	::= K_FOR L_INTEGER(n) K_SECOND|K_SECONDS.

opt_wait(none)		::= .
opt_wait(some)		::= K_WAIT.

query(select)		::= select(q).
query(estcols)		::= estcols(q).
query(estpaircols)	::= estpaircols(q).
query(estpairrow)	::= estpairrow(q).
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

/*
 * XXX Can we reformulate this elegantly as a SELECT on the columns of
 * the btable?
 */
estcols(e)		::= K_ESTIMATE K_COLUMNS K_FROM table_name(btable)
				where(cond) order_by(ord) limit(lim) as(sav).

/*
 * XXX This is really just a SELECT on the join of the table's list of
 * columns with itself.
 */
estpaircols(e)		::= K_ESTIMATE K_PAIRWISE expression(e)
				K_FROM table_name(btable)
				where(cond) order_by(ord) limit(lim) as(sav).

/*
 * XXX This is really just a SELECT on the join of the table with
 * itself.
 *
 * XXX Support multiple column output?  Not clear that's worthwhile at
 * the moment: the only thing it is sensible to do here right now is
 * SIMILARITY.
 */
estpairrow(e)		::= K_ESTIMATE K_PAIRWISE K_ROW expression(e)
				K_FROM table_name(btable)
				where(cond) order_by(ord) limit(lim) as(sav).

select_quant(distinct)	::= K_DISTINCT.
select_quant(all)	::= K_ALL.
select_quant(default)	::= .

select_columns(one)	::= select_column(c).
select_columns(many)	::= select_columns(cs) T_COMMA select_column(c).

select_column(star)	::= T_STAR.
select_column(qstar)	::= table_name(table) T_DOT T_STAR.
select_column(exp)	::= expression(e) as(name).

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

expression(top)		::= boolean_or(e).

boolean_or(or)		::= boolean_or(l) K_OR boolean_and(r).
boolean_or(and)		::= boolean_and(a).

boolean_and(and)	::= boolean_and(l) K_AND boolean_not(r).
boolean_and(not)	::= boolean_not(n).

boolean_not(not)	::= K_NOT boolean_not(n).
boolean_not(equality)	::= equality(c).

equality(is)		::= equality(l) K_IS ordering(r).
equality(isnot)		::= equality(l) K_IS K_NOT ordering(r).
equality(like)		::= equality(l) K_LIKE ordering(r).
equality(notlike)	::= equality(l) K_NOT K_LIKE ordering(r).
equality(like_esc)	::= equality(l) K_LIKE ordering(r)
				K_ESCAPE ordering(e).
equality(notlike_esc)	::= equality(l) K_NOT K_LIKE ordering(r)
				K_ESCAPE ordering(e).
equality(glob)		::= equality(l) K_GLOB ordering(r).
equality(notglob)	::= equality(l) K_NOT K_GLOB ordering(r).
equality(glob_esc)	::= equality(l) K_GLOB ordering(r)
				K_ESCAPE ordering(e).
equality(notglob_esc)	::= equality(l) K_NOT K_GLOB ordering(r)
				K_ESCAPE ordering(e).
equality(regexp)	::= equality(l) K_REGEXP ordering(r).
equality(notregexp)	::= equality(l) K_NOT K_REGEXP ordering(r).
equality(regexp_esc)	::= equality(l) K_REGEXP ordering(r)
				K_ESCAPE ordering(e).
equality(notregexp_esc)	::= equality(l) K_NOT K_REGEXP ordering(r)
				K_ESCAPE ordering(e).
equality(match)		::= equality(l) K_MATCH ordering(r).
equality(notmatch)	::= equality(l) K_NOT K_MATCH ordering(r).
equality(match_esc)	::= equality(l) K_MATCH ordering(r)
				K_ESCAPE ordering(e).
equality(notmatch_esc)	::= equality(l) K_NOT K_MATCH ordering(r)
				K_ESCAPE ordering(e).
equality(between)	::= equality(m) K_BETWEEN ordering(l)
				K_AND ordering(r).
equality(notbetween)	::= equality(m) K_NOT K_BETWEEN ordering(l)
				K_AND ordering(r).
equality(in)		::= equality(e) K_IN T_LROUND query(q) T_RROUND.
equality(notin)		::= equality(e) K_NOT K_IN T_LROUND query(q) T_RROUND.
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
bitwise_not(bql)	::= bqlfn(b).

/*
 * The BQL functions come in five flavours:
 *
 * (1) Functions of two columns: DEPENDENCE PROBABILITY.
 * (2) Functions of one column: DEPENDENCE PROBABILITY WITH C.
 * (3) Functions of two rows: SIMILARITY WITH RESPECT TO C.
 * (4) Functions of one row: SIMILARITY TO 5 WITH RESPECT TO C.
 * (5) Constants: DEPENDENCE PROBABILITY OF C WITH D.
 *
 * Although constants can appear in any context (subject to the
 * constraint that a table is implied by context -- really, the table
 * should be named in the expression), any context for an expression
 * makes sense with only one flavour of functions.  For example:
 *
 * (1) ESTIMATE PAIRWISE DEPENDENCE PROBABILITY FROM T;
 * (2) ESTIMATE COLUMNS ORDER BY DEPENDENCE PROBABILITY WITH C;
 * (3) SELECT SIMILARITY TO 5 WITH RESPECT TO C FROM T;
 *
 * It makes no sense to say
 *
 *	ESTIMATE COLUMNS FROM T WHERE DEPENDENCE PROBABILITY > 5,
 *
 * because WHERE filters a single set of columns, so there's no second
 * argument for DEPENDENCE PROBABILITY.  Similarly, it makes no sense
 * to say
 *
 *	ESTIMATE COLUMNS FROM T WHERE SIMILARITY TO 5 WITH RESPECT TO C > 5,
 *
 * because SIMILARITY TO 5 WITH RESPECT TO C is a function of a row,
 * not a function of a column.
 *
 * We could invent four different expression nonterminals alike in
 * every way except for which of these options are allowed, but it
 * would require duplicating all the other rules for expressions, so
 * instead we'll do a simple-minded type-check after parsing to detect
 * such mistakes as the above.
 *
 * It is tempting to split the `bqlfn' nonterminal into `bql2colfn',
 * `bql1colfn', `bqlrowfn', `bqlconstfn', but that would lead to
 * ambiguous rules: for example, TYPICALITY can be a function of a row
 * or a function of a column.
 *
 * XXX It would be nice if
 *
 *	SELECT PROBABILITY OF X = 1 - PROBABILITY OF Y = 0 FROM T;
 *
 * worked to mean
 *
 *	SELECT PROBABILITY OF X = (1 - PROBABILITY OF Y = 0) FROM T;
 *
 * so that you could also write, e.g.,
 *
 *	SELECT PROBABILITY OF X = A + B FROM T;
 *
 * with A + B meaning the right-hand side of the equation.
 *
 * However, changing primary(e) to expression(e) on the right-hand
 * side of the bqlfn(prob) rule makes the grammar ambiguous, and the
 * surgery necessary to restore the ambiguity is too much trouble.  So
 * instead we'll reject unparenthesized PROBABILITY OF X = V with
 * other operators altogether and require explicit parentheses until
 * someone wants to do that surgery.
 *
 * XXX Oops -- some restructing of the grammar caused us to cease
 * rejecting unparenthesized PROBABILITY OF X = V with other
 * operators.
 */
bqlfn(predprob_row)	::= K_PREDICTIVE K_PROBABILITY K_OF L_NAME(col).
bqlfn(prob_const)	::= K_PROBABILITY K_OF L_NAME(col) T_EQ primary(e).
bqlfn(prob_1col)	::= K_PROBABILITY K_OF K_VALUE primary(e).
bqlfn(typ_1col_or_row)	::= K_TYPICALITY.
bqlfn(typ_const)	::= K_TYPICALITY K_OF L_NAME(col).
bqlfn(sim_1row)		::= K_SIMILARITY K_TO primary(row) wrt(cols).
bqlfn(sim_2row)		::= K_SIMILARITY wrt(cols).
bqlfn(depprob)		::= K_DEPENDENCE K_PROBABILITY ofwith(cols).
bqlfn(mutinf)		::= K_MUTUAL K_INFORMATION ofwith(cols)
				opt_nsamples(nsamp).
bqlfn(correl)		::= K_CORRELATION ofwith(cols).
bqlfn(infer)		::= K_INFER L_NAME(col) K_CONF primary(cf).
bqlfn(primary)		::= primary(p).

/*
 * Parenthesizing the column lists is not what we did before, but is
 * necessary to avoid ambiguity at the comma: is it another select
 * column, or is it another wrt column?
 */
wrt(none)		::= .
wrt(one)		::= K_WITH K_RESPECT K_TO column_list(collist).
wrt(some)		::= K_WITH K_RESPECT K_TO
				T_LROUND column_lists(collists) T_RROUND.

ofwith(bql_2col)	::= .
ofwith(bql_1col)	::= K_WITH L_NAME(col).
ofwith(bql_const)	::= K_OF L_NAME(col1) K_WITH L_NAME(col2).

opt_nsamples(none)	::= .
opt_nsamples(some)	::= K_USING primary(nsamples) K_SAMPLES.

column_lists(one)	::= column_list(collist).
column_lists(many)	::= column_lists(collists)
				T_COMMA|K_AND column_list(collist).
column_list(all)	::= T_STAR.
column_list(column)	::= L_NAME(col).
/*
 * XXX Should really allow any SELECT on a table of columns.  But
 * until we have that notion, are there any other kinds of subqueries
 * that make sense here?
 */
column_list(subquery)	::= T_LROUND estcols(q) T_RROUND.
/* XXX Saved lists.  */

primary(literal)	::= literal(v).
primary(numpar)		::= L_NUMPAR(n).
primary(nampar)		::= L_NAMPAR(n).
primary(apply)		::= L_NAME(fn) T_LROUND opt_expressions(es) T_RROUND.
primary(paren)		::= T_LROUND expression(e) T_RROUND.
primary(subquery)	::= T_LROUND query(q) T_RROUND.
primary(cast)		::= K_CAST T_LROUND expression(e)
				K_AS type(t) T_RROUND.
primary(exists)		::= K_EXISTS T_LROUND query(q) T_RROUND.
primary(column)		::= L_NAME(col).
primary(tabcol)		::= table_name(tab) T_DOT L_NAME(col).
primary(case)		::= K_CASE opt_case_key(k) opt_case_whens(ws)
				opt_case_else(e) K_END.
/*
 * XXX To do:
 *
 * - RAISE (IGNORE|ROLLBACK|ABORT|FAIL, "message")
 */

opt_case_key(none)	::= .
opt_case_key(some)	::= expression(k).

opt_case_whens(none)	::= .
opt_case_whens(some)	::= opt_case_whens(ws) K_WHEN expression(w)
				K_THEN expression(t).

opt_case_else(none)	::= .
opt_case_else(some)	::= K_ELSE expression(e).

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
