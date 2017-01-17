/*
 *  Copyright (c) 2010-2016, MIT Probabilistic Computing Project
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

bql(start)              ::= phrases(phrases).

phrases(none)           ::= .
phrases(some)           ::= phrases(phrases) phrase_opt(phrase) T_SEMI.
phrase_opt(none)        ::= .
phrase_opt(some)        ::= phrase(phrase).
phrase(command)         ::= command(c).
phrase(query)           ::= query(q).

/*
 * Transactions
 */
command(begin)          ::= K_BEGIN.
command(rollback)       ::= K_ROLLBACK.
command(commit)         ::= K_COMMIT.

/* XXX Need database names.      */

/*
 * SQL Data Definition Language subset
 */
command(createtab_as)   ::= K_CREATE temp_opt(temp) K_TABLE
                                ifnotexists(ifnotexists)
                                table_name(name) K_AS query(query).
command(createtab_csv)   ::= K_CREATE temp_opt(temp) K_TABLE
                                ifnotexists(ifnotexists)
                                table_name(name) K_FROM pathname(csv).
command(droptab)        ::= K_DROP K_TABLE ifexists(ifexists) table_name(name).
command(altertab)       ::= K_ALTER K_TABLE table_name(table)
                                altertab_cmds(cmds).

altertab_cmds(one)      ::= altertab_cmd(cmd).
altertab_cmds(many)     ::= altertab_cmds(cmds) T_COMMA altertab_cmd(cmd).

altertab_cmd(renametab) ::= K_RENAME K_TO table_name(name).
altertab_cmd(renamecol) ::= K_RENAME k_column_opt column_name(old)
                                K_TO column_name(new).

k_column_opt            ::= .
k_column_opt            ::= K_COLUMN.

pathname(p)             ::= L_STRING(name).

/*
 * BQL Model Definition Language
 */
command(guess_schema)   ::= K_GUESS K_SCHEMA K_FOR table_name(table).

command(create_pop)     ::= K_CREATE K_POPULATION ifnotexists(ifnotexists)
                                population_name(name)
                                K_FOR table_name(table)
                                with_schema_opt
                                T_LROUND|T_LCURLY pop_schema(schema)
                                        T_RROUND|T_RCURLY.
command(drop_pop)       ::= K_DROP K_POPULATION ifexists(ifexists)
                                population_name(name).

with_schema_opt ::= .
with_schema_opt ::= K_WITH K_SCHEMA.

/*
 * XXX alter population xyz
 *      rename to pqr
 *      add variable ...
 *      remove variable ...
 *      set statistical type ...
 */

command(alterpop)  ::= K_ALTER K_POPULATION
                                population_name(population) alterpop_cmds(cmds).

alterpop_cmds(one)      ::= alterpop_cmd(cmd).
alterpop_cmds(many)     ::= alterpop_cmds(cmds) T_COMMA alterpop_cmd(cmd).

alterpop_cmd(stattype)  ::= K_SET K_STATTYPES|K_STATTYPE
                                K_FOR|K_OF pop_columns(cols)
                                K_TO stattype(stattype).

alterpop_cmd(addvar)    ::= K_ADD K_VARIABLE column_name(col) stattype_opt(st).

pop_schema(one)         ::= pop_clause(cl).
pop_schema(many)        ::= pop_schema(schema) T_SEMI pop_clause(cl).

pop_clause(empty)       ::= .
pop_clause(column)      ::= column_name(col) stattype(st).
pop_clause(model)       ::= K_MODEL pop_columns(cols) K_AS stattype(st).
pop_clause(ignore)      ::= K_IGNORE pop_columns(cols).
pop_clause(guess)       ::= K_GUESS stattypes_for_opt pop_columns_guess(cols).

stattype_opt(none)      ::= .
stattype_opt(one)       ::= stattype(st).

stattype(st)            ::= L_NAME(name).

pop_columns_guess(star) ::= T_LROUND T_STAR T_RROUND.
pop_columns_guess(list) ::= pop_columns(cols).

pop_columns(one)   ::= column_name(c).
pop_columns(many)  ::= pop_columns(cols) T_COMMA column_name(c).

stattypes_for_opt       ::= .
stattypes_for_opt       ::= K_STATTYPES K_FOR.

/* XXX Temporary generators?  */
command(creategen)      ::= K_CREATE K_GENERATOR|K_METAMODEL
                                ifnotexists(ifnotexists0)
                                generator_name(name)
                                ifnotexists(ifnotexists1)
                                K_FOR population_name(pop)
                                baseline_opt(baseline)
                                runtime_name_opt(metamodel)
                                generator_schema_opt(schema).
command(dropgen)        ::= K_DROP K_GENERATOR|K_METAMODEL ifexists(ifexists)
                                generator_name(name).
command(altergen)       ::= K_ALTER K_GENERATOR|K_METAMODEL
                                generator_name(generator) altergen_cmds(cmds).

altergen_cmds(one)      ::= altergen_cmd(cmd).
altergen_cmds(many)     ::= altergen_cmds(cmds) T_COMMA altergen_cmd(cmd).

altergen_cmd(renamegen) ::= K_RENAME K_TO generator_name(name).

runtime_name_opt(none)          ::= .
runtime_name_opt(one)           ::= K_USING metamodel_name(metamodel).

generator_schema_opt(none)      ::= .
generator_schema_opt(some)      ::= T_LROUND|T_LCURLY generator_schema(s)
                                        T_RROUND|T_RCURLY.

generator_schema(one)   ::= generator_schemum(s).
generator_schema(many)  ::= generator_schema(ss) T_COMMA generator_schemum(s).

generator_schemum(empty)        ::= .
generator_schemum(nonempty)     ::= generator_schemum(s) gs_token(t).
gs_token(comp)                  ::= T_LROUND generator_schemum(s) T_RROUND.
gs_token(prim)                  ::= ANY(t).

/*
 * BQL Model Analysis Language
 */
/* XXX No way to initialize individual models after DROP.  */
command(init_models)    ::= K_INITIALIZE L_INTEGER(n) K_MODEL|K_MODELS
                                ifnotexists(ifnotexists)
                                K_FOR generator_name(generator).
command(analyze_models) ::= K_ANALYZE generator_name(generator)
                                anmodelset_opt(models) anlimit(anlimit)
                                anckpt_opt(anckpt)
                                wait_opt(wait)
                                analysis_program_opt(program).
command(drop_models)    ::= K_DROP K_MODEL|K_MODELS modelset_opt(models)
                                K_FROM generator_name(generator).

temp_opt(none)          ::= .
temp_opt(some)          ::= K_TEMP|K_TEMPORARY.
ifexists(none)          ::= .
ifexists(some)          ::= K_IF K_EXISTS.
ifnotexists(none)       ::= .
ifnotexists(some)       ::= K_IF K_NOT K_EXISTS.

anmodelset_opt(none)    ::= .
anmodelset_opt(some)    ::= K_MODEL|K_MODELS modelset(m).

/* XXX Hackery for WITH BASELINE  */
baseline_opt(none)      ::= .
baseline_opt(some)      ::= K_WITH K_BASELINE baseline_name(baseline)
                                param_opt(params).

baseline_name(bl)       ::= L_NAME(name).

param_opt(none)         ::= .
param_opt(some)         ::= T_LSQUARE params(ps) T_RSQUARE.

params(one)             ::= param(param).
params(many)            ::= params(params) T_COMMA param(param).

param(p)                ::= L_NAME(p) T_EQ literal(v).
/* XXX Temporary generators?  */

modelset_opt(none)      ::= .
modelset_opt(some)      ::= modelset(m).

modelset(one)           ::= modelrange(r).
modelset(many)          ::= modelset(m) T_COMMA modelrange(r).

modelrange(single)      ::= L_INTEGER(modelno).
modelrange(multi)       ::= L_INTEGER(minno) T_MINUS L_INTEGER(maxno).

anlimit(one)      ::= K_FOR anduration(duration).
anlimit(two)      ::= K_FOR anduration(duration0) K_OR anduration(duration1).

anckpt_opt(none)        ::= .
anckpt_opt(some)        ::= K_CHECKPOINT anduration(duration).

anduration(iterations)  ::= L_INTEGER(n) K_ITERATION|K_ITERATIONS.
anduration(minutes)     ::= L_INTEGER(n) K_MINUTE|K_MINUTES.
anduration(seconds)     ::= L_INTEGER(n) K_SECOND|K_SECONDS.

wait_opt(none)          ::= .
wait_opt(some)          ::= K_WAIT.

analysis_program_opt(none)      ::= .
analysis_program_opt(some)      ::= T_LROUND analysis_program(p) T_RROUND.
analysis_program(empty)         ::= .
analysis_program(nonempty)      ::= analysis_program(p) analysis_token(t).
analysis_token(compound)        ::= T_LROUND analysis_program(p) T_RROUND.
analysis_token(primitive)       ::= ANY(t).

simulate(s)             ::= K_SIMULATE simulate_columns(cols)
                                K_FROM population_name(population)
                                modelledby_opt(generator)
                                given_opt(constraints)
                                limit(lim)
                                accuracy_opt(acc).
simulate(nolimit)       ::= K_SIMULATE simulate_columns(cols)
                                K_FROM population_name(population)
                                modelledby_opt(generator)
                                given_opt(constraints).

simulate_columns(one)   ::= simulate_column(col).
simulate_columns(many)  ::= simulate_columns(cols) T_COMMA
                                simulate_column(col).

simulate_column(c)      ::= bqlfn(col) as(name).

given_opt(none)         ::= .
given_opt(some)         ::= K_GIVEN constraints(constraints).
constraints(one)        ::= constraint(c).
constraints(many)       ::= constraints(cs) T_COMMA constraint(c).
constraint(c)           ::= column_name(col) T_EQ expression(value).
constraints_opt(none)   ::= .
constraints_opt(some)   ::= constraints(cs).

simulate(models)  ::=
        K_SIMULATE simulate_columns(cols)
        K_FROM K_MODELS K_OF population_name(population)
        modelledby_opt(generator).

/*
 * Queries
 */
query(select)           ::= select(q).
query(estimate)         ::= estimate(q).
query(estby)            ::= estby(q).
query(estcol)           ::= estcol(q).          /* Legacy syntax.  */
query(estpairrow)       ::= estpairrow(q).      /* Legacy syntax.  */
query(estpaircol)       ::= estpaircol(q).      /* Legacy syntax.  */
query(infer)            ::= infer(q).
query(simulate)         ::= simulate(q).
/*
query(estimate_pairwise_row)
                        ::= estimate_pairwise_row(q).
query(create_column_list)
                        ::= create_column_list(q).
*/

/* XXX Support WITH ... SELECT ... (i.e., common table expressions).  */
select(s)               ::= K_SELECT select_quant(quant) select_columns(cols)
                                from_sel_opt(tabs)
                                where(cond)
                                group_by(grouping)
                                order_by(ord)
                                limit_opt(lim).

estimate(e)             ::= K_ESTIMATE select_quant(quant) select_columns(cols)
                                from_est(tabs)
                                modelledby_opt(generator)
                                where(cond)
                                group_by(grouping)
                                order_by(ord)
                                limit_opt(lim).

estcol(e)               ::= K_ESTIMATE K_COLUMNS error T_SEMI.
estpairrow(e)           ::= K_ESTIMATE K_PAIRWISE K_ROW error T_SEMI.
estpaircol(e)           ::= K_ESTIMATE K_PAIRWISE error T_SEMI.

estby(e)                ::= K_ESTIMATE select_quant(quant) select_columns(cols)
                                K_BY|K_WITHIN population_name(population)
                                modelledby_opt(generator).

infer(auto)             ::= K_INFER infer_auto_columns(cols)
                                withconf_opt(conf)
                                nsamples_opt(nsamp)
                                K_FROM population_name(population)
                                modelledby_opt(generator)
                                where(cond) group_by(grouping) order_by(ord)
                                limit_opt(lim).
infer(explicit)         ::= K_INFER K_EXPLICIT infer_exp_columns(cols)
                                K_FROM population_name(population)
                                modelledby_opt(generator)
                                where(cond) group_by(grouping) order_by(ord)
                                limit_opt(lim).

infer_auto_columns(one)         ::= infer_auto_column(c).
infer_auto_columns(many)        ::= infer_auto_columns(cs) T_COMMA
                                        infer_auto_column(c).

infer_auto_column(all)          ::= T_STAR.
infer_auto_column(one)          ::= column_name(col) as(name).

withconf_opt(none)      ::= .
withconf_opt(some)      ::= withconf(conf).

withconf(conf)          ::= K_WITH K_CONFIDENCE primary(conf).

infer_exp_columns(one)  ::= infer_exp_column(c).
infer_exp_columns(many) ::= infer_exp_columns(cs) T_COMMA
                                infer_exp_column(c).

infer_exp_column(sel)   ::= select_column(c).
infer_exp_column(pred)  ::= K_PREDICT column_name(col) as(name)
                                conf_opt(confname)
                                nsamples_opt(nsamp).

conf_opt(none)  ::= .
conf_opt(some)  ::= K_CONFIDENCE column_name(confname).

select_quant(distinct)  ::= K_DISTINCT.
select_quant(all)       ::= K_ALL.
select_quant(default)   ::= .

select_columns(one)     ::= select_column(c).
select_columns(many)    ::= select_columns(cs) T_COMMA select_column(c).

select_column(star)     ::= T_STAR.
select_column(qstar)    ::= table_name(table) T_DOT T_STAR.
select_column(qsub)     ::= table_name(table) T_DOT T_LROUND query(q) T_RROUND.
select_column(exp)      ::= expression(e) as(name).

as(none)                ::= .
as(some)                ::= K_AS L_NAME(name).

from_sel_opt(empty)     ::= .
from_sel_opt(nonempty)  ::= K_FROM select_tables(tables).

from_est(row)           ::= K_FROM population_name(name).
from_est(pairrow)       ::= K_FROM K_PAIRWISE population_name(name).
from_est(col)           ::= K_FROM K_COLUMNS|K_VARIABLES
                                K_OF population_name(name).
from_est(paircol)       ::= K_FROM K_PAIRWISE K_COLUMNS|K_VARIABLES K_OF
                                population_name(name) for(subcols).

modelledby_opt(none)    ::= .
modelledby_opt(some)    ::= K_MODELED|K_MODELLED K_BY generator_name(gen).

/* XXX Allow all kinds of joins.  */
select_tables(one)      ::= select_table(t).
select_tables(many)     ::= select_tables(ts) T_COMMA select_table(t).

select_table(named)     ::= table_name(table) as(name).
select_table(subquery)  ::= T_LROUND query(q) T_RROUND as(name).

for(none)               ::= .
for(one)                ::= K_FOR column_lists(collist).

where(unconditional)    ::= .
where(conditional)      ::= K_WHERE expression(condition).

/* XXX Allow database-qualified names.  */
column_name(cn)         ::= L_NAME(name).
generator_name(unqualified) ::= L_NAME(name).
metamodel_name(mn)      ::= L_NAME(name).
population_name(pn)     ::= L_NAME(name).
table_name(unqualified) ::= L_NAME(name).

group_by(none)          ::= .
group_by(some)          ::= K_GROUP K_BY expressions(keys).
group_by(having)        ::= K_GROUP K_BY expressions(keys)
                                K_HAVING expression(cond).

order_by(none)          ::= .
order_by(some)          ::= K_ORDER K_BY order_keys(keys).
order_keys(one)         ::= order_key(key).
order_keys(many)        ::= order_keys(keys) T_COMMA order_key(key).
order_key(k)            ::= expression(e) order_sense(s).
order_sense(none)       ::= .
order_sense(asc)        ::= K_ASC.
order_sense(desc)       ::= K_DESC.

accuracy_opt(none)      ::= .
accuracy_opt(some)      ::= K_ACCURACY L_INTEGER(acc).

limit_opt(none)         ::= .
limit_opt(some)         ::= limit(lim).

limit(n)                ::= K_LIMIT expression(limit).
limit(offset)           ::= K_LIMIT expression(limit)
                                K_OFFSET expression(offset).
limit(comma)            ::= K_LIMIT expression(offset)
                                T_COMMA expression(limit).

expressions_opt(none)   ::= .
expressions_opt(some)   ::= expressions(es).

expressions(one)        ::= expression(e).
expressions(many)       ::= expressions(es) T_COMMA expression(e).

expression(top)         ::= boolean_or(e).

boolean_or(or)          ::= boolean_or(l) K_OR boolean_and(r).
boolean_or(and)         ::= boolean_and(a).

boolean_and(and)        ::= boolean_and(l) K_AND boolean_not(r).
boolean_and(not)        ::= boolean_not(n).

boolean_not(not)        ::= K_NOT boolean_not(n).
boolean_not(equality)   ::= equality(c).

equality(is)            ::= equality(l) K_IS ordering(r).
equality(isnot)         ::= equality(l) K_IS K_NOT ordering(r).
equality(like)          ::= equality(l) K_LIKE ordering(r).
equality(notlike)       ::= equality(l) K_NOT K_LIKE ordering(r).
equality(like_esc)      ::= equality(l) K_LIKE ordering(r)
                                K_ESCAPE ordering(e).
equality(notlike_esc)   ::= equality(l) K_NOT K_LIKE ordering(r)
                                K_ESCAPE ordering(e).
equality(glob)          ::= equality(l) K_GLOB ordering(r).
equality(notglob)       ::= equality(l) K_NOT K_GLOB ordering(r).
equality(glob_esc)      ::= equality(l) K_GLOB ordering(r)
                                K_ESCAPE ordering(e).
equality(notglob_esc)   ::= equality(l) K_NOT K_GLOB ordering(r)
                                K_ESCAPE ordering(e).
equality(regexp)        ::= equality(l) K_REGEXP ordering(r).
equality(notregexp)     ::= equality(l) K_NOT K_REGEXP ordering(r).
equality(regexp_esc)    ::= equality(l) K_REGEXP ordering(r)
                                K_ESCAPE ordering(e).
equality(notregexp_esc) ::= equality(l) K_NOT K_REGEXP ordering(r)
                                K_ESCAPE ordering(e).
equality(match)         ::= equality(l) K_MATCH ordering(r).
equality(notmatch)      ::= equality(l) K_NOT K_MATCH ordering(r).
equality(match_esc)     ::= equality(l) K_MATCH ordering(r)
                                K_ESCAPE ordering(e).
equality(notmatch_esc)  ::= equality(l) K_NOT K_MATCH ordering(r)
                                K_ESCAPE ordering(e).
equality(between)       ::= equality(m) K_BETWEEN ordering(l)
                                K_AND ordering(r).
equality(notbetween)    ::= equality(m) K_NOT K_BETWEEN ordering(l)
                                K_AND ordering(r).
equality(in)            ::= equality(e) K_IN T_LROUND query(q) T_RROUND.
equality(notin)         ::= equality(e) K_NOT K_IN T_LROUND query(q) T_RROUND.
equality(isnull)        ::= equality(e) K_ISNULL.
equality(notnull)       ::= equality(e) K_NOTNULL.
equality(neq)           ::= equality(l) T_NEQ ordering(r).
equality(eq)            ::= equality(l) T_EQ ordering(r).
equality(ordering)      ::= ordering(o).

ordering(lt)            ::= ordering(l) T_LT bitwise(r).
ordering(leq)           ::= ordering(l) T_LEQ bitwise(r).
ordering(geq)           ::= ordering(l) T_GEQ bitwise(r).
ordering(gt)            ::= ordering(l) T_GT bitwise(r).
ordering(bitwise)       ::= bitwise(b).

bitwise(and)            ::= bitwise(l) T_BITAND additive(r).
bitwise(ior)            ::= bitwise(l) T_BITIOR additive(r).
bitwise(lshift)         ::= bitwise(l) T_LSHIFT additive(r).
bitwise(rshift)         ::= bitwise(l) T_RSHIFT additive(r).
bitwise(additive)       ::= additive(a).

additive(add)           ::= additive(l) T_PLUS multiplicative(r).
additive(sub)           ::= additive(l) T_MINUS multiplicative(r).
additive(mult)          ::= multiplicative(m).

multiplicative(mul)     ::= multiplicative(l) T_STAR concatenative(r).
multiplicative(div)     ::= multiplicative(l) T_SLASH concatenative(r).
multiplicative(rem)     ::= multiplicative(l) T_PERCENT concatenative(r).
multiplicative(conc)    ::= concatenative(c).

concatenative(concat)   ::= concatenative(l) T_CONCAT collating(r).
concatenative(collate)  ::= collating(c).

collating(collate)      ::= collating(e) K_COLLATE L_NAME|L_STRING(c).
collating(unary)        ::= unary(u).

unary(bitwise_not)      ::= T_BITNOT unary(u).
unary(minus)            ::= T_MINUS unary(u).
unary(plus)             ::= T_PLUS unary(u).
unary(bql)              ::= bqlfn(b).

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
 * (1) ESTIMATE DEPENDENCE PROBABILITY PAIRWISE COLUMNS OF FROM T;
 * (2) ESTIMATE * FROM COLUMNS OF T ORDER BY DEPENDENCE PROBABILITY WITH C;
 * (3) SELECT SIMILARITY TO 5 WITH RESPECT TO C FROM T;
 *
 * It makes no sense to say
 *
 *      ESTIMATE * FROM COLUMNS OF T WHERE DEPENDENCE PROBABILITY > 5,
 *
 * because WHERE filters a single set of columns, so there's no second
 * argument for DEPENDENCE PROBABILITY.  Similarly, it makes no sense
 * to say
 *
 *      ESTIMATE * FROM COLUMNS OF T
 *              WHERE SIMILARITY TO 5 WITH RESPECT TO C > 5,
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
 * `bql1colfn', `bqlrowfn', `bqlconstfn', but we used to have an
 * ambiguity here.
 *
 * XXX It would be nice if
 *
 *      SELECT PROBABILITY OF X = 1 - PROBABILITY OF Y = 0 FROM T;
 *
 * worked to mean
 *
 *      SELECT PROBABILITY OF X = (1 - PROBABILITY OF Y = 0) FROM T;
 *
 * so that you could also write, e.g.,
 *
 *      SELECT PROBABILITY OF X = A + B FROM T;
 *
 * with A + B meaning the right-hand side of the equation.
 *
 * However, changing primary(e) to expression(e) on the right-hand
 * side of the bqlfn(prob) rule makes the grammar ambiguous, and the
 * surgery necessary to resolve the ambiguity is too much trouble.  So
 * instead we'll reject unparenthesized PROBABILITY OF X = V with
 * other operators altogether and require explicit parentheses until
 * someone wants to do that surgery.
 *
 * XXX Oops -- some restructing of the grammar caused us to cease
 * rejecting unparenthesized PROBABILITY OF X = V with other
 * operators.
 */
bqlfn(predprob_row)     ::= K_PREDICTIVE K_PROBABILITY K_OF column_name(col).
bqlfn(prob_const)       ::= K_PROBABILITY K_OF column_name(col)
                                T_EQ unary(e).
bqlfn(jprob_const)      ::= K_PROBABILITY K_OF
                                T_LROUND constraints_opt(targets) T_RROUND.
bqlfn(condprob_const)   ::= K_PROBABILITY K_OF column_name(col)
                                T_EQ primary(e)
                                K_GIVEN T_LROUND constraints_opt(constraints)
                                        T_RROUND.
bqlfn(condjprob_const)  ::= K_PROBABILITY K_OF
                                T_LROUND constraints_opt(targets) T_RROUND
                                K_GIVEN T_LROUND constraints_opt(constraints)
                                        T_RROUND.
/* XXX Givens for PROBABILITY OF VALUE function of columns?      */
bqlfn(prob_1col)        ::= K_PROBABILITY K_OF K_VALUE unary(e).
bqlfn(condprob_1col)    ::= K_PROBABILITY K_OF K_VALUE primary(e)
                                K_GIVEN T_LROUND constraints_opt(constraints)
                                        T_RROUND.
bqlfn(sim_1row)         ::= K_SIMILARITY K_TO
                                T_LROUND expression(cond) T_RROUND
                                wrt(cols).
bqlfn(sim_2row)         ::= K_SIMILARITY wrt(cols).
bqlfn(depprob)          ::= K_DEPENDENCE K_PROBABILITY ofwith(cols).

bqlfn(mutinf)           ::= K_MUTUAL K_INFORMATION ofwith(cols)
                                nsamples_opt(nsamp).
bqlfn(cmutinf)          ::= K_MUTUAL K_INFORMATION ofwith(cols)
                                K_GIVEN T_LROUND mi_constraints(constraints)
                                        T_RROUND
                                nsamples_opt(nsamp).

mi_constraint(e)        ::= column_name(col) T_EQ expression(value).
mi_constraint(m)        ::= column_name(col).
mi_constraints(one)     ::= mi_constraint(c).
mi_constraints(many)    ::= mi_constraints(cs) T_COMMA mi_constraint(c).


bqlfn(correl)           ::= K_CORRELATION ofwith(cols).
bqlfn(correl_pval)      ::= K_CORRELATION K_PVALUE ofwith(cols).
bqlfn(predict)          ::= K_PREDICT column_name(col) withconf(conf)
                                nsamples_opt(nsamp).
bqlfn(primary)          ::= primary(p).

/*
 * Parenthesizing the column lists is not what we did before, but is
 * necessary to avoid ambiguity at the comma: is it another select
 * column, or is it another wrt column?
 */
wrt(none)               ::= .
wrt(one)                ::= K_WITH K_RESPECT K_TO column_list(collist).
wrt(some)               ::= K_WITH K_RESPECT K_TO
                                T_LROUND column_lists(collists) T_RROUND.

ofwith(bql_2col)        ::= .
ofwith(bql_1col)        ::= K_WITH column_name(col).
ofwith(bql_const)       ::= K_OF column_name(col1) K_WITH column_name(col2).

nsamples_opt(none)      ::= .
nsamples_opt(some)      ::= K_USING primary(nsamples) K_SAMPLES.

column_lists(one)       ::= column_list(collist).
column_lists(many)      ::= column_lists(collists)
                                T_COMMA|K_AND column_list(collist).
column_list(all)        ::= T_STAR.
column_list(column)     ::= column_name(col).
/*
 * XXX Should really allow any SELECT on a table of columns.  But
 * until we have that notion, are there any other kinds of subqueries
 * that make sense here?
 */
column_list(subquery)   ::= T_LROUND query(q) T_RROUND.

primary(literal)        ::= literal(v).
primary(numpar)         ::= L_NUMPAR(n).
primary(nampar)         ::= L_NAMPAR(n).
primary(apply)          ::= L_NAME(fn) T_LROUND expressions_opt(es) T_RROUND.
primary(apply_distinct) ::= L_NAME(fn) T_LROUND K_DISTINCT expressions_opt(es)
                                T_RROUND.
primary(apply_star)     ::= L_NAME(fn) T_LROUND T_STAR T_RROUND.
primary(paren)          ::= T_LROUND expression(e) T_RROUND.
primary(subquery)       ::= T_LROUND query(q) T_RROUND.
primary(cast)           ::= K_CAST T_LROUND expression(e)
                                K_AS type(t) T_RROUND.
primary(exists)         ::= K_EXISTS T_LROUND query(q) T_RROUND.
primary(column)         ::= column_name(col).
primary(tabcol)         ::= table_name(tab) T_DOT column_name(col).
primary(case)           ::= K_CASE case_key_opt(k) case_whens_opt(ws)
                                case_else_opt(e) K_END.
/*
 * XXX To do:
 *
 * - RAISE (IGNORE|ROLLBACK|ABORT|FAIL, "message")
 */

case_key_opt(none)      ::= .
case_key_opt(some)      ::= expression(k).

case_whens_opt(none)    ::= .
case_whens_opt(some)    ::= case_whens_opt(ws) K_WHEN expression(w)
                                K_THEN expression(t).

case_else_opt(none)     ::= .
case_else_opt(some)     ::= K_ELSE expression(e).

literal(null)           ::= K_NULL.
literal(integer)        ::= L_INTEGER(i).
literal(float)          ::= L_FLOAT(f).
literal(string)         ::= L_STRING(s).

type(name)              ::= typename(n).
type(onearg)            ::= typename(n) T_LROUND typearg(a) T_RROUND.
type(twoarg)            ::= typename(n) T_LROUND typearg(a) T_COMMA
                                typearg(b) T_RROUND.
typename(one)           ::= L_NAME(n).
typename(many)          ::= typename(tn) L_NAME(n).
typearg(unsigned)       ::= L_INTEGER(i).
typearg(positive)       ::= T_PLUS L_INTEGER(i).
typearg(negative)       ::= T_MINUS L_INTEGER(i).

/*
 * XXX For some reason, putting CASE and WHEN here break the parser.
 * Needs further analysis.
 *
 *      grep -o 'K_[A-Z_]*' | awk '{ printf("\t%s\n", $1) }' | sort -u
 */
%fallback L_NAME
        K_ACCURACY
        K_ADD
        K_ALL
        K_ALTER
        K_ANALYZE
        K_AND
        K_AS
        K_ASC
        K_BASELINE
        K_BEGIN
        K_BETWEEN
        K_BTABLE
        K_BY
        /* K_CASE */
        K_CAST
        K_CHECKPOINT
        K_COLLATE
        K_COLUMN
        K_COLUMNS
        K_COMMIT
        K_CONF
        K_CONFIDENCE
        K_CORRELATION
        K_CREATE
        K_DEFAULT
        K_DEPENDENCE
        K_DESC
        K_DISTINCT
        K_DROP
        K_ELSE
        K_END
        K_ESCAPE
        K_ESTIMATE
        K_EXISTS
        K_EXPLICIT
        K_FOR
        K_FROM
        K_GENERATOR
        K_GIVEN
        K_GLOB
        K_GROUP
        K_GUESS
        K_HAVING
        K_IF
        K_IGNORE
        K_IN
        K_INFER
        K_INFORMATION
        K_INITIALIZE
        K_IS
        K_ISNULL
        K_ITERATION
        K_ITERATIONS
        K_LATENT
        K_LIKE
        K_LIMIT
        K_MATCH
        K_METAMODEL
        K_MINUTE
        K_MINUTES
        K_MODEL
        K_MODELED
        K_MODELLED
        K_MODELS
        K_MUTUAL
        K_NOT
        K_NOTNULL
        K_NULL
        K_OF
        K_OFFSET
        K_OR
        K_ORDER
        K_PAIRWISE
        K_POPULATION
        K_PREDICT
        K_PREDICTIVE
        K_PROBABILITY
        K_PVALUE
        K_REGEXP
        K_RENAME
        K_RESPECT
        K_ROLLBACK
        K_ROW
        K_SAMPLES
        K_SCHEMA
        K_SECOND
        K_SECONDS
        K_SELECT
        K_SET
        K_SIMILARITY
        K_SIMULATE
        K_STATTYPE
        K_STATTYPES
        K_TABLE
        K_TEMP
        K_TEMPORARY
        K_THEN
        K_TO
        K_UNSET
        K_USING
        K_VALUE
        K_VARIABLE
        K_VARIABLES
        K_WAIT
        /* K_WHEN */
        K_WHERE
        K_WITH
        K_WITHIN
        .

/*
 * Wildcard token, matches anything else.
 */
%wildcard ANY.
