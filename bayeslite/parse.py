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

"""BQL parser front end."""

import StringIO

import bayeslite.ast as ast
import bayeslite.grammar as grammar
import bayeslite.scan as scan
from bayeslite.exception import BQLParseError

def parse_bql_phrases(scanner):
    semantics = BQLSemantics()
    parser = grammar.Parser(semantics)
    while not semantics.failed:
        token = scanner.read()
        semantics.context.append(token)
        if token[0] == -1:      # error
            semantics.syntax_error(token)
        else:
            if token[0] == 0:   # EOF
                # Implicit ; at EOF.
                parser.feed((grammar.T_SEMI, ''))
            parser.feed(token)
        if semantics.phrase is not None:
            phrase = semantics.phrase
            semantics.phrase = None
            if 0 < len(semantics.errors):
                # Keep parsing in order to detect more errors, but
                # don't yield any broken phrases in case the caller
                # will try to process them before we finish parsing
                # the whole thing.
                continue
            if 0 < scanner.n_numpar:
                n_numpar = scanner.n_numpar
                nampar_map = scanner.nampar_map
                yield ast.Parametrized(phrase, n_numpar, nampar_map)
            else:
                yield phrase
        if token[0] == 0:       # EOF
            break
    if 0 < len(semantics.errors):
        raise BQLParseError(semantics.errors)
    if semantics.failed:
        raise BQLParseError(['parse failed mysteriously!'])

def parse_bql_string_pos(string):
    """Yield ``(phrase, pos)`` for each BQL phrase in `string`.

    `phrase` is the parsed AST.  `pos` is zero-based index of the code
    point at which `phrase` starts.
    """
    scanner = scan.BQLScanner(StringIO.StringIO(string), '(string)')
    phrases = parse_bql_phrases(scanner)
    # XXX Don't dig out internals of scanner: fix plex to have a
    # public API for finding the current position.
    return ((phrase, scanner.cur_pos) for phrase in phrases)

def parse_bql_string_pos_1(string):
    """Return ``(phrase, pos)`` for the first BQL phrase in `string`.

    May not report parse errors afterward.
    """
    for phrase, pos in parse_bql_string_pos(string):
        return (phrase, pos)
    return None

def parse_bql_string(string):
    """Yield each parsed BQL phrase AST in `string`."""
    return (phrase for phrase, _pos in parse_bql_string_pos(string))

def bql_string_complete_p(string):
    """True if `string` has at least one complete BQL phrase or error.

    False if empty or if the last BQL phrase is incomplete.
    """
    scanner = scan.BQLScanner(StringIO.StringIO(string), '(string)')
    semantics = BQLSemantics()
    parser = grammar.Parser(semantics)
    nonsemi = False
    while not semantics.failed:
        token = scanner.read()
        if token[0] == -1:      # error
            # Say it's complete so the caller will try to parse it and
            # choke on the error.
            return True
        elif token[0] == 0:
            # EOF.  Hope we have a complete phrase.
            break
        elif token[0] != grammar.T_SEMI:
            # Got a non-semicolon token.  Clear any previous phrase,
            # if we had one.
            nonsemi = True
            semantics.phrase = None
        parser.feed(token)
    if 0 < len(semantics.errors):
        return True
    if semantics.failed:
        return True
    return (not nonsemi) or (semantics.phrase is not None)

class BQLSemantics(object):
    def __init__(self):
        self.phrase = None
        self.context = []
        self.errors = []
        self.failed = False

    def accept(self):
        pass
    def parse_failed(self):
        self.failed = True
    def syntax_error(self, (number, text)):
        # XXX Adapt lemonade to help us identify what the allowed
        # subsequent tokens are.
        #
        # XXX Record source position.
        if number == -1:        # error
            self.errors.append('Skipping bad token: %s' % (text,))
        else:
            self.errors.append('Syntax error near [%s] after [%s]' % (
                text, ' '.join([str(tok) for (pos, tok) in self.context[:-1]])))

    def p_bql_start(self, phrases):
        pass
    def p_phrases_none(self):
        pass
    def p_phrases_some(self, phrases, phrase):
        pass

    def p_phrase_opt_none(self):
        pass
    def p_phrase_opt_some(self, phrase):
        assert self.phrase is None
        self.phrase = phrase

    def p_phrase_command(self, c):              return c
    def p_phrase_query(self, q):                return q

    # Transactions
    def p_command_begin(self):                  return ast.Begin()
    def p_command_rollback(self):               return ast.Rollback()
    def p_command_commit(self):                 return ast.Commit()

    # SQL Data Definition Language subset
    def p_command_createtab_as(self, temp, ifnotexists, name, query):
        return ast.CreateTabAs(temp, ifnotexists, name, query)
    def p_command_createtab_csv(self, temp, ifnotexists, name, csv):
        return ast.CreateTabCsv(temp, ifnotexists, name, csv)
    def p_command_droptab(self, ifexists, name):
        return ast.DropTab(ifexists, name)
    def p_command_altertab(self, table, cmds):
        return ast.AlterTab(table, cmds)

    def p_pathname_p(self, name):               return name

    def p_altertab_cmds_one(self, cmd):         return [cmd]
    def p_altertab_cmds_many(self, cmds, cmd):  cmds.append(cmd); return cmds
    def p_altertab_cmd_renametab(self, name):
        return ast.AlterTabRenameTab(name)
    def p_altertab_cmd_renamecol(self, old, new):
        return ast.AlterTabRenameCol(old, new)

    # BQL Model Definition Language
    def p_command_guess_schema(self, table):
        return ast.GuessSchema(table)

    def p_command_create_pop(self, ifnotexists, name, table, schema):
        return ast.CreatePop(ifnotexists, name, table, schema)
    def p_command_create_pop_implicit(self, ifnotexists, table, schema):
        return ast.CreatePop(ifnotexists, None, table, schema)
    def p_command_drop_pop(self, ifexists, name):
        return ast.DropPop(ifexists, name)
    def p_command_alterpop(self, population, cmds):
        return ast.AlterPop(population, cmds)

    def p_alterpop_cmds_one(self, cmd):         return [cmd]
    def p_alterpop_cmds_many(self, cmds, cmd):  cmds.append(cmd); return cmds
    def p_alterpop_cmd_renamepop(self, name):
        return ast.AlterPopRenamePop(name)
    def p_alterpop_cmd_stattype(self, cols, stattype):
        return ast.AlterPopStatType(cols, stattype)
    def p_alterpop_cmd_addvar(self, col, st):
        return ast.AlterPopAddVar(col, st)

    def p_pop_schema_one(self, cl):             return [cl]
    def p_pop_schema_many(self, schema, cl):
        if cl:
            schema.append(cl);
        return schema

    def p_pop_clause_empty(self):            return None
    def p_pop_clause_column(self, col, st):  return ast.PopModelVars([col], st)
    def p_pop_clause_stattype(self, cols, st): return ast.PopModelVars(cols, st)
    def p_pop_clause_ignore(self, cols):     return ast.PopIgnoreVars(cols)
    def p_pop_clause_guess(self, cols):      return ast.PopGuessVars(cols)

    def p_model_opt_none(self):              return None
    def p_model_opt_one(self):               return None
    def p_as_opt_none(self):                 return None
    def p_as_opt_one(self):                  return None

    def p_stattype_opt_none(self):           return None
    def p_stattype_opt_one(self, st):        return st
    def p_stattype_st(self, name):           return name

    def p_pop_columns_one(self, c):          return [c]
    def p_pop_columns_many(self, cols, c):   cols.append(c); return cols

    def p_pop_columns_guess_star(self):       return '*'
    def p_pop_columns_guess_list(self, cols): return cols

    def p_command_creategen(self, ifnotexists, name, pop, backend, schema):
        return ast.CreateGen(name, ifnotexists, pop, backend, schema)
    def p_command_creategen_implicit(self, ifnotexists, pop, backend, schema):
        return ast.CreateGen(None, ifnotexists, pop, backend, schema)
    def p_command_dropgen(self, ifexists, name):
        return ast.DropGen(ifexists, name)
    def p_command_altergen(self, generator, models, cmds):
        return ast.AlterGen(generator, models, cmds)

    def p_altergen_cmds_one(self, cmd):         return [cmd]
    def p_altergen_cmds_many(self, cmds, cmd):  cmds.append(cmd); return cmds
    def p_altergen_cmd_renamegen(self, name):
        return ast.AlterGenRenameGen(name)
    def p_altergen_cmd_generic(self, s):
        return ast.AlterGenGeneric(s)

    def p_backend_name_opt_none(self):            return None
    def p_backend_name_opt_one(self, backend):    return backend

    def p_generator_schema_opt_none(self):      return [[]]
    def p_generator_schema_opt_some(self, s):   return s
    def p_generator_schema_one(self, s):        return [s]
    def p_generator_schema_many(self, ss, s):   ss.append(s); return ss
    def p_generator_schemum_empty(self):        return []
    def p_generator_schemum_nonempty(self, s, t): s.append(t); return s
    def p_gs_token_prim(self, t):               return t
    def p_gs_token_comp(self, s):               return s

    def p_stattype_s(self, name):
        return name

    # BQL Model Analysis Language
    def p_command_init_models(self, n, ifnotexists, generator):
        return ast.InitModels(ifnotexists, generator, n)
    def p_command_analyze_models(
            self, generator, models, anlimit, anckpt, program):
        iters = [lim[1] for lim in anlimit if lim and lim[0] == 'iterations']
        secs = [lim[1] for lim in anlimit if lim and lim[0] == 'seconds']
        iterations = min(iters) if iters else None
        seconds = min(secs) if secs else None
        ckpt_iterations = None
        ckpt_seconds = None
        if anckpt is not None:
            ckpt_iterations = anckpt[1] if anckpt[0] == 'iterations' else None
            ckpt_seconds = anckpt[1] if anckpt[0] == 'seconds' else None
        return ast.AnalyzeModels(generator, models, iterations, seconds,
            ckpt_iterations, ckpt_seconds, program)
    def p_command_drop_models(self, models, generator):
        return ast.DropModels(generator, models)

    def p_temp_opt_none(self):                  return False
    def p_temp_opt_some(self):                  return True
    def p_ifexists_none(self):                  return False
    def p_ifexists_some(self):                  return True
    def p_ifnotexists_none(self):               return False
    def p_ifnotexists_some(self):               return True

    def p_anmodelset_opt_none(self):            return None
    def p_anmodelset_opt_some(self, m):         return sorted(m)
    def p_anmodelset_matched_opt_none(self):    return None
    def p_anmodelset_matched_opt_some(self, m): return sorted(m)
    def p_modelset_opt_none(self):              return None
    def p_modelset_opt_some(self, m):           return sorted(m)
    def p_modelset_one(self, r):                return r
    def p_modelset_many(self, m, r):            m += r; return m
    def p_modelrange_single(self, modelno):     return [modelno]
    def p_modelrange_multi(self, minno, maxno): return range(minno, maxno + 1)

    def p_anlimit_one(self, duration):             return (duration, None)
    def p_anlimit_two(self, duration0, duration1): return (duration0, duration1)
    def p_anckpt_opt_none(self):                return None
    def p_anckpt_opt_some(self, duration):      return duration

    def p_anduration_iterations(self, n):       return ('iterations', n)
    def p_anduration_minutes(self, n):          return ('seconds', 60*n)
    def p_anduration_seconds(self, n):          return ('seconds', n)

    def p_analysis_program_opt_none(self):      return None
    def p_analysis_program_opt_some(self, p):   return p
    def p_analysis_program_empty(self):         return []
    def p_analysis_program_nonempty(self, p, t): p += t; return p
    def p_analysis_token_compound(self, p):     return ['('] + p + [')']
    def p_analysis_token_primitive(self, t):    return [t]

    def p_command_regress(self, target, givens, nsamp, pop, generator,
            modelnos):
        return ast.Regress(target, givens, nsamp, pop, generator, modelnos)

    def p_simulate_s(self, cols, population, generator, modelnos, constraints,
            lim, acc):
        for c in cols:
            if isinstance(c, ast.SelColSub):
                continue
            if isinstance(c, ast.SelColExp) and \
               isinstance(c.expression, ast.ExpCol):
                continue
            self.errors.append('simulate only accepts population variables.')
            return None
        return ast.Simulate(
            cols, population, generator, modelnos, constraints, lim.limit, acc)
    def p_simulate_nolimit(self, cols, population, generator, modelnos,
            constraints):
        # XXX Report source location.
        self.errors.append('simulate missing limit')
        for c in cols:
            if isinstance(c, ast.SelColSub):
                continue
            if isinstance(c, ast.SelColExp) and \
               isinstance(c.expression, ast.ExpCol):
                continue
            self.errors.append('simulate only accepts population variables.')
            return None
        return ast.Simulate(
            cols, population, generator, modelnos, constraints, 0, None)
    def p_simulate_models(self, cols, population, generator):
        return ast.SimulateModelsExp(cols, population, generator)

    def p_given_opt_none(self):                 return []
    def p_given_opt_some(self, constraints):    return constraints
    def p_constraints_one(self, c):             return [c]
    def p_constraints_many(self, cs, c):        cs.append(c); return cs
    def p_constraint_c(self, col, value):       return (col, value)
    def p_constraints_opt_none(self):           return []
    def p_constraints_opt_some(self, cs):       return cs
    def p_constraints_list_one(self, cs):       return [cs]
    def p_constraints_list_some(self, css, cs): css.append(cs); return css

    def p_query_select(self, q):                return q
    def p_query_estimate(self, q):              return q
    def p_query_estcol(self, q):                return q
    def p_query_estpairrow(self, q):            return q
    def p_query_estpaircol(self, q):            return q
    def p_query_estby(self, q):                 return q
    def p_query_infer(self, q):                 return q
    def p_query_simulate(self, q):              return q
    def p_query_estimate_pairwise_row(self, q): return q
    def p_query_create_column_list(self, q):    return q

    def p_select_s(self, quant, cols, tabs, cond, grouping, ord, lim):
        return ast.Select(quant, cols, tabs, cond, grouping, ord, lim)

    def p_estimate_e(self, quant, cols, tabs, generator, modelnos, cond,
            grouping, ord, lim):
        constructor = tabs
        return constructor(quant, cols, generator, modelnos, cond, grouping,
            ord, lim)

    def p_estcol_e(self):
        self.errors.append("deprecated `ESTIMATE COLUMNS'"
            ": use `ESTIMATE ... FROM COLUMNS OF'")
    def p_estpairrow_e(self):
        self.errors.append("deprecated `ESTIMATE PAIRWISE ROW'"
            ": use `ESTIMATE ... FROM PAIRWISE'")
    def p_estpaircol_e(self):
        self.errors.append("deprecated `ESTIMATE PAIRWISE'"
            ": use `ESTIMATE ... FROM PAIRWISE COLUMNS OF'")

    def p_estby_e(self, quant, cols, population, generator, modelnos):
        return ast.EstBy(quant, cols, population, generator, modelnos)

    def p_infer_auto(self, cols, conf, nsamp, population, generator, modelnos,
            cond, grouping, ord, lim):
        return ast.InferAuto(cols, conf, nsamp, population, generator, modelnos,
            cond, grouping, ord, lim)
    def p_infer_explicit(self, cols, population, generator, modelnos, cond,
            grouping, ord, lim):
        return ast.InferExplicit(cols, population, generator, modelnos, cond,
            grouping, ord, lim)

    def p_infer_auto_columns_one(self, c):      return [c]
    def p_infer_auto_columns_many(self, cs, c): cs.append(c); return cs

    def p_infer_auto_column_all(self):
        return ast.InfColAll()
    def p_infer_auto_column_one(self, col, name):
        return ast.InfColOne(col, name)

    def p_withconf_opt_none(self):
        return ast.ExpLit(ast.LitInt(0))
    def p_withconf_opt_some(self, conf):        return conf
    def p_withconf_conf(self, conf):            return conf

    def p_infer_exp_columns_one(self, c):       return [c]
    def p_infer_exp_columns_many(self, cs, c):  cs.append(c); return cs
    def p_infer_exp_column_sel(self, c):        return c
    def p_infer_exp_column_pred(self, col, name, confname, nsamp):
        return ast.PredCol(col, name, confname, nsamp)

    def p_conf_opt_none(self):                  return None
    def p_conf_opt_some(self, confname):        return confname

    def p_select_quant_distinct(self):          return ast.SELQUANT_DISTINCT
    def p_select_quant_all(self):               return ast.SELQUANT_ALL
    def p_select_quant_default(self):           return ast.SELQUANT_ALL

    def p_select_columns_one(self, c):          return [c]
    def p_select_columns_many(self, cs, c):     cs.append(c); return cs

    def p_select_column_star(self):             return ast.SelColAll(None)
    def p_select_column_qstar(self, table):     return ast.SelColAll(table)
    def p_select_column_qsub(self, table, q):   return ast.SelColSub(table, q)
    def p_select_column_exp(self, e, name):     return ast.SelColExp(e, name)

    def p_as_none(self):                        return None
    def p_as_some(self, name):                  return name

    def p_from_sel_opt_empty(self):             return None
    def p_from_sel_opt_nonempty(self, tables):  return tables

    def p_from_est_row(self, name):
        def c(quant, cols, generator, modelnos, cond, grouping, ord, lim):
            return ast.Estimate(quant, cols, name, generator, modelnos, cond,
                grouping, ord, lim)
        return c
    def p_from_est_pairrow(self, name):
        def c(quant, cols, generator, modelnos, cond, grouping, ord, lim):
            return ast.EstPairRow(cols, name, generator, modelnos, cond, ord,
                lim)
        return c
    def p_from_est_col(self, name):
        def c(quant, cols, generator, modelnos, cond, grouping, ord, lim):
            return ast.EstCols(cols, name, generator, modelnos, cond, ord, lim)
        return c
    def p_from_est_paircol(self, name, subcols):
        def c(quant, cols, generator, modelnos, cond, grouping, ord, lim):
            return ast.EstPairCols(cols, name, subcols, generator, modelnos,
                cond, ord, lim)
        return c

    def p_modeledby_opt_none(self):             return None
    def p_modeledby_opt_some(self, gen):        return gen

    def p_usingmodel_opt_none(self):            return None
    def p_usingmodel_opt_some(self, modelnos):  return modelnos

    def p_select_tables_one(self, t):           return [t]
    def p_select_tables_many(self, ts, t):      ts.append(t); return ts
    def p_select_table_named(self, table, name): return ast.SelTab(table, name)
    def p_select_table_subquery(self, q, name):  return ast.SelTab(q, name)

    def p_for_none(self):                       return None
    def p_for_one(self, collist):               return collist

    def p_where_unconditional(self):            return None
    def p_where_conditional(self, condition):   return condition

    def p_column_name_cn(self, name):           return name
    def p_generator_name_unqualified(self, name): return name
    def p_backend_name_bn(self, name):          return name
    def p_population_name_pn(self, name):       return name
    def p_table_name_unqualified(self, name):   return name

    def p_group_by_none(self):                  return None
    def p_group_by_some(self, keys):            return ast.Grouping(keys, None)
    def p_group_by_having(self, keys, cond):    return ast.Grouping(keys, cond)

    def p_order_by_none(self):                  return None
    def p_order_by_some(self, keys):            return keys
    def p_order_keys_one(self, key):            return [key]
    def p_order_keys_many(self, keys, key):     keys.append(key); return keys
    def p_order_key_k(self, e, s):              return ast.Ord(e, s)
    def p_order_sense_none(self):               return ast.ORD_ASC
    def p_order_sense_asc(self):                return ast.ORD_ASC
    def p_order_sense_desc(self):               return ast.ORD_DESC

    def p_limit_opt_none(self):                 return None
    def p_limit_opt_some(self, lim):            return lim

    def p_accuracy_opt_none(self):              return None
    def p_accuracy_opt_some(self, acc):         return acc

    def p_limit_n(self, limit):                 return ast.Lim(limit, None)
    def p_limit_offset(self, limit, offset):    return ast.Lim(limit, offset)
    def p_limit_comma(self, offset, limit):     return ast.Lim(limit, offset)

    def p_expressions_opt_none(self):           return []
    def p_expressions_opt_some(self, es):       return es

    def p_expressions_one(self, e):             return [e]
    def p_expressions_many(self, es, e):        es.append(e); return es

    def p_expression_top(self, e):      return e
    def p_bqlfn0_exp(self, e):          return e
    def p_expression1_top(self, e):     return e

    def p_boolean_or_or(self, l, r):    return ast.op(ast.OP_BOOLOR, l, r)
    def p_boolean_or_and(self, a):      return a
    def p_boolean_and_and(self, l, r):  return ast.op(ast.OP_BOOLAND, l, r)
    def p_boolean_and_not(self, n):     return n
    def p_boolean_not_not(self, n):     return ast.op(ast.OP_BOOLNOT, n)
    def p_boolean_not_equality(self, c):
                                        return c
    def p_equality_is(self, l, r):      return ast.op(ast.OP_IS, l, r)
    def p_equality_isnot(self, l, r):   return ast.op(ast.OP_ISNOT, l, r)
    def p_equality_like(self, l, r):    return ast.op(ast.OP_LIKE, l, r)
    def p_equality_notlike(self, l, r): return ast.op(ast.OP_NOTLIKE, l, r)
    def p_equality_like_esc(self, l, r, e):
                                        return ast.op(ast.OP_LIKE_ESC, l, r, e)
    def p_equality_notlike_esc(self, l, r, e):
                                        return ast.op(ast.OP_NOTLIKE_ESC,
                                            l, r, e)
    def p_equality_glob(self, l, r):    return ast.op(ast.OP_GLOB, l, r)
    def p_equality_notglob(self, l, r): return ast.op(ast.OP_NOTGLOB, l, r)
    def p_equality_glob_esc(self, l, r, e):
                                        return ast.op(ast.OP_GLOB_ESC, l, r, e)
    def p_equality_notglob_esc(self, l, r, e):
                                        return ast.op(ast.OP_NOTGLOB_ESC,
                                            l, r, e)
    def p_equality_regexp(self, l, r):  return ast.op(ast.OP_REGEXP, l, r)
    def p_equality_notregexp(self, l, r):
                                        return ast.op(ast.OP_NOTREGEXP, l, r)
    def p_equality_regexp_esc(self, l, r, e):
                                        return ast.op(ast.OP_REGEXP_ESC,
                                            l, r, e)
    def p_equality_notregexp_esc(self, l, r, e):
                                        return ast.op(ast.OP_NOTREGEXP_ESC,
                                            l, r, e)
    def p_equality_match(self, l, r):   return ast.op(ast.OP_MATCH, l, r)
    def p_equality_notmatch(self, l, r):
                                        return ast.op(ast.OP_NOTMATCH, l, r)
    def p_equality_match_esc(self, l, r, e):
                                        return ast.op(ast.OP_MATCH_ESC, l, r, e)
    def p_equality_notmatch_esc(self, l, r, e):
                                        return ast.op(ast.OP_NOTMATCH_ESC,
                                            l, r, e)
    def p_equality_between(self, m, l, r):
                                        return ast.op(ast.OP_BETWEEN, m, l, r)
    def p_equality_notbetween(self, m, l, r):
                                        return ast.op(ast.OP_NOTBETWEEN, m,l,r)
    def p_equality_in_query(self, e, q): return ast.ExpInQuery(e, True, q)
    def p_equality_notin_query(self, e, q): return ast.ExpInQuery(e, False, q)
    def p_equality_in_exp(self, e, es): return ast.ExpInExp(e, True, es)
    def p_equality_notin_exp(self, e, es): return ast.ExpInExp(e, False, es)
    def p_equality_isnull(self, e):     return ast.op(ast.OP_ISNULL, e)
    def p_equality_notnull(self, e):    return ast.op(ast.OP_NOTNULL, e)
    def p_equality_neq(self, l, r):     return ast.op(ast.OP_NEQ, l, r)
    def p_equality_eq(self, l, r):      return ast.op(ast.OP_EQ, l, r)
    def p_equality_ordering(self, o):   return o
    def p_ordering_lt(self, l, r):      return ast.op(ast.OP_LT, l, r)
    def p_ordering_leq(self, l, r):     return ast.op(ast.OP_LEQ, l, r)
    def p_ordering_geq(self, l, r):     return ast.op(ast.OP_GEQ, l, r)
    def p_ordering_gt(self, l, r):      return ast.op(ast.OP_GT, l, r)
    def p_ordering_bitwise(self, b):    return b
    def p_bitwise_and(self, l, r):      return ast.op(ast.OP_BITAND, l, r)
    def p_bitwise_ior(self, l, r):      return ast.op(ast.OP_BITIOR, l, r)
    def p_bitwise_lshift(self, l, r):   return ast.op(ast.OP_LSHIFT, l, r)
    def p_bitwise_rshift(self, l, r):   return ast.op(ast.OP_RSHIFT, l, r)
    def p_bitwise_additive(self, a):    return a
    def p_additive_add(self, l, r):     return ast.op(ast.OP_ADD, l, r)
    def p_additive_sub(self, l, r):     return ast.op(ast.OP_SUB, l, r)
    def p_additive_mult(self, m):       return m
    def p_multiplicative_mul(self, l, r):
                                        return ast.op(ast.OP_MUL, l, r)
    def p_multiplicative_div(self, l, r):
                                        return ast.op(ast.OP_DIV, l, r)
    def p_multiplicative_rem(self, l, r):
                                        return ast.op(ast.OP_REM, l, r)
    def p_multiplicative_conc(self, c): return c
    def p_concatenative_concat(self, l, r):
                                        return ast.op(ast.OP_CONCAT, l, r)
    def p_concatenative_collate(self, c):
                                        return c
    def p_collating_collate(self, e, c):
                                        return ast.ExpCollate(e, c)
    def p_collating_unary(self, u):     return u
    def p_unary_bitwise_not(self, u):   return ast.op(ast.OP_BITNOT, u)
    def p_unary_minus(self, u):         return ast.op(ast.OP_NEGATE, u)
    def p_unary_plus(self, u):          return ast.op(ast.OP_PLUSID, u)
    def p_unary_bql(self, b):           return b

    def p_bqlfn_predprob_row(self, target):
        return ast.ExpBQLPredProb([target], [])
    def p_bqlfn_jpredprob_row(self, targets):
        return ast.ExpBQLPredProb(targets, [])
    def p_bqlfn_condpredprob_row(self, target, constraints):
        return ast.ExpBQLPredProb([target], constraints)
    def p_bqlfn_condjpredprob_row(self, targets, constraints):
        return ast.ExpBQLPredProb(targets, constraints)
    def p_bqlfn_prob_const(self, col, e):       return ast.ExpBQLProbDensity(
                                                    [(col, e)], [])
    def p_bqlfn_jprob_const(self, targets):     return ast.ExpBQLProbDensity(
                                                    targets, [])
    def p_bqlfn_condprob_const(self, col, e, constraints):
                                                return ast.ExpBQLProbDensity(
                                                    [(col, e)], constraints)
    def p_bqlfn_condjprob_const(self, targets, constraints):
                                                return ast.ExpBQLProbDensity(
                                                    targets, constraints)
    def p_bqlfn_prob_1col(self, e):             return ast.ExpBQLProbDensityFn(
                                                    e, [])
    def p_bqlfn_condprob_1col(self, e, constraints):
                                                return ast.ExpBQLProbDensityFn(
                                                    e, constraints)
    def p_bqlfn_sim_const(self, cond0, cond1, col):
        return ast.ExpBQLSim(cond0, cond1, col)
    def p_bqlfn_sim_1row(self, cond, col):
        return ast.ExpBQLSim(None, cond, col)
    def p_bqlfn_sim_2row(self, col):
        return ast.ExpBQLSim(None, None, col)

    def p_bqlfn_predrel_existing(self, cond0, cond1, col):
        return ast.ExpBQLPredRel(cond0, cond1, None, col)
    def p_bqlfn_predrel_hypothetical(self, cond0, constraints, col):
        return ast.ExpBQLPredRel(cond0, None, constraints, col)
    def p_bqlfn_predrel_both(self, cond0, cond1, constraints, col):
        return ast.ExpBQLPredRel(cond0, cond1, constraints, col)

    def p_bqlfn_depprob(self, cols):            return ast.ExpBQLDepProb(*cols)

    def p_bqlfn_mutinf(self, cols, constraints, nsamp):
        return ast.ExpBQLMutInf(cols[0], cols[1], constraints, nsamp)
    def p_bqlfn_prob_est(self, e):              return ast.ExpBQLProbEst(e)

    def p_predrel_of_opt_none(self):            return None
    def p_predrel_of_opt_one(self, cond0):      return cond0

    def p_existing_rows_one(self, cond):        return cond
    def p_hypothetical_rows_one(self, cs):      return cs

    def p_ofwithmulti_bql_2col(self):                   return (None, None)
    def p_ofwithmulti_bql_1col(self, cols):             return (cols, None)
    def p_ofwithmulti_bql_const(self, cols0, cols1):    return (cols0, cols1)

    def p_mi_columns_one(self, col):                return [col]
    def p_mi_columns_many(self, cols):              return cols

    def p_mi_column_list_one(self, col):            return [col]
    def p_mi_column_list_many(self, cols, col):
        cols.append(col)
        return cols

    def p_mi_given_opt_none(self):                  return None
    def p_mi_given_opt_some(self, constraints):     return constraints

    def p_mi_constraint_equality(self, col, value):
        return (col, value)
    def p_mi_constraint_marginal(self, col):
        return (col, ast.ExpLit(ast.LitNull(0)))
    def p_mi_constraints_one(self, c):
        return [c]
    def p_mi_constraints_many(self, cs, c):
        cs.append(c); return cs

    def p_bqlfn_correl(self, cols):             return ast.ExpBQLCorrel(*cols)
    def p_bqlfn_correl_pval(self, cols):
        return ast.ExpBQLCorrelPval(*cols)
    def p_bqlfn_predict(self, col, conf, nsamp):
        return ast.ExpBQLPredict(col, conf, nsamp)
    def p_bqlfn_primary(self, p):               return p

    def p_wrt_one(self, col):                   return [col]

    def p_ofwith_bql_2col(self):                return (None, None)
    def p_ofwith_bql_1col(self, col):           return (col, None)
    def p_ofwith_bql_const(self, col1, col2):   return (col1, col2)

    def p_nsamples_opt_none(self):              return None
    def p_nsamples_opt_some(self, nsamples):    return nsamples

    def p_column_lists_one(self, collist):
        return [collist]
    def p_column_lists_many(self, collists, collist):
        collists.append(collist)
        return collists

    def p_column_list_all(self):                return ast.ColListAll()
    def p_column_list_column(self, col):        return ast.ColListLit([col])
    def p_column_list_subquery(self, q):        return ast.ColListSub(q)

    def p_primary_literal(self, v):             return ast.ExpLit(v)
    def p_primary_numpar(self, n):              return ast.ExpNumpar(n)
    def p_primary_nampar(self, n):              return ast.ExpNampar(n[0],n[1])
    def p_primary_apply(self, fn, es):          return ast.ExpApp(False,fn,es)
    def p_primary_apply_distinct(self, fn, es): return ast.ExpApp(True, fn, es)
    def p_primary_apply_star(self, fn):         return ast.ExpAppStar(fn)
    def p_primary_paren(self, e):               return e
    def p_primary_subquery(self, q):            return ast.ExpSub(q)
    def p_primary_cast(self, e, t):             return ast.ExpCast(e, t)
    def p_primary_exists(self, q):              return ast.ExpExists(q)
    def p_primary_column(self, col):            return ast.ExpCol(None, col)
    def p_primary_tabcol(self, tab, col):       return ast.ExpCol(tab, col)
    def p_primary_case(self, k, ws, e):         return ast.ExpCase(k, ws, e)

    def p_case_key_opt_none(self):              return None
    def p_case_key_opt_some(self, k):           return k
    def p_case_whens_opt_none(self):            return []
    def p_case_whens_opt_some(self, ws, w, t):  ws.append((w, t)); return ws
    def p_case_else_opt_none(self):             return None
    def p_case_else_opt_some(self, e):          return e

    def p_literal_null(self):                   return ast.LitNull(None)
    def p_literal_integer(self, i):             return ast.LitInt(i)
    def p_literal_float(self, f):               return ast.LitFloat(f)
    def p_literal_string(self, s):              return ast.LitString(s)

    def p_type_name(self, n):                   return ast.Type(n, [])
    def p_type_onearg(self, n, a):              return ast.Type(n, [a])
    def p_type_twoarg(self, n, a, b):           return ast.Type(n, [a, b])
    def p_typename_one(self, n):                return [n]
    def p_typename_many(self, tn, n):           tn.append(n); return tn
    def p_typearg_unsigned(self, i):            return i
    def p_typearg_positive(self, i):            return i
    def p_typearg_negative(self, i):            return -i
