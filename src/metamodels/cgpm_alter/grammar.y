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

/*
 * Terminal conventions:
 * - T_ means a punctuation token.
 * - K_ means a keyword.
 * - L_ means a lexeme, which has useful associated text, e.g. an integer.
 */


alter(start)        ::= phrases(ps).

phrases(one)        ::= phrase(p).
phrases(many)       ::= phrases(ps) T_COMMA phrase(p).

phrase(none)        ::= .

phrase(set_var_dependency)  ::= K_ENSURE K_VARIABLE|K_VARIABLES columns(cols)
                                    dependency(dep).

phrase(set_var_cluster)     ::= K_ENSURE K_VARIABLE|K_VARIABLES
                                    columns(cols0) K_IN K_VIEW K_OF
                                    column_name(col1).

phrase(set_var_cluster_singleton)   ::= K_ENSURE K_VARIABLE|K_VARIABLES
                                            columns(cols)
                                            K_IN K_SINGLETON K_VIEW.

phrase(set_var_cluster_conc)    ::= K_SET K_VIEW K_CONCENTRATION K_PARAMETER
                                        K_TO concentration(conc).

phrase(set_row_cluster)         ::= K_ENSURE K_ROW|K_ROWS rows(rows0)
                                        K_IN K_CLUSTER K_OF K_ROW
                                        row_index(row1)
                                        K_WITHIN K_VIEW K_OF column_name(col).

phrase(set_row_cluster_singleton)   ::= K_ENSURE K_ROW|K_ROWS rows(rows0)
                                            K_IN K_SINGLETON K_CLUSTER
                                            K_WITHIN K_VIEW
                                            K_OF column_name(col).

phrase(set_row_cluster_conc)   ::= K_SET K_ROW K_CLUSTER
                                        K_CONCENTRATION K_PARAMETER
                                        K_WITHIN K_VIEW K_OF column_name(col)
                                        K_TO concentration(conc).

dependency(independent) ::= K_INDEPENDENT.
dependency(dependent)   ::= K_DEPENDENT.

columns(one)         ::= column_name(col).
columns(all)         ::= T_STAR.
columns(many)        ::= T_LROUND column_list(cols) T_RROUND.

column_list(one)     ::= column_name(col).
column_list(many)    ::= column_list(cols) T_COMMA column_name(col).

column_name(n)      ::= L_NAME(n).

rows(one)           ::= row_index(row).
rows(all)           ::= T_STAR.
rows(many)          ::= T_LROUND row_list(rows) T_RROUND.

row_list(one)       ::= row_index(row).
row_list(many)      ::= row_list(rows) T_COMMA row_index(row).

row_index(n)        ::= L_NUMBER(n).

concentration(c)    ::= L_NUMBER(n).
