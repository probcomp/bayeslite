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

cgpm(empty)			::= .
cgpm(schema)		::= schema(s).

schema(one)			::= clause(c).
schema(some)		::= schema(s) T_COMMA clause_opt(c).

clause_opt(none)	::= .
clause_opt(some)	::= clause(c).

clause(basic)		::= var(var) dist(dist) param_opt(params).
clause(foreign)		::= K_MODEL vars(outputs) given_opt(inputs)
			K_USING foreign(name) param_opt(params).
clause(subsamp)		::= K_SUBSAMPLE L_NUMBER(n).
clause(latent)		::= K_LATENT var(var) stattype(st).

dist(name)			::= L_NAME(dist).
foreign(name)		::= L_NAME(foreign).

given_opt(none)		::= .
given_opt(some)		::= K_GIVEN vars(vars).

vars(one)			::= var(var).
vars(many)			::= vars(vars) T_COMMA var(var).
var(name)			::= L_NAME(var).

stattype(s)			::= L_NAME(st).

param_opt(none)		::= .
param_opt(some)		::= T_LROUND params(ps) T_RROUND.
params(one)			::= param(param).
params(many)		::= params(params) T_COMMA param(param).

param(num)			::= L_NAME(p) T_EQ L_NUMBER(num).
param(nam)			::= L_NAME(p) T_EQ L_NAME(nam).
