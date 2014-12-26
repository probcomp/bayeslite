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

import bayeslite.scan as scan

class BQLSemantics(object):
    def p_bql_start(self, phrases):
        return phrases
    def p_bql_phrases_one(self, phrase):
        return [] if phrase is None else [phrase]
    def p_bql_phrases_many(self, phrases, phrase):
        if phrase is not None:
            phrases.append(phrase)
        return phrases

    def p_bql_phrase_empty(self):               return None
    def p_bql_phrase_query(self, q):            return q
    def p_bql_phrase_command(self, c):          return c

    def p_query_q(self, action, body):          return action + body
    def p_query_action_none(self):              return []
    def p_query_action_freq(self):              return ['freq']
    def p_query_action_hist(self):              return ['hist']
    def p_query_action_summarize(self):         return ['summarize']
    def p_query_action_plot(self):              return ['plot']

    def p_query_body_select(self, q):                   return q
    def p_query_body_infer(self, q):                    return q
    def p_query_body_simulate(self, q):                 return q
    def p_query_body_estimate_pairwise_row(self, q):    return q
    def p_query_body_create_column_list(self, q):       return q

    def p_select_one(self, select):             return select
