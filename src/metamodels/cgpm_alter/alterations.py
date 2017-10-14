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


from bayeslite.metamodels.cgpm_alter import parse

from cgpm.mixtures.view import View


def make_set_var_dependency(dependency):
    def func_dep(state):
        f = make_set_var_cluster(state.outputs[1:], state.outputs[0])
        state = f(state)
        return state
    def func_indep(state):
        for output in state.outputs:
            f = make_set_var_cluster([output], parse.SingletonCluster)
            state = f(state)
        return state
    if dependency == parse.EnsureDependent:
        return func_dep
    elif dependency == parse.EnsureIndependent:
        return func_indep
    raise ValueError('Unknown dependency: %s' % (dependency,))

def make_set_var_cluster(columns0, column1):
    def func_existing(state):
        for col0 in exclude(columns0, column1):
            d_col0 = state.dim_for(col0)
            v_col0 = state.Zv(col0)
            v_col1 = state.Zv(column1)
            state._migrate_dim(v_col0, v_col1, d_col0)
        return state
    def func_singleton(state):
        new_view_index = max(state.views) + 1
        new_view = View(
            state.X,
            outputs=[state.crp_id_view + new_view_index],
            rng=state.rng
        )
        state._append_view(new_view, new_view_index)
        for col0 in columns0:
            d_col0 = state.dim_for(col0)
            v_col0 = state.Zv(col0)
            state._migrate_dim(v_col0, new_view_index, d_col0)
        return state
    if column1 == parse.SingletonCluster:
        return func_singleton
    else:
        return func_existing

def make_set_var_cluster_conc(concentration):
    def func(state):
        # XXX No abstraction.
        state.crp.hypers['alpha'] = 1./concentration
        return state
    return func

def make_set_row_cluster(rows0, row1, column):
    def func_existing(state):
        view = state.view_for(column)
        k_row1 = view.Zr(row1)
        for row0 in exclude(rows0, row1):
            view._migrate_row(row0, k_row1)
        return state
    def func_singleton(state):
        view = state.view_for(column)
        k_singleton = view.Zr(row1)
        for row0 in rows0:
            view._migrate_row(row0, k_singleton)
        return state
    if row1 == parse.SingletonCluster:
        return func_singleton
    else:
        return func_existing

def make_set_row_cluster_conc(column, concentration):
    def func(state):
        view = state.view_for(column)
        view.crp.hypers['alpha'] = 1./concentration
        return state
    return func

def exclude(iterable, ignore):
    for item in iterable:
        if item != ignore:
            yield item
