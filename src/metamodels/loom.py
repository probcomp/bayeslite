"""The Loom Model is a work in progress.

This module implements the :class:`bayeslite.IBayesDBMetamodel`
interface for the Loom Model.
"""
import math
import random

import bayeslite.core as core
import bayeslite.metamodel as metamodel

from bayeslite.sqlite3_util import sqlite3_quote_name

class LoomMetamodel(metamodel.IBayesDBMetamodel):
    """Loom metamodel for BayesDB."""

    def __init__(self, seed=0):
        self.prng = random.Random(seed)

    def name(self): return 'loom'

    def register(self, bdb):
        pass

    def create_generator(self, bdb, generator_id, schema, **kwargs):
        pass

    def initialize_models(self, *args, **kwargs):
        pass

    def analyze_models(self, bdb, generator_id, modelnos=None, iterations=1,
            max_seconds=None, ckpt_iterations=None, ckpt_seconds=None,
            program=None):
        pass

    def column_dependence_probability(self, bdb, generator_id, modelnos, colno0,
            colno1):
        return 0

    def column_mutual_information(self, bdb, generator_id, modelnos, colnos0,
            colnos1, constraints, numsamples):
        return [0]

    def row_similarity(self, bdb, generator_id, modelnos, rowid, target_rowid,
            colnos):
        return 0

    def predict_confidence(self, bdb, generator_id, modelnos, rowid, colno,
            numsamples=None):
        return (0, 1)

    def simulate_joint(self, bdb, generator_id, modelnos, rowid, targets,
            _constraints, num_samples=1, accuracy=None):
        return [[0 for _ in targets] for __ num_samples]

    def logpdf_joint(self, bdb, generator_id, modelnos, rowid, targets,
            constraints):
        return 0.0

