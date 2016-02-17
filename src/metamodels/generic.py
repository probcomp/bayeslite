class GenericMetamodel(IBayesDBMetamodel):

    def name(self):
        raise NotImplementedError

    def register(self, bdb):
        raise NotImplementedError

    def create_generator(self, bdb, table, schema, instantiate):
        raise NotImplementedError

    def drop_generator(self, bdb, generator_id):
        raise NotImplementedError

    def rename_column(self, bdb, generator_id, oldname, newname):
        raise NotImplementedError

    def initialize_models(self, bdb, generator_id, modelnos, model_config):
        raise NotImplementedError

    def drop_models(self, bdb, generator_id, modelnos=None):
        raise NotImplementedError

    def analyze_models(self, bdb, generator_id, modelnos=None, iterations=1,
            max_seconds=None, ckpt_iterations=None, ckpt_seconds=None):
        raise NotImplementedError

    def column_dependence_probability(self, bdb, generator_id, modelno, colno0,
            colno1):
        raise NotImplementedError

    def column_mutual_information(self, bdb, generator_id, modelno, colno0,
            colno1, numsamples=100):
        raise NotImplementedError

    def row_similarity(self, bdb, generator_id, modelno, rowid, target_rowid,
            colnos):
        raise NotImplementedError

    def predict_confidence(self, bdb, generator_id, modelno, colno, rowid,
            numsamples=None):
        raise NotImplementedError

    def simulate_joint(self, bdb, generator_id, targets, constraints, modelno,
            num_predictions=1):
        raise NotImplementedError

    def logpdf_joint(self, bdb, generator_id, targets, constraints,
            modelno=None):
        raise NotImplementedError


PreprocessedData = namedtuple('PreprocessedData', ['bayesdb_schema', 'metadata'])

class GenericGpm(object):

    def preprocess_data(self, parsed_schema, original_data_array):
        # returns PreprocessedData
        raise NotImplementedError

    def state_factory(self):
        # Returns a function that takes PreprocessedData as an argument and
        # returns a GenericState.
        raise NotImplementedError

ColnoValue = namedtuple('ColnoValue', ['colno', 'value'])

class GenericState(object):

    @classmethod
    def from_json(cls):
        raise NotImplementedError

    def to_json(self):
        raise NotImplementedError

    def transition_iterations(self, iterations):
        raise NotImplementedError

    def transition_seconds(self, seconds):
        raise NotImplementedError

    def column_dependence_probability(self, state, colno0, colno1):
        # Unbiased dependence estimate, will be averaged.
        raise NotImplementedError

    def column_mutual_information(self, state, colno0, colno1):
        # Unbiased estimate, will be averaged.
        raise NotImplementedError

    def row_similarity(self, row1, row2, colnos):
        # Unbiased estimate, will be averaged.
        raise NotImplementedError

    def predict_confidence(self, colno, rowid):
        raise NotImplementedError

    def simulate_missing_columns(self, rowid, colnos, N):
        raise NotImplementedError

    def simulate_new_rows(self, colnos, given_colno_values, N):
        raise NotImplementedError

    def logpdf_missing_columns(self, rowid, target_colno_values):
        raise NotImplementedError

    def logpdf_new_rows(self, target_colno_values, given_colno_values):
        raise NotImplementedError
