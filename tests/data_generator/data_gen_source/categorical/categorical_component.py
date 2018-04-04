import numpy as np

from tests.data_generator.data_gen_source.abstract.abstract_component import AbstractComponent


class CategoricalComponent(AbstractComponent):
    """
    Component that has a categorical distribution
    """

    def __init__(self, categorical):
        """
        Constructor
        :param categorical: distribution in form of [0.2, 0.0, 0.5, 0.3] or {0: 0.2, 2: 0.5, 3: 0.3}
                            note: dictionary form can support labels other than integers
                            note: also supports multivariate categoricals: [[0.2, 0.8], {"dog": 0.5, "cat": 0.5}]
        """
        # standardize format by wrapping univariate categoricals in list (so processed like multivariate)
        if type(categorical) is dict or (type(categorical[0]) is not list and type(categorical[0]) is not dict):
            categorical = [categorical]

        self.labels = []
        self.probabilities = []
        self.dict_forms = []

        for dimension in categorical:  # dimension is a univariate categorical that makes up a multivariate categorical
            dimension_labels = []
            dimension_probabilities = []
            # standardize to dictionary form
            if type(dimension) is list:
                dict_form = categorical_list_to_dict(dimension)
            else:
                dict_form = dimension
            # ensure categorical sums to 1
            if sum(dict_form.values()) != 1:
                raise ValueError("categorical " + str(dimension) + " is not valid, probabilities are not equal to 1")
            # turn dictionary into two lists: one for labels and one for probability (also for numpy)
            # list needed to order
            for key in sorted(dict_form.keys()):
                dimension_labels.append(key)
                dimension_probabilities.append(dict_form[key])

            self.labels.append(dimension_labels)
            self.probabilities.append(dimension_probabilities)
            self.dict_forms.append(dict_form)

    def simulate(self, size=1, seed=None):
        if seed is not None:
            np.random.seed(seed)

        out = []
        for index in range(len(self.labels)):  # for each dimension
            out.append(np.random.choice(self.labels[index], size, p=self.probabilities[index]).tolist())
        if len(out) != 1:  # multivariate
            return list(zip(*out))  # transpose to mix two lists of separate categoricals

        return out[0]  # univariate is wrapped in list, don't zip univariate, it wraps all values in tuple

    def likelihood(self, data):
        if type(data) != list:  # wrap univariate outer (to support single value input)
            data = [data]

        total_probability = 1
        for value in data:
            if type(value) != list:  # wrap univariate
                value = [value]
            if len(value) != len(self.labels):  # verify input
                raise ValueError("Combination doesn't match the number of dimensions in the categorical")

            value_probability = 1

            # check probability of each dimension for each part of sequence (value)
            # combination: [ [0, "dog"], [1, "cat"]...] value: [0, "dog"]
            # check 0 against first dimension categorical, "dog" against second dimension categorical
            for index, dimension in enumerate(value):
                value_probability *= self.dict_forms[index][dimension] \
                    if dimension in self.dict_forms[index].keys() else 0.0
            total_probability *= value_probability

        return total_probability


def categorical_list_to_dict(dimension):
    """
    Turns a categorical list that uses indices into a dictionary representation,
    eliminating zeroes in the process
    :param dimension: in list form
    :return: categorical in dict form without any choices with 0 probability
    """
    out_dict = {}
    for index in range(len(dimension)):
        if dimension[index] != 0.0:
            out_dict[index] = dimension[index]
    return out_dict
