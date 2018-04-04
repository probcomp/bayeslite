import numpy as np
import scipy.stats as sp

from tests.data_generator.data_gen_source.abstract.abstract_component import AbstractComponent


class GaussianComponent(AbstractComponent):
    """
    Component that has a Gaussian distribution
    """

    def __init__(self, mean, standard_deviation):
        """
        Constructor
        :param mean: of Gaussian
        :param standard_deviation: of Gaussian
        """
        self.mean = mean
        self.standard_deviation = standard_deviation

        if type(mean) is list or type(standard_deviation) is list:  # multivariate
            if len(mean) != len(standard_deviation):
                raise ValueError("number of means and standard deviations don't match")
            self.dimensions = len(mean)
            self.covariance = np.diag(standard_deviation)
            self.representation = sp.multivariate_normal(self.mean, self.covariance)

        else:  # univariate
            self.dimensions = 1
            self.representation = sp.norm(self.mean, self.standard_deviation)

    def likelihood(self, data):
        if type(data) != list:  # wrap univariate outer to support single value input
            data = [data]
        # works for all dimensions, multivariate_normal and normal scipy classes both have pdf method
        return np.sum(self.representation.logpdf(data))  # prod multiplies list together
        # likelihood just returns list of all pdf values in a list

    def simulate(self, size=1, seed=None):
        if seed is not None:
            np.random.seed(seed)

        # multivariate
        if self.dimensions != 1:
            return np.random.multivariate_normal(self.mean, self.covariance, size)

        # univariate
        return np.random.normal(self.mean, self.standard_deviation, size)
