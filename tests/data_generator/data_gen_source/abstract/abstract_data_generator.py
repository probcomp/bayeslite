from abc import ABCMeta, abstractmethod


class AbstractDataGenerator:
    """
    Mixture model that supports sampling (data generation) as well as a likelihood / log of probability density
    function. Mixture is made up of various components of the same type (for now)
    """
    __metaclass__ = ABCMeta

    def __init__(self, component_parameters, weights=None):
        """
        Constructor
        :param component_parameters: list of parameters for each component
        :param weights: probability of picking each component, sums to 1. Defaults to uniform distribution.
        """
        if type(component_parameters) is not list or type(component_parameters[0]) is not list:
            # wrap to support single 1D input e.g. GaussianGenerator([0.5, 0])
            component_parameters = [component_parameters]

        if weights is None:  # uniform distribution
            self.weights = [1.0 / len(component_parameters)] * len(component_parameters)
        elif sum(weights) != 1:
            raise ValueError("weights need to sum to 1")
        else:
            self.weights = weights

        if len(self.weights) != len(component_parameters):
            raise ValueError("number of weights do not match number of component parameters i.e. \
            cannot decide number of components")

        self.component_parameters = component_parameters

    @abstractmethod
    def simulate(self, size=1, seed=None):
        """
        Sample or get values from this model i.e. the generated data
        Note: the size parameter here is the number of desired "rows" of data
        Example with two components and uniform weights:
            calling generator.simulate(100) will combine comp1.simulate(50) and comp2.simulate(50)
        :param size: the number of values to sample
        :param seed: random seed
        :return: list of values sampled from this component
        """
        pass

    @abstractmethod
    def likelihood(self, data):
        """
        Returns the likelihood or probability of permutation of values to be sampled from this generator
        :param data: of values to get likelihood of, can support non-list values for 1D components
                    e.g. 1D: [0.1, 0.3] (two values with 1 dimension)    2D: [[0.1, 0.3]] (one value with 2 dimensions)
        :return: probability of combination being sampled
        """
        pass
