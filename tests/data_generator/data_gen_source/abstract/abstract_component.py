from abc import ABCMeta, abstractmethod


class AbstractComponent:
    """
    Component of a mixture model that supports sampling as well as a likelihood / log of probability density
    function
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def simulate(self, size, seed=None):
        """
        Sample or get values from this component
        Note: this samples a "row" of data e.g. simulate on a 2D component will give 2 simulated values,
            one for each of the dimensions (which may have different distributions)
        :param size: the number of values to sample
        :param seed: random seed
        :return: list of values sampled from this component (always in a list, even for a single 1D value)
        """
        pass

    @abstractmethod
    def likelihood(self, data):
        """
        Returns the likelihood or probability of the given values to be sampled from this component
        :param data: of values to get likelihood of, can support non-list values for 1D components
                    e.g. 1D: [0.1, 0.3] (two values with 1 dimension)    2D: [[0.1, 0.3]] (one value with 2 dimensions)
        :return: probability of combination being sampled
        """
        pass
