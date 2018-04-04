import math

import numpy as np

from tests.data_generator.data_gen_source.abstract.abstract_data_generator import AbstractDataGenerator
from tests.data_generator.data_gen_source.gaussian.gaussian_component import GaussianComponent


class GaussianGenerator(AbstractDataGenerator):
    def __init__(self, component_parameters, weights=None):
        super(GaussianGenerator, self).__init__(component_parameters, weights)
        self.__init_components()

    def __init_components(self):
        self.components = []
        if type(self.component_parameters[0]) is not list:  # passed in component_parameters were just [0, 1]
            # 1D 1 Component Gaussian
            self.components.append(GaussianComponent(*self.component_parameters))
            return
        for params in self.component_parameters:
            self.components.append(GaussianComponent(*params))

    def likelihood(self, data):
        if type(data) != list:  # wrap univariate outer to support single value input
            data = [data]

        total_probability = 0
        for d in data:
            value_probability = 0
            for index in range(len(self.components)):
                value_probability += self.weights[index] * math.e ** self.components[index].likelihood(d)
                # e is to convert logpdf -> pdf so I can add probabilities (to get OR probability)
            total_probability += np.log(value_probability)  # turn into log only after adding together
            # sum for probability happening all at once because logarithm
        return total_probability

    def simulate(self, size=1, seed=None):
        out = []
        for index, component in enumerate(self.components):
            sample = component.simulate(int(round(size * self.weights[index])), seed)  # sample based on size of weight
            out.extend(sample)

        while len(out) > size:  # rounding gives too much samples
            out.pop(np.random.choice(range(len(out))))  # pick a random choice to pop
        while len(out) < size:  # rounded gives too little samples
            out.extend(np.random.choice(self.components, p=self.weights).simulate(size - len(out), seed))
            # pick a component based on weights and simulate from it

        return out
