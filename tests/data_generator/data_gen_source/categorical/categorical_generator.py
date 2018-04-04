from numpy.random import np

from tests.data_generator.data_gen_source.abstract.abstract_data_generator import AbstractDataGenerator
from tests.data_generator.data_gen_source.categorical.categorical_component import CategoricalComponent


class CategoricalGenerator(AbstractDataGenerator):
    def __init__(self, component_parameters, weights=None):
        super(CategoricalGenerator, self).__init__(component_parameters, weights)
        self.__init_components()

    def __init_components(self):
        self.components = []
        for params in self.component_parameters:
            self.components.append(CategoricalComponent(params))

    def likelihood(self, data):
        if type(data) is not list:
            data = [data]

        total_probability = 1.0
        for d in data:
            value_probability = 0.0
            for index in range(len(self.components)):
                value_probability += self.weights[index] * self.components[index].likelihood(d)
            total_probability *= value_probability
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
