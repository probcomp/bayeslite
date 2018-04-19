import pandas as pd
import numpy as np
from matplotlib import pyplot as plt

from categorical.categorical_generator import CategoricalGenerator
from gaussian.gaussian_generator import GaussianGenerator

import sys
import os
sys.path.append(os.getcwd())


def generate_gaussian(gaussian_parameters, rows):
    """
    Generates data from a Gaussian mixture model
    :param gaussian_parameters: for Gaussian generator, see gaussian_generator.py
        example 1-dimensional, 2 component:
            [ [0, 0.1], [10, 0.1] ]
            is [[mean of 1st component, deviation of 1st component] ...
        example 2-dimensional, 2 component:
            [ [[0, 0], [0.5, 0.5]], [[10, 10], [1, 1]] ]
            is [ [[mean 1st dimension 1st component, mean 2nd dimension 2nd component] ...
    :param rows: of data to generate
    :return: data written to gaussian.csv in same directory, data as Pandas dataframe
    """
    gaussian_generator = GaussianGenerator(gaussian_parameters)
    samples = gaussian_generator.simulate(rows)

    data_frame = pd.DataFrame(samples)
    data_frame.to_csv("gaussian.csv", header=True, index=False)

    return data_frame


def generate_categorical(categorical_parameters, rows):
    """
    Generates data from a categorical mixture model
    :param categorical_parameters: for Categorical generator, see categorical_generator.py
        example 2 component:
            [{"a": 0.5, "b": 0.5}, {"a": 0.5, "c": 0.5}]
            is [{1st component category: 1st component probability} ...
    :param rows: of data to generate
    :return: data written to categorical.csv in same directory, data as Pandas dataframe
    """

    categorical_generator = CategoricalGenerator(categorical_parameters)
    samples = categorical_generator.simulate(rows)

    data = np.reshape(samples, rows)
    data_frame = pd.DataFrame(data)

    data_frame.to_csv("categorical.csv", header=True, index=False)


generate_gaussian([[[0, 5], [0.4, 0.4]], [[10, 15], [0.1, 0.1]]], 1000)

