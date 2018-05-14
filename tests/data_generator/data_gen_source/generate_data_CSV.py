import pandas as pd
import numpy as np
from matplotlib import pyplot as plt

from tests.data_generator.data_gen_source.categorical.categorical_generator import CategoricalGenerator
from tests.data_generator.data_gen_source.gaussian.gaussian_generator import GaussianGenerator


def generate_gaussian(gaussian_parameters, rows, out_path=None, seed=None):
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
    if out_path is None:
        out_path = "gaussian.csv"

    gaussian_generator = GaussianGenerator(gaussian_parameters)
    samples = gaussian_generator.simulate(rows, seed)

    data_frame = pd.DataFrame(samples)
    data_frame.to_csv(out_path, header=True, index=False)

    return data_frame


def generate_categorical(categorical_parameters, rows, out_path=None, seed=None):
    """
    Generates data from a categorical mixture model
    :param categorical_parameters: for Categorical generator, see categorical_generator.py
        example 2 component:
            [{"a": 0.5, "b": 0.5}, {"a": 0.5, "c": 0.5}]
            is [{1st component category: 1st component probability} ...
    :param rows: of data to generate
    :return: data written to categorical.csv in same directory, data as Pandas dataframe
    """
    if out_path is None:
        out_path = "categorical.csv"

    categorical_generator = CategoricalGenerator(categorical_parameters)
    samples = categorical_generator.simulate(rows, seed)

    data_frame = pd.DataFrame(samples)
    data_frame.to_csv(out_path, header=True, index=False)

    return data_frame


# generate_gaussian([[[0, 5], [0.4, 0.4]], [[10, 15], [0.1, 0.1]]], 1000)

