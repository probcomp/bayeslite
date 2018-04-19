import pandas as pd

from data_generator.data_gen_source.categorical.categorical_generator import CategoricalGenerator
from data_generator.data_gen_source.gaussian.gaussian_generator import GaussianGenerator


def data_to_likelihood(in_path, component_type, component_parameters, weights=None, out_path=None):
    """
    Takes data and returns a DataFrame with the likelihood of each row
    :param in_path: input path of data in .csv form
    :param component_type: type of distribution i.e. "gaussian" or "categorical"
    :param component_parameters: of distribution to use, see abstract_data_generator and its concrete classes
    :param weights: of distribution to use
    :param out_path: output path to save the likelihood as a .csv, None to not save
    :return: DataFrame of likelihood with shape: same number of rows as data, 1 column
            also writes to out_path if given
    """
    if component_type == "categorical":
        generator = CategoricalGenerator(component_parameters, weights)
    elif component_type == "gaussian":
        generator = GaussianGenerator(component_parameters, weights)
    else:
        raise ValueError("unrecognized component_type: " + component_type)

    read_data = pd.read_csv(in_path)
    likelihood_data = []

    for row_index in range(read_data.shape[0]):
        formatted = [read_data.loc[row_index].tolist()]
        # need to wrap in a list to count as one sample with multiple dimensions
        likelihood_data.append(generator.likelihood(formatted))
        # same row, first (and only) column
    likelihood_data = pd.DataFrame(data=likelihood_data, columns=["probability"])

    if out_path is not None:
        likelihood_data.to_csv(out_path, index=False, header=True)

    return likelihood_data


likelihood = data_to_likelihood(in_path="gaussian.csv", component_type="gaussian",
                                component_parameters=[[[0, 5], [0.4, 0.4]], [[10, 15], [0.1, 0.1]]],
                                out_path="gaussian_likelihood.csv")
