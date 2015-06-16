import numpy as np
import pandas as pd
from scipy.spatial.distance import pdist, squareform
from scipy.stats.mstats import rankdata

import random
import csv


def read_csv_header(csv_filename):
    with open(csv_filename, 'rb') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        header = csvreader.next()
    return header


def remove_csv_values(csv_filename, out_filename, cols, prop_missing):
    """Remove values from a column in in csv and save the output.

    Parameters
    ----------
    csv_filename : str
        The path of the csv file from which to remove entries. This csv should
        have no missing entries.
    out_filename : str
        The path where the new csv is to be saved.
    cols : list<str>
        The columns from which to remove the values. Usually, all columns
        modeled by a generator.
    prop_missing : float
        The proportion of data, in  (0, 1), of data to remove.

    Returns
    -------
    indices : dict
        Each key is a colum, each value is a list of the key values for the
        rows of that column that were removed.
    values: dict
        Each key is a colum, each value is a list of the values that were
        removed. The order of these values correspondes to `indices`.
    """

    # assumes key is the first column in the csv
    df = pd.DataFrame.from_csv(csv_filename)
    keys = list(df.index)

    num_missing = int(df.shape[0]*df.shape[1]*prop_missing)
    assert num_missing > 0
    print num_missing

    all_indices = [(col, key,) for key in keys for col in cols]

    idxs = random.sample(all_indices, num_missing)

    indices = dict()
    values = dict()

    for col, idx in idxs:
        val = df.loc[idx, col]
        df.loc[idx, col] = float('NaN')
        if indices.get(col, None) is None:
            indices[col] = [idx]
            values[col] = [val]
        else:
            indices[col].append(idx)
            values[col].append(val)

    df.to_csv(out_filename, na_rep='NaN')

    return indices, values


def data_to_csv(data, filename):
    """Build header and save numpy data as filename"""
    num_cols = data.shape[1]
    if num_cols == 2:
        header = 'x,y'
    else:
        header = ','.join('c' + str(i) for i in range(num_cols))
    np.savetxt(filename, data, delimiter=',', header=header, comments='')


def kernel_two_sample_test(X, Y, permutations = 2500):
    '''
    This funciton tests the null hypothesis that X and Y are samples drawn
    from the same population of arbitrary dimension D. The permutation method
    (non-parametric) is used, the test statistic is 
    E[k(X,X')] + E[k(Y,Y')] - 2E[k(X,Y)].
    A Gaussian kernel is used with width equal to the median distance between
    vectors in the aggregate sample.

    For more information see:
        http://www.stat.berkeley.edu/~sbalakri/Papers/MMD12.pdf
        https://normaldeviate.wordpress.com/2012/07/14/modern-two-sample-tests/

    :param X: N by D numpy array of samples from the first population.
        Each row is a D-dimensional data point.
    :param Y: M by D numpy array of samples from the second population.
        Each row is a D-dimensional data point.
    :param permutations: (optional) number of times to resample, default 2500.

    :returns: p-value of the statistical test
    '''

    assert isinstance(X, np.ndarray)
    assert isinstance(Y, np.ndarray)
    assert X.shape[1] == Y.shape[1]

    N = X.shape[0]
    M = Y.shape[0]

    # compute the observed statistic
    t_star = _compute_kernel_statistic(X,Y)
    T = [t_star]

    # pool the samples
    S = np.vstack((X,Y))

    # compute resampled test statistics
    for k in xrange(permutations):
        np.random.shuffle(S)
        Xp, Yp = S[:N], S[N:]
        tb = _compute_kernel_statistic(Xp, Yp)
        T.append(tb)

    # fraction of samples larger than observed t_star
    f = len(T) - rankdata(T)[0]
    return 1. * f / (len(T))

def _compute_kernel_statistic(X, Y):
    """Compute a single two-sample test statistic"""
    assert isinstance(X, np.ndarray)
    assert isinstance(Y, np.ndarray)
    assert X.shape[1] == Y.shape[1]

    N = X.shape[0]
    M = Y.shape[0]

    # determine width of Gaussian kernel
    Pxyxy = pdist(np.vstack((X,Y)),'euclidean')
    s = np.median(Pxyxy)
    if s == 0:
        s = 1

    Kxy = squareform(Pxyxy)[:N,N:]
    Exy = np.exp(- Kxy ** 2 / s ** 2)
    Exy = np.mean(Exy)
    
    Kxx = squareform(pdist(X),'euclidean')
    Exx = np.exp(- Kxx ** 2 / s ** 2)
    Exx = np.mean(Exx)

    Kyy = squareform(pdist(Y),'euclidean')
    Eyy = np.exp(- Kyy ** 2 / s ** 2)
    Eyy = np.mean(Eyy)

    return Exx + Eyy - 2*Exy

def gen_zero_correlation_data(which, n, clarity):
    """Generate data from a zero-correlation dateset."""
    zcfuns = {
        'sine': _gen_sine_wave,
        'dots': _gen_dots,
        'diamond': _gen_diamond,
        'x': _gen_x,
        'ring': _gen_ring
        }
    func = zcfuns.get(which.lower(), None)
    if func is None:
        raise KeyError('Valid data generators are {}.'.format(zcfuns.keys()))
    return func(n, clarity)


# Zero-correlation data
def _gen_sine_wave(n, clarity):
    x = np.linspace(-5., 5., n)
    y = np.cos(x)
    data = np.zeros((n, 2))
    data[:, 0] = x
    data[:, 1] = y

    if clarity < 1.0:
        jitter_std = 2*(1.0-clarity)
        data += np.random.normal(0.0, jitter_std, data.shape)

    return data


def _gen_dots(n, clarity):
    clarity = min(.97, clarity)
    data = np.zeros((n, 2))
    centers = [-4, 4]
    sigma = np.eye(2)
    sigma[0, 0] = 6.0*(1.0-clarity)
    sigma[1, 1] = 6.0*(1.0-clarity)
    for i in range(n):
        mu = [random.choice(centers), random.choice(centers)]
        data[i, :] = np.random.multivariate_normal(mu, sigma)
    return data


def _gen_diamond(n, clarity):
    data = np.zeros((n, 2))
    for i in range(n):
        y = random.random()
        x = random.random()
        # rejection sampling
        while y > (1-x):
            y = random.random()
            x = random.random()
        if random.random() < .5:
            x = -x
        if random.random() < .5:
            y = -y
        data[i, :] = [x, y]

    if clarity < 1.0:
        jitter_std = (1.0-clarity)
        data += np.random.normal(0.0, jitter_std, data.shape)

    return data


def _gen_x(n, clarity):
    s = int(n/2.0)
    rho = clarity
    sigma_a = np.array([[1.0, rho], [rho, 1.0]])
    sigma_b = np.array([[1.0, -rho], [-rho, 1.0]])
    data = np.vstack((np.random.multivariate_normal([0, 0], sigma_a, s),
                      np.random.multivariate_normal([0, 0], sigma_b, s)))
    return data


def _gen_ring(n, clarity):
    width = max(.01, (1-clarity))
    rmax = 1.0
    rmin = rmax-width
    data = np.zeros((n, 2))

    def sample():
        x = 2*(random.random()-.5)
        y = 2*(random.random()-.5)
        r = (x**2 + y**2)**.5
        return x, y, r

    for i in range(n):
        x, y, r = sample()
        while r < rmin or rmax < r:
            x, y, r = sample()
        data[i, :] = [x, y]
    return data

def main():
    from matplotlib import pyplot as plt
    from matplotlib import gridspec

    n = 1000
    clarity = [0, .1, .25, .5, .75, .9, 1.0]
    types = ['dots', 'sine', 'x', 'diamond', 'ring']

    plt.figure(tight_layout=True, facecolor='white')
    gs = gridspec.GridSpec(len(types), len(clarity))

    for i, c in enumerate(clarity):
        for j, w in enumerate(types):
            ax = plt.subplot(gs[j, i])
            data = gen_zero_correlation_data(w, n, c)
            ax.scatter(data[:, 0], data[:, 1], alpha=.5, color='deeppink')
            if j == len(types) - 1:
                ax.set_xlabel('Clarity = %1.2f' % (c,))
            if c == 0:
                ax.set_ylabel(w)
    plt.show()

# if __name__ == "__main__":
    # main()
    