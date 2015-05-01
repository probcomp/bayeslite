import numpy as np
import random


def data_to_csv(data, filename):
    """Build header and save numpy data as filename"""
    num_cols = data.shape[1]
    if num_cols == 2:
        header = 'x,y'
    else:
        header = ','.join('c' + str(i) for i in range(num_cols))
    np.savetxt(filename, data, delimiter=',', header=header, comments='')


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


if __name__ == "__main__":
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
