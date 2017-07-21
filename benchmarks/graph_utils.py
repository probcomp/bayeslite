import csv
import time

from collections import OrderedDict
from itertools import chain

import matplotlib.pyplot as plt


def read_data(csv_file_name):
    with open(csv_file_name, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        data = OrderedDict()
        for num_rows in reader.next():
            data[num_rows] = []
        for row in reader:
            for index in range(len(row)):
                data[data.keys()[index]].append(row[index])
    return data


def data_to_axes(data):
    x_analyze = list(chain(*([[num_rows]*len(data[num_rows])
        for num_rows in data.keys()])))
    y_analyze = list(chain(*data.values()))
    return x_analyze, y_analyze


def graph_one(file_1, title):
    x, y = data_to_axes(read_data(file_1))
    plt.figure()
    plt.scatter(x, y, c='b')
    plt.title(title)
    plt.xlabel('Number of Rows')
    plt.ylabel('Analysis Time (sec)')
    plt.legend(loc='upper left')
    name = '%s-%s.png' % (time.time(), 'synthetic')
    plt.savefig(name)
    print(name)
    plt.show()


def two(cgpm, loom, title):
    x_1, y_1 = data_to_axes(read_data(cgpm))
    x_2, y_2 = data_to_axes(read_data(loom))
    plt.figure()
    plt.scatter(x_1, y_1, c='r', label='cgpm')
    plt.scatter(x_2, y_2, c='b', label='loom')
    plt.title(title)
    plt.xlabel('Number of Rows')
    plt.ylabel('Analysis Time (sec)')
    plt.legend(loc='upper left')
    plt.yscale('log')
    name = '%s-%s.png' % (time.time(), 'synthetic')
    plt.savefig(name)
    print(name)
    plt.show()


two('1500644539.26-cgpm-1v1c-analyze.csv',
    '1500560082.92-loom-analyze.csv',
    '''Loom vs CGPM: Single Model Analysis Runtime\n
    On A One View One Cluster Dataset''')
