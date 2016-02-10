# Dockerfile that builds, installs, and tests bayeslite. It is for development
# only; users should use the python package.

FROM        ubuntu:15.10
RUN         apt-get update -qq

# For installation: python, pip, apt, pkg
# For CrossCat: libboost-dev
# Faster than if pip did it: numpy, scipy, pandas
RUN apt-get install -y -qq python2.7-dev python-pip apt-utils pkg-config \
    libboost-dev python-numpy python-scipy python-pandas

# For installation: setuptools
# For testing: pexpect, cython
RUN pip -q install setuptools pexpect cython

RUN BOOST_ROOT=/usr/include pip -q install crosscat

WORKDIR /bayeslite
COPY Makefile VERSION check.sh pythenv.sh setup.py ./
COPY external ./external
COPY src ./src
COPY tests ./tests
COPY shell ./shell
RUN for d in tests shell; do find $d -name __pycache__ -o -name "*.pyc" -exec rm -fr {} \; ; done

RUN     pip -q install .
RUN     ./check.sh tests/ shell/tests/
