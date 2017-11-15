#!/bin/bash
set -ev

# if this build was invoked by a tag or a cron, then upload the package. otherwise exit.
if [ -n "${TRAVIS_TAG}" ]; then
  conda install anaconda-client
  # if tag didn't come from master, add the "dev" label
  if [ $(git branch --contains tags/${TRAVIS_TAG}) = "master" ]; then
    anaconda -t ${CONDA_UPLOAD_TOKEN} upload -u ${CONDA_USER} ~/miniconda/conda-bld/linux-64/bayeslite-*.tar.bz2 --force
  else
    anaconda -t ${CONDA_UPLOAD_TOKEN} upload -u ${CONDA_USER} -l dev ~/miniconda/conda-bld/linux-64/bayeslite-*.tar.bz2 --force
  fi
elif [ ${TRAVIS_EVENT_TYPE} = "cron" ]; then
  conda install anaconda-client
  anaconda -t ${CONDA_UPLOAD_TOKEN} upload -u ${CONDA_USER} -l nightly ~/miniconda/conda-bld/linux-64/bayeslite-*.tar.bz2 --force
else
  exit 0
fi
