#!/bin/bash
set -ev

# fyi, the logic below is necessary due to the fact that on a tagged build, TRAVIS_BRANCH and TRAVIS_TAG are the same
# in the case of a tagged build, use the REAL_BRANCH env var defined in travis.yml
if [ -n "${TRAVIS_TAG}" ]; then
  conda install anaconda-client
  # if tag didn't come from master, add the "dev" label
  if [ ${REAL_BRANCH} = "master" ]; then
    anaconda -t ${CONDA_UPLOAD_TOKEN} upload -u ${CONDA_USER} ~/miniconda/conda-bld/linux-64/${PACKAGE_NAME}-*.tar.bz2 --force
  else
    anaconda -t ${CONDA_UPLOAD_TOKEN} upload -u ${CONDA_USER} -l dev ~/miniconda/conda-bld/linux-64/${PACKAGE_NAME}-*.tar.bz2 --force
  fi
elif [ ${TRAVIS_BRANCH} = "master" ]; then
  if [ ${TRAVIS_EVENT_TYPE} = "cron" ]; then
    # don't build package for nightly cron.. this is just for test stability info
    exit 0
  else
    conda install anaconda-client
    anaconda -t ${CONDA_UPLOAD_TOKEN} upload -u ${CONDA_USER} -l edge ~/miniconda/conda-bld/linux-64/${PACKAGE_NAME}-*.tar.bz2 --force
  fi
else
  exit 0
fi
