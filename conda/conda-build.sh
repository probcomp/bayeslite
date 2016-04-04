#!/bin/bash

set -xe

conda install conda-build
# Build crosscat package
conda skeleton pypi crosscat && conda build crosscat && rm -rf crosscat
# Build bayeslite-apsw package
conda skeleton pypi bayeslite-apsw
# conda-build complains about the "-" in the version field and the "::" in the
# licence field of the meta.yaml file. Remove them.
sed -i .bak 's/version: "3.9.2-r1"/version: "3.9.2"/' bayeslite-apsw/meta.yaml
sed -i .bak 's/:://' bayeslite-apsw/meta.yaml
conda build bayeslite-apsw && rm -rf bayeslite-apsw
# Build bayeslite package
conda skeleton pypi bayeslite && conda build bayeslite && rm -rf bayeslite
