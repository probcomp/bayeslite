FROM continuumio/anaconda:dda6835905cae649a9fa59685ab47066f2c18de4dd03e9e761d74a857e9960c7

RUN apt-get update && apt-get install -y g++ git libboost-dev
RUN conda install conda-build
# Build crosscat package
RUN conda skeleton pypi crosscat && conda build crosscat && rm -rf crosscat
# Build bayeslite-apsw package
RUN conda skeleton pypi bayeslite-apsw
# conda-build complains about the "-" in the version field and the "::" in the
# licence field of the meta.yaml file. Remove them.
RUN sed -i 's/version: "3.9.2-r1"/version: "3.9.2"/' bayeslite-apsw/meta.yaml && \
    sed -i 's/:://' bayeslite-apsw/meta.yaml
RUN conda build bayeslite-apsw && rm -rf bayeslite-apsw
# Build bayeslite package
RUN conda skeleton pypi bayeslite && conda build bayeslite && rm -rf bayeslite
