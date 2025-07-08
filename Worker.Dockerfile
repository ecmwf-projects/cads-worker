FROM ghcr.io/dask/dask:2025.1.0-py3.11

ARG MODE=stable
ARG CADS_PAT
ARG CDS_PAT

WORKDIR /src

COPY ./git-*-repos.py /src/

COPY environment.${MODE} /src/environment
COPY default-enviroment.yaml /src/environment.yml
COPY environment-common.yml /src/environment-common.yml
COPY environment-dask.yml /src/environment-dask.yml

RUN conda install -y -n base -c conda-forge gitpython typer conda-merge

#COPY ./worker/environment.yml /src/environment.yml

SHELL ["/bin/bash", "-c"]

RUN set -a && source environment \
    && CADS_PAT=${CADS_PAT} \
    && CDS_PAT=${CDS_PAT} \
    && python ./git-clone-repos.py --default-branch \
    cacholote \
    cads-adaptors \
    cads-broker \
    cads-common \
    cads-mars-server \
    cads-worker \
    --bitbucket cds-common@cds-common \
    --bitbucket cdscompute@cdscompute

RUN conda run -n base conda-merge \
    /src/environment-common.yml \
    /src/environment-dask.yml \
    /src/environment.yml \
    /src/cacholote/environment.yml \
    /src/cads-adaptors/environment.yml \
    /src/cads-broker/environment.yml \
    /src/cads-common/environment.yml \
    /src/cads-mars-server/environment.yml \
    /src/cads-worker/environment.yml \
    > /src/combined-environment.yml \
    && mamba env update -n base -f /src/combined-environment.yml \
    && conda clean -afy

RUN conda run -n base pip install --no-deps \
    -e ./cacholote \
    -e ./cads-adaptors \
    -e ./cads-broker \
    -e ./cads-common \
    -e ./cads-mars-server \
    -e ./cads-worker \
    && conda run -n base pip install \
    -e ./cds-common \
    -e ./cdscompute

RUN mkdir -p /cache/downloads/cams-europe-air-quality-forecasts/ \
    && mkdir -p /cache/tmp/ \
    && mkdir -p /cache/debug/

WORKDIR /root
