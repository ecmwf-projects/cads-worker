FROM continuumio/miniconda3

WORKDIR /src/cads-worker

COPY environment.yml /src/cads-worker/

RUN conda install -c conda-forge gcc python=3.11 \
    && conda env update -n base -f environment.yml

COPY . /src/cads-worker

RUN pip install --no-deps -e .
