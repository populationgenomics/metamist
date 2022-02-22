FROM debian:buster-slim

ARG SM_ENVIRONMENT

ENV MAMBA_ROOT_PREFIX /root/micromamba
ENV PATH $MAMBA_ROOT_PREFIX/bin:$PATH
ENV PORT 8000
ENV SM_ENVIRONMENT ${SM_ENVIRONMENT}
# Allow statements and log messages to immediately appear in the Knative logs.
ENV PYTHONUNBUFFERED 1

EXPOSE $PORT

WORKDIR /app/sample_metadata/
COPY requirements.txt requirements.txt

RUN apt-get update && \
    apt-get install -y wget bash bzip2 zip build-essential && \
    rm -r /var/lib/apt/lists/* /var/cache/apt/* && \
    wget -qO- https://api.anaconda.org/download/conda-forge/micromamba/0.8.2/linux-64/micromamba-0.8.2-he9b6cbd_0.tar.bz2 | tar -xvj -C /usr/local bin/micromamba && \
    mkdir $MAMBA_ROOT_PREFIX && \
    micromamba install -y --prefix $MAMBA_ROOT_PREFIX -c conda-forge \
        python=3.10 pip && \
    pip install -r requirements.txt && \
    rm -r /root/micromamba/pkgs

COPY api api
COPY db db/
COPY models models/

CMD uvicorn --port ${PORT} --host 0.0.0.0 api.server:app
