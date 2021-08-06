FROM debian:buster-slim

ENV MAMBA_ROOT_PREFIX /root/micromamba
ENV PATH $MAMBA_ROOT_PREFIX/bin:$PATH
ENV SM_ENVIRONMENT PRODUCTION

ARG SM_VERSION

COPY requirements.txt requirements.txt

RUN apt-get update && \
    apt-get install -y wget bash bzip2 zip && \
	wget -qO- https://api.anaconda.org/download/conda-forge/micromamba/0.8.2/linux-64/micromamba-0.8.2-he9b6cbd_0.tar.bz2 | tar -xvj -C /usr/local bin/micromamba && \
    mkdir $MAMBA_ROOT_PREFIX && \
    micromamba install -y --prefix $MAMBA_ROOT_PREFIX -c cpg -c bioconda -c conda-forge google-cloud-sdk sample-metadata==${SM_VERSION} && \
    pip install -r requirements.txt && \
    rm -r /root/micromamba/pkgs
