FROM liquibase/liquibase:4.4.2

ENV MAMBA_ROOT_PREFIX /root/micromamba
ENV PATH $MAMBA_ROOT_PREFIX/bin:$PATH
ENV PORT 8000
# Allow statements and log messages to immediately appear in the Knative logs.
ENV PYTHONUNBUFFERED 1

USER root

EXPOSE $PORT

WORKDIR /app/

RUN apt-get update && \
    apt-get install -y wget bash bzip2 zip && \
    rm -r /var/lib/apt/lists/* /var/cache/apt/* && \
    wget -qO- https://api.anaconda.org/download/conda-forge/micromamba/0.8.2/linux-64/micromamba-0.8.2-he9b6cbd_0.tar.bz2 | tar -xvj -C /usr/local bin/micromamba && \
    wget 'https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/2.7.2/mariadb-java-client-2.7.2.jar' && \
    mkdir $MAMBA_ROOT_PREFIX && \
    micromamba install -y --prefix $MAMBA_ROOT_PREFIX \
    -c conda-forge python=3.7 pip && \
    pip install pymysql google-cloud-secret-manager==2.2.0 fastapi[all]==0.65.1 uvicorn==0.13.4 && \
    rm -r /root/micromamba/pkgs

COPY main.py /app/main.py

COPY --from=us-docker.pkg.dev/berglas/berglas/berglas:latest /bin/berglas /bin/berglas

ENTRYPOINT exec /bin/berglas exec -- uvicorn --port ${PORT} --host 0.0.0.0 main:app
