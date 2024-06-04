#!/bin/bash

# To avoid pip-compile deciding requirements on your specific machine,
# we use a docker container to compile the requirements, which should match
# the architecture of the dev environment.

docker run --platform linux/amd64 -v $(pwd):/opt/metamist python:3.11 /bin/bash -c '
    cd /opt/metamist;
    pip install pip-tools;
    pip-compile requirements.in > requirements.txt;
    pip-compile --output-file=requirements-dev.txt requirements-dev.in requirements.in
'
