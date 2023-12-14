FROM python:3.11

ARG SM_ENVIRONMENT

ENV PORT 8000
ENV SM_ENVIRONMENT ${SM_ENVIRONMENT}
# Allow statements and log messages to immediately appear in the Knative logs.
ENV PYTHONUNBUFFERED 1

EXPOSE $PORT

WORKDIR /app/sample_metadata/
COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY api api
COPY db db/
COPY models models/

CMD uvicorn --port ${PORT} --host 0.0.0.0 api.server:app
