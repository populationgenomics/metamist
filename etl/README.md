# ETL

Cloud Functions for ETL.

## ETL_POST

etl_post function accepts json payload. It will insert new record in BigQuery table defined by env variable BIGQUERY_TABLE and pushes new message to PUBSUB_TOPIC


## ETL_LOAD

etl_load function expects "request_id" in the payload. It is setup as push subscriber to PUBSUB_TOPIC.


## How to test locally

Please use your personal dev project as `$PROJECT_NAME`.

### 1. Setup your environment

```bash
# setup gcloud authentication
gcloud auth application-default login

export PROJECT_NAME="gcp-project-name"
export BIGQUERY_TABLE="$PROJECT_NAME.metamist.etl-data"
export BIGQUERY_LOG_TABLE="$PROJECT_NAME.metamist.etl-logs"
export PUBSUB_TOPIC="projects/$PROJECT_NAME/topics/etl-topic"

# setup to run local version of sample-metadata
export SM_ENVIRONMENT=local
```

### 2. Create BQ table "$PROJECT_NAME.metamist.etl-data"

### 3. Create TOPIC "projects/$PROJECT_NAME/topics/etl-topic"

### 4. Setup python env

```bash
cd post
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

### 5. Start EXTRACT Fun locally

```bash
functions-framework-python --target etl_extract --debug
```

### 6. Call etl_extract

```bash
curl -X 'POST' \
  'http://localhost:8080/' \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -d '{"identifier": "AB0002", "name": "j smith", "age": 50, "measurement": "98.7", "observation": "B++", "receipt_date": "1/02/2023"}'
```

Should return something like this:

```bash
{
  "id": "76263e55-a869-4604-afe2-441d9c20221e",
  "success": true
}
```

### 7. Start LOAD Fun locally

Repeat Step 4 inside folder load

```bash
functions-framework-python --target etl_load --debug
```

### 8. Call etl_load

Replace request_id with the id returned in Step 6

```bash
curl -X 'POST' \
  'http://localhost:8080/' \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -d '{"request_id": "76263e55-a869-4604-afe2-441d9c20221e"}'
```

Should return something like this:

```bash
{
  "id": "76263e55-a869-4604-afe2-441d9c20221e",
  "success": true
}
```


### 9. Deploy functions for testing on the cloud

```bash
cd ../load

gcloud functions deploy etl_load \
    --gen2 \
    --runtime=python311 \
    --project=$PROJECT_NAME \
    --region=australia-southeast1 \
    --source=. \
    --entry-point=etl_load \
    --trigger-http \
    --no-allow-unauthenticated \
    --set-env-vars BIGQUERY_TABLE=$BIGQUERY_TABLE \
    --set-env-vars PUBSUB_TOPIC=$PUBSUB_TOPIC
```

```bash
cd ../extract

gcloud functions deploy etl_extract \
    --gen2 \
    --runtime=python311 \
    --project=$PROJECT_NAME \
    --region=australia-southeast1 \
    --source=. \
    --entry-point=etl_post \
    --trigger-http \
    --no-allow-unauthenticated \
    --set-env-vars BIGQUERY_TABLE=$BIGQUERY_TABLE \
    --set-env-vars PUBSUB_TOPIC=$PUBSUB_TOPIC
```
