# Running tests locally

Clone the repo and install the env

```bash
git clone https://github.com/populationgenomics/sample-metadata
cd sample-metadata
conda env create -n sample-metadata --file environment-dev.yml
conda activate sample-metadata
```

Configure required environment variables

```bash
export SM_ENVIRONMENT=LOCAL
export SM_DEV_DB_PROJECT=sm_dev
export SM_DEV_DB_USER=root
export SM_DEV_DB_PASSWORD=root
export SM_DEV_DB_PORT=3307
export SM_DEV_DB_HOST=127.0.0.1
```

Start the DB server

```bash
export NAME=mariadb-sm
docker stop $NAME
docker rm $NAME
docker run -d -p $SM_DEV_DB_PORT:3306 -e MARIADB_ALLOW_EMPTY_ROOT_PASSWORD=1 --name $NAME mariadb
docker inspect --format="{{if .Config.Healthcheck}}{{print .State.Health.Status}}{{end}}" $NAME
# Wait until started
until mysql --host=$SM_DEV_DB_HOST --port=$SM_DEV_DB_PORT -u$SM_DEV_DB_USER -e 'show databases;'; do sleep 3; done
```

Create a DB

```bash
mysql --host=$SM_DEV_DB_HOST --port=$SM_DEV_DB_PORT -u$SM_DEV_DB_USER -e 'CREATE DATABASE '$SM_DEV_DB_PROJECT';'
# mysql --host=$SM_DEV_DB_HOST --port=$SM_DEV_DB_PORT -u$SM_DEV_DB_USER -e 'show databases;'
```

Install tables

```bash
cd db
liquibase update --url jdbc:mariadb://$SM_DEV_DB_HOST:$SM_DEV_DB_PORT/$SM_DEV_DB_PROJECT --username=$SM_DEV_DB_USER --classpath mariadb-java-client-2.7.3.jar --changelog-file=project.xml

# mysql --host=$SM_DEV_DB_HOST --port=$SM_DEV_DB_PORT -u $SM_DEV_DB_USER -e 'use '$SM_DEV_DB_PROJECT'; show tables;'
```

Add project into the DB

```bash
export INPUT_PROJECT=test_input_project
export OUTPUT_PROJECT=test_output_project
export USER=sample-metadata-deploy@sample-metadata.iam.gserviceaccount.com
export GCP_ID=sample-metadata

mysql --host=$SM_DEV_DB_HOST --port=$SM_DEV_DB_PORT -u $SM_DEV_DB_USER -e 'use '$SM_DEV_DB_PROJECT'; insert into project (id, name, author, dataset, gcp_id, read_secret_name, write_secret_name) \
values \
(1, "'$INPUT_PROJECT'", "'$USER'", "'$INPUT_PROJECT'", "'$GCP_ID'", "'$INPUT_PROJECT'-ci-sample-metadata-main-read-members-cache", "'$INPUT_PROJECT'-ci-sample-metadata-main-write-members-cache"), \
(2, "'$OUTPUT_PROJECT'", "'$USER'", "'$OUTPUT_PROJECT'", "'$GCP_ID'", "'$OUTPUT_PROJECT'-ci-sample-metadata-main-read-members-cache", "'$OUTPUT_PROJECT'-ci-sample-metadata-main-write-members-cache");'

mysql --host=$SM_DEV_DB_HOST --port=$SM_DEV_DB_PORT -u $SM_DEV_DB_USER -e 'use '$SM_DEV_DB_PROJECT'; select * from project;'
```

Create secrets to test access to a project

```bash
# To read and NOT write input project:
gcloud secrets create $INPUT_PROJECT-ci-sample-metadata-main-read-members-cache --project $GCP_ID
gcloud secrets create $INPUT_PROJECT-ci-sample-metadata-main-write-members-cache --project $GCP_ID

gcloud secrets versions add $INPUT_PROJECT-ci-sample-metadata-main-read-members-cache --data-file=<(echo ,$USER,) --project $GCP_ID
# Note empty user list for the write secret:
gcloud secrets versions add $INPUT_PROJECT-ci-sample-metadata-main-write-members-cache --data-file=<(echo ,) --project $GCP_ID

# To read and write input project:
gcloud secrets create $OUTPUT_PROJECT-ci-sample-metadata-main-read-members-cache --project $GCP_ID
gcloud secrets create $OUTPUT_PROJECT-ci-sample-metadata-main-write-members-cache --project $GCP_ID

gcloud secrets versions add $OUTPUT_PROJECT-ci-sample-metadata-main-read-members-cache --data-file=<(echo ,$USER,) --project $GCP_ID
gcloud secrets versions add $OUTPUT_PROJECT-ci-sample-metadata-main-write-members-cache --data-file=<(echo ,$USER,) --project $GCP_ID
```

Generate and install API

```bash
python regenerate_api.py
pip install -e .
```

Start the server to populate samples (can do in a separate window)

```bash
export SM_ALLOWALLACCESS=1
python3 -m api.server
```

Populate samples

```bash
python test/test_add_samples_for_joint_calling.py
```

Stop the server and restart with SM_ALLOWALLACCESS unset, to test permissions

```bash
export SM_ALLOWALLACCESS=0
python3 -m api.server
```

Run the test that simulates the joint-calling workflow

```bash
python test/test_joint_calling_workflow.py
```
