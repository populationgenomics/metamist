# Sample Metadata

An API to manage sample + other related metadata.

## Environment variables

Sample-metadata API uses environment variables to manage it's configuration.

### Running the web server

The default behaviour for the web server is to:

- Use a GCP secret for the DB credentials

You can configure this with:

```shell
# use credentials defined in db/python/connect.py:dev_config
export SM_ENVIRONMENT=dev
# use specific mysql settings
export SM_DEV_DB_PROJECT=sm_dev
export SM_DEV_DB_USER=root
export SM_DEV_DB_PORT=3307
export SM_DEV_DB_HOST=127.0.0.1
export SM_DEV_DB_PASSWORD=root
export SM_ENVIRONMENT=dev
```

### Using the Python API

The default behaviour for the installable Python API is to:

- Use the production server
- With a service account

You can configure these separately with:

```shell
# use localhost:8000
export SM_ENVIRONMENT=dev
 # use a local account to generate identity-token for auth
export SM_USE_SERVICE_ACCOUNT=false
```


## Database

Sample-metadata DB uses MariaDB (for the system-versioned-tables) to store metadata.

There are two dbs you'll need to initialise:

- `sm_admin`: Stores project-independent metadata (like the internal_sample_id to project map)
- `sm_<project>`: Stores project specicic information.

In development, we recommend `sm_dev`, as [there is a default dev_config](https://github.com/populationgenomics/sample-metadata/blob/8b122453d1cd26c09966b8e54909bf712da5263e/db/python/connect.py#L78-L85).

Schema is managed with _liquibase_ with the `project.xml` and `global.xml` changelogs. To use liquibase, you'll need to download the `mariadb` driver:

```shell
pushd db/
wget https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/2.7.2/mariadb-java-client-2.7.2.jar
popd
```

### Using Maria DB docker image

Pull mariadb image

```bash
docker pull mariadb
```

Run a mariadb container that will server your database. `-p 3307:3306` remaps the port to 3307 in case if you local MySQL is already using 3306

```bash
docker stop mysql-p3307  # stop and remove if the container already exists
docker rm mysql-p3307
docker run -p 3307:3306 --name mysql-p3307 -e MYSQL_ROOT_PASSWORD=root -d mariadb
```

Initialize databases (you may need to enter the password on each command).

```bash
mysql --host=127.0.0.1 --port=3307 -u root -p -e 'CREATE DATABASE sm_dev;'
mysql --host=127.0.0.1 --port=3307 -u root -p -e 'CREATE DATABASE sm_admin;'
mysql --host=127.0.0.1 --port=3307 -u root -p -e 'show databases;'
```

Download [`mariadb-java-client-2.7.2.jar`](https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/2.7.2/mariadb-java-client-2.7.2.jar) into the `db/` folder.

Then create the database schemas.

```bash
pushd db/
liquibase update --url jdbc:mariadb://127.0.0.1:3307/sm_admin --username=root --password=root --classpath mariadb-java-client-2.7.3.jar --changelog-file=global.xml
liquibase update --url jdbc:mariadb://127.0.0.1:3307/sm_dev --username=root --password=root --classpath mariadb-java-client-2.7.3.jar --changelog-file=project.xml
```

Finally, start the server, making use of the environment variables to point it to your local Maria DB server

```bash
export SM_DEV_DB_PROJECT=sm_dev
export SM_DEV_DB_USER=root
export SM_DEV_DB_PORT=3307
export SM_DEV_DB_HOST=127.0.0.1
export SM_DEV_DB_PASSWORD=root
export SM_ENVIRONMENT=dev
export SM_USE_SERVICE_ACCOUNT=false
python3 -m api.server
```

## Debugging

### Start the server

```bash
python3 -m api.server
# or
gunicorn --bind :$PORT --worker-class aiohttp.GunicornWebWorker api.main:start_app
```

### Running a file directly

Due to the way python imports work, you're unable to run files directly. To run files, you must run them as a module, for example, to run the `db/python/layers/sample.py` file directly (in case you put an `if __name__ == "__main__"` block in there), you could use the following:

```bash
# convert '/' to '.' and drop the '.py'
python -m db.python.layers.sample
```

### VSCode

VSCode allows you to debug python modules, we could debug the web API at `api/server.py` by considering the following `launch.json`:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: SampleLayer",
            "type": "python",
            "request": "launch",
            "module": "api.server"
        }
    ]
}
```

We could now place breakpoints on the sample route (ie: `api/routes/sample.py`), and debug requests as they come in.

### OpenAPI and Swagger

The Web API uses `apispec` with OpenAPI3 annotations on each route to describe interactions with the server. We can generate a swagger UI and an installable
python module based on these annotations.

Some handy links:

- [OpenAPI specification](https://swagger.io/specification/)
- [Describing parameters](https://swagger.io/docs/specification/describing-parameters/)
- [Describing request body](https://swagger.io/docs/specification/describing-request-body/)
- [Media types](https://swagger.io/docs/specification/media-types/)

The web API exposes this schema in two ways:

- Swagger UI: `http://localhost:8000/docs`
    - You can use this to construct requests to the server
    - Make sure you fill in the Bearer token (at the top right )
- OpenAPI schema: `http://localhost:8000/schema.json`
    - Returns a JSON with the full OpenAPI 3 compliant schema.
    - You could put this into the [Swagger editor](https://editor.swagger.io/) to see the same "Swagger UI" that `/api/docs` exposes.
    - We generate the sample_metadata installable Python API based on this schema.

#### Generating the installable API

The installable API is automatically generated through the `condarise.yml` GitHub action and uploaded to the [CPG conda organisation](https://anaconda.org/cpg).

You could generate the installable API and install it with pip by running:

```bash
# this will start the api.server, so make sure you have the dependencies installed,
python regenerate_api.py \
    && pip install .
```

Or you can build the docker file, and specify that

```bash
# SM_DOCKER is a known env variable to regenerate_api.py
export SM_DOCKER="cpg/sample-metadata-server:dev"
docker build -t $SM_DOCKER -f deploy/api/Dockerfile .
python regenerate_apy.py
```


## Adding a new project

Run the `scripts/create_project.py` script on a machine.


## Deployment

You'll want to complete the following steps:

- Ensure there is a database created for each project (with the database name being the project),
- Ensure there are secrets in `projects/sample_metadata/secrets/databases/versions/latest`, that's an array of objects with keys `dbname, host, port, username, password`.
- Ensure `google-cloud` was installed

```bash
export SM_ENVIRONMENT='PRODUCTION'
```
