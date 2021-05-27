# Sample Metadata

An API to manage sample + other related metadata.

## First time setup

Add the mariadb jar into the db folder:

```bash
pushd db/
wget https://cdn.mysql.com//Downloads/Connector-J/mysql-connector-java-8.0.24.zip

# ensure you have the database created
mysql -u root -e 'CREATE DATABASE my_dev';

# now you can run liquibase
liquibase update
```

## Deployment

You'll want to complete the following steps:

- Ensure there is a database created for each project (with the database name being the project),
- Ensure there are secrets in `projects/sample_metadata/secrets/databases/versions/latest`, that's an array of objects with keys `dbname, host, port, username, password`.
- Ensure `google-cloud` was installed

```bash
export SM_ENVIRONMENT='PRODUCTION'
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

- Swagger UI: `http://localhost:5000/api/docs`
    - You can use this to construct requests to the server
    - Make sure you fill in the Bearer token (at the top right )
- OpenAPI schema: `http://localhost:5000/api/schema.json`
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

To add a new project, you must:

- Connect to mysql
- Create database: `CREATE DATABASE {project}_sm;`
- Apply schema: _TBD_
- Create user: `CREATE USER '{project}'@'%' IDENTIFIED BY {generated-password};`
- Give permissions: ``GRANT ALL PRIVILEGES ON `{project}`.* TO 'tob_wgs'@'%';``
- Flush privileges: `FLUSH PRIVILEGES;`
- Update the secrets at `projects/sample_metadata/secrets/databases/versions/latest` to include an extra entry.
