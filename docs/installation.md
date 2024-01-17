# Installation

This document provides detailed instructions on how to install the project. Follow these steps to set up the project on your local system.

## Prerequisites

[Homebrew](https://brew.sh) is the simplest way to install system dependencies needed for this project.

[Chocolatey](https://community.chocolatey.org/) is a good equivalent to Homebrew for package management in Windows.

## System Requirements


- **Node/NPM** (recommend using nvm)
- **MariaDB** (using MariaDB in docker is also good)
- **Java** (for Liquibase / OpenAPI  Generator)
- **Liquibase**
- **OpenAPI Generator**
- **pyenv**
- **wget** *(optional)*

### Mac

```bash
brew install nvm
brew install java
brew install liquibase
brew install pyenv
brew install wget

# skip if you wish to install via docker
brew install mariadb@10.8

```

### Windows

```bash
# Assuming you have Chocolatey
choco install nvm
choco install jdk8
choco install liquibase
choco install pyenv-win
choco install wget

# skip if you wish to install via docker
choco install mariadb --version=10.8.3
```

```bash
# Install npm
nvm install --lts

# Once you have npm set up, install the OpenAPI generator:
npm install @openapitools/openapi-generator-cli -g
openapi-generator-cli version-manager set 5.3.0
```

## Installation Steps

### Creating the environment

- Python dependencies for the `metamist` API package are listed in `setup.py`.
- Additional dev requirements are listed in `requirements-dev.txt`.
- Packages for the sever-side code are listed in `requirements.txt`.

We *STRONGLY* encourage the use of `pyenv` for managing Python versions. Debugging and the server will run on a minimum python version of 3.10.

Use of a virtual environment to contain all requirements is highly recommended:

```bash
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Installs metamist as a package
pip install --editable .
```

You will also need to set the following environment variables. Adjust the paths if you installed the dependencies using an alternate means:

```bash
# homebrew should export this on an M1 Mac
# the intel default is /usr/local
export HB_PREFIX=${HOMEBREW_PREFIX-/usr/local}

# installing Java through brew recommendation
export CPPFLAGS="-I$HB_PREFIX/opt/openjdk/include"
export PATH="$HB_PREFIX/bin:$PATH:$HB_PREFIX/opt/openjdk/bin"

# installing liquibase through brew recommendation
export LIQUIBASE_HOME=$(brew --prefix)/opt/liquibase/libexec


# node
export NVM_DIR="$HOME/.nvm"
[ -s "$HB_PREFIX/opt/nvm/nvm.sh" ] && \. "$HB_PREFIX/opt/nvm/nvm.sh"  # This loads nvm
[ -s "$HB_PREFIX/opt/nvm/etc/bash_completion.d/nvm" ] && \. "$HB_PREFIX/opt/nvm/etc/bash_completion.d/nvm"  # This loads nvm bash_completion

# openapi
export OPENAPI_COMMAND="npx @openapitools/openapi-generator-cli"
alias openapi-generator="npx @openapitools/openapi-generator-cli"

# mariadb
export PATH="$HB_PREFIX/opt/mariadb@10.8/bin:$PATH"


# metamist config
export SM_ENVIRONMENT=LOCAL # good default to have
export SM_DEV_DB_USER=sm_api # makes it easier to copy liquibase update command
```

You can also add these to your shell config file e.g `.zshrc` or `.bashrc` for persistence to new sessions.

### Database Setup - Native Installation

Set the following environment variables:

```bash
export SM_DEV_DB_USER=sm_api
export SM_DEV_DB_PASSWORD= # empty password
export SM_DEV_DB_HOST=127.0.0.1
export SM_DEV_DB_PORT=3306 # default mariadb port
export SM_DEV_DB_NAME=sm_dev;
```

Next, create the database `sm_dev` in MariaDB.

> In newer versions of MariaDB, the root user is protected.

Create a new user `sm_api` and provide permissions:

```bash
sudo mysql -u root --execute "
  CREATE DATABASE sm_dev;
  CREATE USER sm_api@'%';
  CREATE USER sm_api@localhost;
  CREATE ROLE sm_api_role;
  GRANT sm_api_role TO sm_api@'%';
  GRANT sm_api_role TO sm_api@localhost;
  SET DEFAULT ROLE sm_api_role FOR sm_api@'%';
  SET DEFAULT ROLE sm_api_role FOR sm_api@localhost;
  GRANT ALL PRIVILEGES ON sm_dev.* TO sm_api_role;
"
```

Using `liquibase` we can now set up the tables as per the schema in `db/project.xml`:

```bash
pushd db/
wget https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/3.0.3/mariadb-java-client-3.0.3.jar
liquibase \
    --changeLogFile project.xml \
    --url jdbc:mariadb://localhost/sm_dev \
    --driver org.mariadb.jdbc.Driver \
    --classpath mariadb-java-client-3.0.3.jar \
    --username ${SM_DEV_DB_USER:-root} \
    update
popd
```

### Database Setup - Docker Installation

Ensure you have Docker installed or follow [this guide](https://docs.docker.com/engine/install/) to setup.

Pull the image:

```bash
docker pull mariadb:10.8.3
```

Run the container on port 3306:

```bash
docker run --name mariadb-p3306 -e MYSQL_ALLOW_EMPTY_PASSWORD=1 -p 3306:3306 -d docker.io/library/mariadb:10.8.3
```

If you have a local MySQL instance already running on port 3306, you can map the docker container to run on 3307:

```bash
docker run --name mariadb-p3307 -e MYSQL_ALLOW_EMPTY_PASSWORD=1 -p 3307:3306 -d docker.io/library/mariadb:10.8.3
```

You can now execute bash commands inside a shell:

```bash
docker exec -it mariadb-p3306 bash
```

Set up the database with the `sm_api` user and appropriate permissions:

```bash
mysql -u root --execute "
  CREATE DATABASE sm_dev;
  CREATE USER sm_api@'%';
  CREATE USER sm_api@localhost;
  CREATE ROLE sm_api_role;
  GRANT sm_api_role TO sm_api@'%';
  GRANT sm_api_role TO sm_api@localhost;
  SET DEFAULT ROLE sm_api_role FOR sm_api@'%';
  SET DEFAULT ROLE sm_api_role FOR sm_api@localhost;
  GRANT ALL PRIVILEGES ON sm_dev.* TO sm_api_role;
"
```

Exit the container bash shell once done and on the host, run liquibase with the correct port mapping to set up the tables:

```bash
pushd db/
wget https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/3.0.3/mariadb-java-client-3.0.3.jar
liquibase \
    --changeLogFile project.xml \
    --url jdbc:mariadb://127.0.0.1:3306/sm_dev \
    --driver org.mariadb.jdbc.Driver \
    --classpath mariadb-java-client-3.0.3.jar \
    --username root \
    update
popd
```

Ensure the database port environment variable matches the mapping above:

```bash
export SM_DEV_DB_PORT=3306 # or 3307
```

## Running the server

You'll want to set the following environment variables (permanently) in your local development environment.

The `SM_ENVIRONMENT`, `SM_LOCALONLY_DEFAULTUSER` and `SM_ALLOWALLACCESS` environment variables allow access to a local metamist server without providing a bearer token.

This will allow you to test the front-end components that access data. This happens automatically on the production instance through the Google identity-aware-proxy.

```bash
# ensures the SWAGGER page points to your local: (localhost:8000/docs)
# and ensures if you use the PythonAPI, it also points to your local
export SM_ENVIRONMENT=LOCAL
# skips permission checks in your local environment
export SM_ALLOWALLACCESS=true
# uses your username as the "author" in requests
export SM_LOCALONLY_DEFAULTUSER=$(whoami)
```

With those variables set, it is a good time to populate some test data if this is your first time running this server:

```bash
python3 test/data/generate_data.py
```

You can now run the server:

```bash
# start the server
python3 -m api.server
# OR
# uvicorn --port 8000 --host 0.0.0.0 api.server:app
```

### Running and Debugging in VS Code

The following `launch.json` is a good base to debug the web server in VS Code:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Run API",
            "type": "python",
            "request": "launch",
            "module": "api.server",
            "justMyCode": false,
            "env": {
                "SM_ALLOWALLACCESS": "true",
                "SM_LOCALONLY_DEFAULTUSER": "<user>-local",
                "SM_ENVIRONMENT": "local",
                "SM_DEV_DB_USER": "sm_api",
            }
        }
    ]
}
```

You can now place breakpoints anywhere and debug the API with "Run API" under the *Run and Debug* tab (⌘⇧D) or (Ctrl+Shift+D):

![Run and Debug](../resources/debug-api.png)


## Contributing

See the [Contributing](contributing.md) page for instructions on how to make changes and regenerate the API.
