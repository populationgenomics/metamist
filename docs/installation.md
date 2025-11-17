# Installation

This document will walk you through the steps required for setting up a local installation of Metamist.

**Note** this is a Metamist Developer guide, not a Metamist user guide. These steps are only required if you need to test a script on a local version of Metamist or work on contributions to the Metamist code base. If you just need to access and work with Metamist data see [the instructions here](/README.md#usage)


## Clone the codebase

```bash
# Clone the repo
git clone git@github.com:populationgenomics/metamist.git

# enter the metamist repo directory
cd metamist
```

## API setup

### Install python requirements

Makesure [uv](https://docs.astral.sh/uv/getting-started/installation/) is locally installed. Since Metamist currently is deployed with python 3.11, you should also use 3.11 locally to ensure that any changes you make are compatible with the deployed version of Metamist.


```bash
# creates virtual env, install python version (3.11) and install dependencies
uv python install 3.11

uv sync --frozen --group dev

# activate virtualenv
source .venv/bin/activate
```

### Database setup

#### Running MariaDB

Metamist uses a MariaDB 11 database. Docker is the easiest way to run the Metamist MariaDB database locally.

We have found that [OrbStack](https://orbstack.dev/) is faster and easier to use than [Docker Desktop](https://docs.docker.com/desktop/) but either should work fine.

If you would prefer to not use Docker, you could also install MariaDB standalone using [homebrew](brew.sh) or another package manager.

To start the database, run:

```bash
docker run --name mariadb-p3306 -e MYSQL_ALLOW_EMPTY_PASSWORD=1 -p 3306:3306 -d docker.io/library/mariadb:11.7.2
```

If port 3306 is already in use, you can specify a different port in the mapping. For example:

```bash
docker run --name mariadb-p3307 -e MYSQL_ALLOW_EMPTY_PASSWORD=1 -p 3307:3306 -d docker.io/library/mariadb:11.7.2
```


#### Setting up the database and permissions

1. Connect to the running container:

```bash
docker exec -it mariadb-p3306 bash
```

2. In the container shell, run the MariaDB CLI to create the database, user, and roles:

```bash
mariadb -u root --execute "
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

Once that is done you can disconnect from the container.


#### Running database migrations

Metamist uses liquibase to manage database migrations. To create the Metamist database tables you will need to run the migrations.

Start by installing liquibase:

```bash
brew install liquibase
```

Then in the `db` folder, run the migrations:

```bash
cd db

liquibase \
    --changeLogFile project.xml \
    --url jdbc:mariadb://127.0.0.1:3306/sm_dev \
    --driver org.mariadb.jdbc.Driver \
    --username root \
    update
```

This should create all the necessary tables.


### Setting environment variables

To run the API you'll need to set some environment variables. You can either add these to your bash/zsh profile, or if you use vscode you can set up a `.vscode/launch.json` file to make it easy to run and debug the API in vscode. Make sure to choose a username for the `SM_LOCALONLY_DEFAULTUSER` variable, this is the username that will be used for all local operations, it can take any format that you like.


in `.bashrc` or `.zshrc`

```bash
export SM_LOCALONLY_DEFAULTUSER="<localusername>"
export SM_URL="http://localhost:8000"
export SM_ENVIRONMENT="local"

export SM_DEV_DB_USER="sm_api"
export SM_DEV_DB_PORT="3306"
```


If you do go down the route of setting the variables in your project's vscode `launch.json`, it is still a good idea to set the first 3 environment variables above in your bash/zsh profile. These variables are used by some of the scripts below, for example to generate the Metamist API. If they aren't set then your generated API will point to the production Metamist.

in `.vscode/launch.json`

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Run API",
            "type": "debugpy",
            "request": "launch",
            "module": "api.server",
            "justMyCode": false,
            "env": {
                "SM_URL": "http://localhost:8000",
                "SM_LOCALONLY_DEFAULTUSER": "<localusername>",
                "SM_ENVIRONMENT": "local",
                "SM_DEV_DB_USER": "sm_api",
                "SM_DEV_DB_PORT": "3306"

            }
        }
    ]
}
```



### Giving yourself project creator permissions

To bootstrap the database with some data, your local user will need permissions. To provide these you will need to connect to the database again.


Start a shell in the container:

```bash
docker exec -it mariadb-p3306 bash
```

Enter the mariadb command prompt

```bash
mariadb
```

Switch your database to sm_dev

```sql
USE sm_dev;
```

Add your local username to the `project-creators` and `members-admin` groups:

```sql
INSERT INTO group_member(group_id, member)
SELECT id, '<localusername>'
FROM `group` WHERE name IN('project-creators', 'members-admin');

```

You can now exit the mariadb prompt and the container.


### Building and installing the local python client

Next up, we need to run the API generator to create the python client, which will then be used to load some test data into the database.

This is handled by the `regenerate_api.py` script.

This script requires [openapi-generator](https://openapi-generator.tech/docs/installation/) to be installed, this is included as part of Metamist's python dev requirements, so make sure that you have your virtual env activated before running:

```bash
uv run python regenerate_api.py
```

If you have installed openapi generator using a different method you can set the `OPENAPI_COMMAND` environment variable to configure the command to use.


Once the API is generated, you can install it by running:

```bash
uv pip install -e .
```


### Starting the API

Now that everything is set up, you can start the api server. If you are using vscode you can do this with the debugger, by first setting up the `launch.json` as described [above](#setting-environment-variables), and then running the API with "Run API" under the *Run and Debug* tab (⌘⇧D) or (Ctrl+Shift+D). F5 is the default shortcut to run the current launch config.

![Run and Debug](../resources/debug-api.png)

If you are not using vscode, you can run the API with uvicorn.

```bash
uv run uvicorn --port 8000 --host 0.0.0.0 api.server:app
```


### Generating some data

To add some data to your database, you can run the `test/data/generate_data.py` script. Make sure you have your virtualenv activated and the `SM_ENVIRONMENT` and `SM_LOCALONLY_DEFAULTUSER` environment variables set before running this, otherwise the Metamist python client will try to add the data to production Metamist.


```bash
uv run python3 test/data/generate_data.py
```


### Conclusion

At this point, your API should be fully functional. You can test scripts by setting the `SM_ENVIRONMENT` variable to `local` so that the Metamist python client points to your local installation. You can also work on developing backend features.

- The GraphiQL explorer is accessible at: [http://localhost:8000/graphql](http://localhost:8000/graphql)
- The swagger http api documentation is accessible at: [http://localhost:8000/docs](http://localhost:8000/docs)

To set up the Metamist web client, read on.



## Web Client Setup

The Metamist web client is a React single page application that calls the Metamist apis and displays metadata in a user interface.

To get up and running you will need [nodejs](https://nodejs.org/en) installed, there are a few options for managing node versions but we recommend [fnm](https://github.com/Schniz/fnm) as it is lightweight, simple and provides a similar api to `nvm` while being much much faster.

Other installation options are outlined on the [nodejs download page](https://nodejs.org/en/download).

The Metamist client should work with a variety of node versions but for the purposes of this setup we'll install 22 which is the LTS version at the time of writing.


```bash
fnm use 22
```

Then we can install the web client npm dependencies

```bash
cd web
npm install
npm run compile # generate the graphql api integration
npm start
```


The web client should now be running at [http://localhost:5173](http://localhost:5173)




## Deployment

The CPG deploy is managed through Cloud Run on the Google Cloud Platform.
The deploy github action builds the container, and is deployed.

Additionally you can access Metamist through the identity-aware proxy (IAP),
which handles the authentication through OAuth, allowing you to access the
front-end.


## Performance Profiling

If you are working on performance issues it can be handy to generate a report that shows which bits of code are taking most of the time. The api server has pyinstrument profiling support that can be turned on by setting  the environment variable `SM_PROFILE_REQUESTS` to `true`.

There are a few different options for outputting profiles which can be specified in the `SM_PROFILE_REQUESTS_OUTPUT` environment variable. The possible values are `text` which will print the profiling results to stdout, `html` which will generate an interactive pyinstrument report, or `json` which will generate a json profiling report which can be dropped into [speedscope](https://www.speedscope.app/) to explore the profile.

You can output multiple report types by specifying the types in a list like: `export SM_PROFILE_REQUESTS_OUTPUT=json,text,html`
