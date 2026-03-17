
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

Make sure [uv](https://docs.astral.sh/uv/getting-started/installation/) is locally installed.
The following commands will create a virtual env, install python version 3.14 (as specified in `pyproject.toml`) and install dependencies:
```bash
uv venv --seed
uv sync
```

### Database setup

#### Running Postgres
Metamist uses a Postgres database. Docker is the easiest way to run the Metamist Postgres database locally.
We have found that [OrbStack](https://orbstack.dev/) is faster and easier to use than [Docker Desktop](https://docs.docker.com/desktop/) but either should work fine.

Setup the Postgres container with docker:
```bash
cd db
docker compose up -d
```

Then run the database migrations:
```bash
docker compose exec postgres dbmate up
```

At this point, the database will be sufficiently setup to run the unit tests.

### Running the API

#### Setting environment variables
To run the API you'll need to set some environment variables. You can either add these to your bash/zsh profile, or if you use vscode you can set up a `.vscode/launch.json` file to make it easy to run and debug the API in vscode. 
Make sure to choose a username for the `SM_LOCALONLY_DEFAULTUSER` variable, this is the username that will be used for all local operations, it can take any format that you like.

In `.bashrc` or `.zshrc`, add the following lines:
```bash
export SM_LOCALONLY_DEFAULTUSER="<localusername>"
export SM_ENVIRONMENT="local"

export SM_DEV_DB_NAME="metamist_db"
export SM_DEV_DB_USER="metamist"
export SM_DEV_DB_PASSWORD="metamist_password"
export SM_DEV_DB_PORT="5432"
```

> [!NOTE]
> If you are using VSCode to run the API, you should still set the first two variables from the above snippet in your bash/zsh profile.
> These variables are used in scripts we will run later in the setup, and will not be inherited from your `launch.json` file, *so ensure that the values are the same*.

In your `.vscode/launch.json`, create the following configuration:
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
                "SM_LOCALONLY_DEFAULTUSER": "<localusername>",
                "SM_ENVIRONMENT": "local",
                "SM_DEV_DB_NAME": "metamist_db",
                "SM_DEV_DB_USER": "metamist",
                "SM_DEV_DB_PASSWORD": "metamist_password",
                "SM_DEV_DB_PORT": "5432",
            }
        }
    ]
}
```

#### Giving yourself project creator permissions

To bootstrap the database with some data, your local user will need permissions. To provide these you will need to connect to the database.

Start a shell in the container:

```bash
docker exec -it metamist_postgres bash
```

Enter the Postgres command prompt

```bash
psql "postgresql://metamist:metamist_password@localhost:5432/metamist_db?options=--search_path%3Dmain"
```

Add your local username to the `project-creators` and `members-admin` groups:

```sql
INSERT INTO group_member(group_id, member)
SELECT id, '<localusername>'
FROM "group" WHERE name IN ('project-creators', 'members-admin');
```
> [!NOTE]
> Make sure that the `<localusername>` is identical to what you set in the `SM_LOCALONLY_DEFAULTUSER` environment variable.

You can now exit the Postgres prompt and the container.


#### Building and installing the local python client

Next up, we need to run the API generator to create the python client, which will then be used to load some test data into the database.

This is handled by the `regenerate_api.py` script.

This script requires [openapi-generator](https://openapi-generator.tech/docs/installation/) to be installed, this is included as part of Metamist's python dev requirements, so you shouldn't need to install anything additional.

**Firstly**, ensure that your virtual environment is activated. Then run the following:

```bash
uv run regenerate_api.py
```
Run the command `npx @openapitools/openapi-generator-cli version` if you are prompted to do so.

If you have installed openapi generator using a different method you can set the `OPENAPI_COMMAND` environment variable to configure the command to use.

The API will be generated in the directory `packages/metamist/src/metamist` and you can build the API for packaging using the `pyproject.toml` file in the `packages/metamist/` directory.

#### Starting the API

Now that everything is set up, you can start the api server. If you are using vscode you can do this with the debugger, by first setting up the `launch.json` as described [above](#setting-environment-variables), and then running the API with "Run API" under the *Run and Debug* tab (⌘⇧D) or (Ctrl+Shift+D). F5 is the default shortcut to run the current launch config.

![Run and Debug](../resources/debug-api.png)

If you are not using vscode, you can run the API with uvicorn.

```bash
uv run uvicorn --port 8000 --host 0.0.0.0 api.server:app
```


#### Generating some data

To add some data to your database, you can run the `test/data/generate_data.py` script. Make sure you have your virtualenv activated and the `SM_ENVIRONMENT` and `SM_LOCALONLY_DEFAULTUSER` environment variables set before running this, otherwise the Metamist python client will try to add the data to production Metamist.


```bash
uv run test/data/generate_data.py
```


#### Conclusion

At this point, your API should be fully functional. You can test scripts by setting the `SM_ENVIRONMENT` variable to `local` so that the Metamist python client points to your local installation. You can also work on developing backend features.

- The GraphiQL explorer is accessible at: [http://localhost:8000/graphql](http://localhost:8000/graphql)
- The swagger http api documentation is accessible at: [http://localhost:8000/docs](http://localhost:8000/docs)

To set up the Metamist web client, read on.



## Web Client Setup

The Metamist web client is a React single page application that calls the Metamist APIs and displays metadata in a user interface.

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
