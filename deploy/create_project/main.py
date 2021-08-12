#!/usr/bin/env python3
# pylint: disable=logging-not-lazy,logging-fstring-interpolation
"""
Creates a project, by:
- creating a database in mariadb
- creating a user w/ generated password
- update credentials in secret

Requires:
- pymysql
- google-cloud-secret-manager==2.2.0
    - Ensure you have the latest version of pip, as it installs significantly quicker
"""
import os
import logging
import json
import random
import string
import subprocess
import urllib.request
import time
import traceback

from fastapi import FastAPI, Request, HTTPException, Body
from fastapi.responses import JSONResponse
import pymysql
from google.cloud import secretmanager


CREDENTIALS = os.getenv('SM_CREDS')
if CREDENTIALS is None:
    raise ValueError('SM credentials were not provided')
creds = json.loads(CREDENTIALS)
MARIADB_HOST = creds['host']
MARIADB_USER = creds['username']
MARIADB_PASSWORD = creds['password']
MARIADB_PORT = int(creds.get('port', 3306))

EXTERNAL_HOST = 'sm-db-vm-instance.australia-southeast1-b.c.sample-metadata.internal'

logging.basicConfig(level=logging.DEBUG)


app = FastAPI()


@app.middleware('http')
async def add_process_time_header(request: Request, call_next):
    """Add X-Process-Time to all requests for logging"""
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers['X-Process-Time'] = f'{round(process_time * 1000, 1)}ms'
    return response


@app.exception_handler(Exception)
async def exception_handler(_: Request, e: Exception):
    """Generic exception handler"""
    base_params = {}
    add_stacktrace = True

    if add_stacktrace:
        st = traceback.format_exc()
        base_params['stacktrace'] = st

    if isinstance(e, HTTPException):
        code = e.status_code
        name = e.detail

    else:
        code = 500
        name = str(type(e).__name__)

    return JSONResponse(
        status_code=code,
        content={**base_params, 'name': name, 'description': str(e)},
    )


@app.post('/project')
async def create_project_request(project_name: str = Body(..., embed=True)):
    """POST request for creating a project"""
    create_database(project_name, update_secret=True)
    return {'success': True}


def create_database(
    project: str,
    username: str = None,
    password: str = None,
    update_secret: bool = False,
):
    """Driver function to create new database in sample-metadata"""
    if not project.isidentifier():
        raise ValueError(
            'The project name must only consist of alphabetical characters'
        )

    project_name = project.lower()

    databasename = f'sm_{project_name}'
    _username = username or f'sm_{project_name}'
    _password = password or _generate_password()

    connection = _get_connection()
    cursor: pymysql.cursors.Cursor = connection.cursor()

    _create_mariadb_database(cursor, databasename)
    _create_user(
        cursor=cursor,
        dbname=databasename,
        username=_username,
        password=_password,
        force_recreate_user=True,
    )
    _grant_privileges_to_database(cursor, databasename, _username)

    _apply_schema(databasename)

    if update_secret:
        secret_manager = secretmanager.SecretManagerServiceClient()

        _update_databases_secret(
            project=project_name,
            databasename=databasename,
            username=_username,
            password=_password,
            secret_manager=secret_manager,
        )


def _update_databases_secret(secret_manager, project, databasename, username, password):
    # read secret first
    initial_secret = _read_sm_secret(secret_manager, 'databases')
    credentials_list = json.loads(initial_secret)
    credentials_list.append(
        {
            'project': project,
            'dbname': databasename,
            'host': EXTERNAL_HOST,
            'username': username,
            'password': password,
        }
    )
    logging.info('Updating secret with new credentials')

    _write_sm_secret(secret_manager, 'databases', json.dumps(credentials_list))


def _read_sm_secret(secret_manager, name: str) -> str:
    """Reads the latest version of the given secret from Google's Secret Manager."""
    # pylint: disable=import-outside-toplevel,no-name-in-module

    secret_name = f'projects/sample-metadata/secrets/{name}/versions/latest'
    response = secret_manager.access_secret_version(request={'name': secret_name})
    return response.payload.data.decode('UTF-8')


def _write_sm_secret(secret_manager, name: str, value: str):

    parent = secret_manager.secret_path('sample-metadata', name)
    secret_manager.add_secret_version(
        request={'parent': parent, 'payload': {'data': value.encode('UTF-8')}}
    )


def _create_mariadb_database(cursor, name):
    logging.debug(f'Creating database "{name}"')
    _query = f'CREATE DATABASE IF NOT EXISTS {name};'

    try:
        cursor.execute(_query)
    except Exception as exp:
        logging.critical("Couldn't create database: " + repr(exp))
        raise


def _generate_password(length=-1):
    """Generate random string of letters"""
    # Don't like special characters in the password, it always seems to cause problems
    # so just make it longer
    if length <= 0:
        length = random.randint(24, 30)
    password_characters = string.ascii_letters + string.digits + '-_'
    return ''.join(random.choice(password_characters) for _ in range(length))


def _get_connection() -> pymysql.Connection:
    sqlcon = pymysql.connect(
        host=MARIADB_HOST,
        user=MARIADB_USER,
        password=MARIADB_PASSWORD,
        port=MARIADB_PORT,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
    )

    return sqlcon


def _create_user(
    cursor: pymysql.cursors.Cursor,
    dbname,
    username,
    password,
    force_recreate_user=False,
) -> bool:

    # find if user exists
    _existing_query = (
        'SELECT COUNT(user) as count FROM mysql.user WHERE user = %s AND host = "%%"'
    )
    cursor.execute(_existing_query, (username,))
    row = cursor.fetchone()
    if row['count'] > 0:
        if not force_recreate_user:
            logging.warning(
                'The user "{username}" already exists, skipping creation of user'
            )
            return False

        logging.critical(
            f'The user "{username}" already exists in the database, will remove and re-add'
        )
        _drop_user_query = 'DROP USER %s@"%%"'
        cursor.execute(_drop_user_query, (username,))

    logging.info(f"Creating user '{dbname}'@'%' with password")

    _create_user_query = 'CREATE USER %s@"%%" IDENTIFIED BY %s;'

    try:
        cursor.execute(_create_user_query, (username, password))
    except Exception as exp:
        logging.critical("Couldn't create user: " + repr(exp))
        raise

    return True


def _grant_privileges_to_database(
    cursor, databasename, username, flush_privileges=True
):

    logging.debug(f'Granting {username} privileges on DB {databasename}')
    _query = f'GRANT CREATE, DROP, DELETE, INSERT, SELECT, UPDATE, ALTER, INDEX ON {databasename}.* TO %s@"%%";'

    try:
        cursor.execute(_query, (username,))
        if flush_privileges:
            cursor.execute('FLUSH privileges;')
    except Exception as exp:
        logging.critical(
            f"Couldn't grant privileges for '{username}' to '{databasename}': "
            + repr(exp)
        )
        raise


def _apply_schema(databasename):

    changelog_file = 'https://raw.githubusercontent.com/populationgenomics/sample-metadata/add-foundations/db/project.xml'
    mariadb_java_client_url = 'https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/2.7.2/mariadb-java-client-2.7.2.jar'
    mariadb_java_client_file = 'mariadb-java-client-2.7.2.jar'

    if not os.path.exists(mariadb_java_client_file):
        with urllib.request.urlopen(mariadb_java_client_url) as f, open(
            mariadb_java_client_file, 'w+b'
        ) as javafile:
            javafile.write(f.read())

    changelog_filename = 'project.xml'

    with open(changelog_filename, 'w+b') as tmp_changelog:
        with urllib.request.urlopen(changelog_file) as f:
            tmp_changelog.write(f.read())

    command = [
        'liquibase',
        '--headless=true',
        # '--logLevel=debug',
        *('--changeLogFile', changelog_filename),
        *('--url', f'jdbc:mariadb://{MARIADB_HOST}:{MARIADB_PORT}/{databasename}'),
        *('--driver', 'org.mariadb.jdbc.Driver'),
        *('--classpath', 'mariadb-java-client-2.7.2.jar'),
        'update',
    ]

    if MARIADB_USER != 'root':
        command.extend(['--username', MARIADB_USER])
    if MARIADB_PASSWORD:
        command.extend(['--password', MARIADB_PASSWORD])

    jcommand = ' '.join(command)
    print(f'Running "{jcommand.replace(MARIADB_PASSWORD, "****")}"')
    try:
        output = subprocess.check_output(jcommand, shell=True)
        print(output.decode())
    except subprocess.CalledProcessError as exp:
        if exp.stderr:
            print(exp.stderr.decode())
        if exp.stdout:
            print(exp.stdout.decode())

    # os.remove(changelog_filename)

    return True


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host='0.0.0.0', port=int(os.getenv('PORT', '8000')), debug=True)
