#!/usr/bin/env python3
# pylint: disable=logging-not-lazy,logging-fstring-interpolation
"""
Creates a project, by:
- creating a database in mariadb
- creating a user w/ generated password
- update credentials in secret
"""
import logging
import json
import random
import string

import pymysql
from google.cloud import secretmanager


MARIADB_HOST = "localhost"
MARIADB_USER = "root"
MARIADB_PASSWORD = None
MARIADB_PORT = 3306

EXTERNAL_HOST = "sm-db-vm-instance.australia-southeast1-b.c.sample-metadata.internal"

logging.basicConfig(level=logging.DEBUG)


def create_database(project: str):
    """Driver function to create new database in sample-metadata"""
    if not project.isalpha():
        raise ValueError(
            "The project name must only consist of alphabetical characters"
        )

    project_name = project.lower()

    databasename = project_name
    username = f"sm_{project_name}"
    password = _generate_password()

    connection = _get_connection()
    cursor: pymysql.cursors.Cursor = connection.cursor()

    _create_mariadb_database(cursor, databasename)
    _create_user(cursor, databasename, username, password)
    _grant_privileges_to_database(cursor, databasename, username)

    secret_manager = secretmanager.SecretManagerServiceClient()

    _update_databases_secret(
        project=project_name,
        databasename=databasename,
        username=username,
        password=password,
        secret_manager=secret_manager,
    )


def _update_databases_secret(secret_manager, project, databasename, username, password):
    # read secret first
    initial_secret = _read_sm_secret(secret_manager, "databases")
    credentials_list = json.loads(initial_secret)
    credentials_list.append(
        {
            "project": project,
            "dbname": databasename,
            "host": EXTERNAL_HOST,
            "username": username,
            "password": password,
        }
    )
    logging.info("Updating secret ")

    _write_sm_secret(secret_manager, "databases", json.dumps(credentials_list))


def _read_sm_secret(secret_manager, name: str) -> str:
    """Reads the latest version of the given secret from Google's Secret Manager."""
    # pylint: disable=import-outside-toplevel,no-name-in-module

    secret_name = f"projects/sample-metadata/secrets/{name}/versions/latest"
    response = secret_manager.access_secret_version(request={"name": secret_name})
    return response.payload.data.decode("UTF-8")


def _write_sm_secret(secret_manager, name: str, value: str):

    parent = secret_manager.secret_path("sample-metadata", name)
    secret_manager.add_secret_version(
        request={"parent": parent, "payload": {"data": value.encode("UTF-8")}}
    )


def _create_mariadb_database(cursor, name):
    logging.debug(f'Creating database "{name}"')
    _query = f"CREATE DATABASE IF NOT EXISTS {name};"

    try:
        cursor.execute(_query)
    except Exception as exp:
        logging.critical("Couldn't create database: " + repr(exp))
        raise


def _generate_password(length=14):
    """Generate random string of letters"""
    # I don't like '@' in the password, it always seems to cause problems
    password_characters = string.ascii_letters + string.digits + "-!#&$"
    return "".join(random.choice(password_characters) for _ in range(length))


def _get_connection() -> pymysql.Connection:
    sqlcon = pymysql.connect(
        host=MARIADB_HOST,
        user=MARIADB_USER,
        password=MARIADB_PASSWORD,
        port=MARIADB_PORT,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )

    return sqlcon


def _create_user(cursor: pymysql.cursors.Cursor, dbname, username, password) -> bool:

    # find if user exists
    _existing_query = (
        'SELECT COUNT(user) as count FROM mysql.user WHERE user = %s AND host = "%%"'
    )
    cursor.execute(_existing_query, (username,))
    row = cursor.fetchone()
    if row["count"] > 0:
        logging.critical(
            f'The user "{username}" already exists in the database, will remove and re-add'
        )
        _drop_user_query = "DROP USER %s@'%%'"
        cursor.execute(_drop_user_query, (username,))

    logging.info(f"Creating user '{dbname}'@'%' with password")

    _create_user_query = "CREATE USER %s@'%%' IDENTIFIED BY %s;"

    try:
        cursor.execute(_create_user_query, (username, password))
    except Exception as exp:
        logging.critical("Couldn't create user: " + repr(exp))
        raise

    return True


def _grant_privileges_to_database(
    cursor, databasename, username, flush_privileges=True
):

    logging.debug(f"Granting {username} privileges on DB {databasename}")
    _query = f"GRANT CREATE, DROP, DELETE, INSERT, SELECT, UPDATE, ALTER, INDEX ON {databasename}.* TO %s@'%%';"

    try:
        cursor.execute(_query, (username,))
        if flush_privileges:
            cursor.execute("FLUSH privileges;")
    except Exception as exp:
        logging.critical(
            f"Couldn't grant privileges for '{username}' to '{databasename}': "
            + repr(exp)
        )
        raise


def from_args(args=None):
    """Run create_database(*)"""

    # pylint: disable=import-outside-toplevel
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("project", help="Project name")

    pargs = parser.parse_args(args)
    create_database(pargs.project)


if __name__ == "__main__":
    from_args(["seqr"])
