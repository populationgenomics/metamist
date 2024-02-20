import json
import os
import subprocess
from google.cloud import logging, secretmanager
from typing_extensions import Literal

# Google Secret Manager client setup
SECRET_CLIENT = secretmanager.SecretManagerServiceClient()
LOGGING_CLIENT = logging.Client()
SECRET_PROJECT = 'sample-metadata'
SECRET_NAME = 'mariadb-liquibase-credentials'


def read_db_credentials() -> dict[Literal['username', 'password'], str]:
    """Get database credentials from Secret Manager."""
    try:
        secret_path = SECRET_CLIENT.secret_version_path(SECRET_PROJECT, SECRET_NAME, 'latest')
        response = SECRET_CLIENT.access_secret_version(request={'name': secret_path})
        return json.loads(response.payload.data.decode('UTF-8'))
    except Exception as e:
        raise Exception(f'Could not access database credentials: {e}') from e


def run_liquibase_update():
    """Run the Liquibase update command."""

    log_name = 'liquibase_log'
    logger = LOGGING_CLIENT.logger(log_name)

    # Fetch the credentials
    credentials = read_db_credentials()
    db_username = credentials['username']
    db_password = credentials['password']
    db_hostname = credentials['hostname']
    db_name = os.getenv('DB_NAME', 'development')

    # Construct the Liquibase command
    liquibase_cmd = [
        "./liquibase/liquibase",
        "--changeLogFile=project.xml",
        f"--url=jdbc:mariadb://{db_hostname}/{db_name}",
        "--driver=org.mariadb.jdbc.Driver",
        "--classpath=mariadb-java-client-3.0.3.jar",
        "update"
    ]

    try:
        # Execute the Liquibase command
        subprocess.run(liquibase_cmd,
                       shell=True,
                       check=True,
                       stderr=subprocess.DEVNULL,
                       env={'LIQUIBASE_COMMAND_PASSWORD': db_password,
                            'LIQUIBASE_COMMAND_USERNAME': db_username,
                            **os.environ},)
    except subprocess.CalledProcessError as e:
        text = f'Liquibase schema update failed: {e}\n {e.stderr}'
        logger.log_text(text, severity='ERROR')
        return


if __name__ == '__main__':
    run_liquibase_update()
