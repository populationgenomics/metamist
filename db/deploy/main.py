import contextlib
import json
import os
import subprocess
import tempfile
from typing import Dict, Literal

from fastapi import FastAPI, HTTPException, Query, Request
from google.cloud import logging, secretmanager

app = FastAPI()

# Setup for Google Cloud clients and logging
SECRET_CLIENT = secretmanager.SecretManagerServiceClient()
LOGGING_CLIENT = logging.Client()
SECRET_PROJECT = 'sample-metadata'
SECRET_NAME = 'liquibase-schema-updater'
log_name = 'lb_schema_update_log'
logger = LOGGING_CLIENT.logger(log_name)

# Important to maintain this filename otherwise Liquibase fails to recognise previous migrations
changelog_file = 'project.xml'


def read_db_credentials(env: Literal['prod', 'dev']) -> Dict[Literal['dbname', 'username', 'password', 'host'], str]:
    """Get database credentials from Secret Manager."""
    try:
        secret_path = SECRET_CLIENT.secret_version_path(SECRET_PROJECT, SECRET_NAME, 'latest')
        response = SECRET_CLIENT.access_secret_version(request={'name': secret_path})
        return json.loads(response.payload.data.decode('UTF-8'))[env]
    except Exception as e:  # Broad exception for example; refine as needed
        text = f'Failed to retrieve or parse secrets: {e}'
        logger.log_text(text, severity='ERROR')
        raise HTTPException(status_code=500, detail=text) from e


@app.post('/execute-liquibase')
async def execute_liquibase(request: Request, environment: Literal['prod', 'dev'] = Query(default='dev', regex='^(prod|dev)$')):
    """Endpoint to remotely trigger Liquibase commands on a GCP VM using XML content."""
    xml_content = await request.body()

    # Clean up the local temporary file
    credentials = read_db_credentials(env=environment)
    db_username = credentials['username']
    db_password = credentials['password']
    db_hostname = credentials['host']
    db_name = credentials['dbname']

    # Temporary file creation with XML content
    with tempfile.TemporaryDirectory() as tempdir:
        # Specify the file path within the temporary directory
        with contextlib.chdir(tempdir):  # pylint: disable=E1101
            with open(changelog_file, 'wb') as temp_file:
                temp_file.write(xml_content)
                temp_file_path = temp_file.name  # Store file path to use later
                remote_file_path = os.path.basename(temp_file_path)

            # The actual command to run on the VM
            liquibase_command = [
                '/opt/liquibase/liquibase',
                f'--changeLogFile={remote_file_path}',
                f'--url=jdbc:mariadb://{db_hostname}/{db_name}',
                f'--driver=org.mariadb.jdbc.Driver',
                f'--classpath=/opt/mariadb-java-client-3.0.3.jar',
                'update',
            ]

            try:
                # Execute the gcloud command
                result = subprocess.run(liquibase_command, check=True, capture_output=True, text=True, env={'LIQUIBASE_COMMAND_PASSWORD': db_password, 'LIQUIBASE_COMMAND_USERNAME': db_username, **os.environ},)
                logger.log_text(f'Liquibase update successful: {result.stdout}', severity='INFO')
                os.remove(temp_file_path)
                return {'message': 'Liquibase update executed successfully', 'output': result.stdout}
            except subprocess.CalledProcessError as e:
                text = f'Failed to execute Liquibase update: {e.stderr}'
                logger.log_text(text, severity='ERROR')
                raise HTTPException(status_code=500, detail=text) from e


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
