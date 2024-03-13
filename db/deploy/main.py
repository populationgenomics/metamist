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


def read_db_credentials(env: Literal['prod', 'dev']) -> Dict[Literal['dbname', 'username', 'password', 'host', 'vm-name', 'vm-zone', 'vm-project'], str]:
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

    # Temporary file creation with XML content
    with tempfile.NamedTemporaryFile(delete=False, suffix='.xml') as temp_file:
        temp_file.write(xml_content)
        temp_file_path = temp_file.name  # Store file path to use later

    # Clean up the local temporary file
    credentials = read_db_credentials(env=environment)
    db_username = credentials['username']
    db_password = credentials['password']
    db_hostname = credentials['host']
    db_name = credentials['dbname']

    # Define VM details
    vm_name = credentials['vm-name']
    zone = credentials['vm-zone']
    project = credentials['vm-project']
    remote_directory = '/tmp'  # Directory on VM where the file should be copied
    remote_file_path = f'{os.path.basename(temp_file_path)}'
    remote_abs_file_path = f'{remote_directory}/{remote_file_path}'

    # Command to copy the file to the VM
    scp_command = [
        'gcloud',
        'compute',
        'scp',
        temp_file_path,
        f'{vm_name}:{remote_abs_file_path}',
        '--zone',
        zone,
        '--project',
        project
    ]

    try:
        # Copy XML file to VM
        subprocess.run(scp_command, check=True, capture_output=True, text=True)
        logger.log_text('Copied XML file successfully.', severity='INFO')
    except subprocess.CalledProcessError as e:
        text = f'Failed to copy XML content: {e.stderr}'
        logger.log_text(text, severity='ERROR')
        raise HTTPException(status_code=500, detail=text) from e

    # The actual command to run on the VM
    liquibase_command = f"""
        echo 'Running Liquibase updates...' &&
        liquibase --search-path={remote_directory} --changeLogFile={remote_file_path} --url=jdbc:mariadb://{db_hostname}/{db_name} --driver=org.mariadb.jdbc.Driver --classpath=/opt/mariadb-java-client-3.0.3.jar update --log-level=FINE --username={db_username} --password={db_password}
        """

    # Command to SSH into the VM and run the Liquibase update script
    ssh_command = [
        'gcloud',
        'compute',
        'ssh',
        vm_name,
        '--zone',
        zone,
        '--project',
        project,
        '--command',
        liquibase_command,
    ]

    try:
        # Execute the gcloud command
        result = subprocess.run(ssh_command, check=True, capture_output=True, text=True)
        logger.log_text(f'Liquibase update successful: {result.stdout}', severity='INFO')
        os.remove(temp_file_path)
        return {'message': 'Liquibase update executed successfully', 'output': result.stdout}
    except subprocess.CalledProcessError as e:
        text = f'Failed to execute Liquibase update via SSH: {e.stderr}'
        logger.log_text(text, severity='ERROR')
        raise HTTPException(status_code=500, detail=text) from e


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
