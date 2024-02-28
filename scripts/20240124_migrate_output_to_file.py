import asyncio
import json
import re
from textwrap import dedent
from typing import Dict

import click
from cloudpathlib import AnyPath
from databases import Database
from google.cloud.storage import Client

from db.python.connect import CredentialedDatabaseConfiguration  # pylint: disable=C0415
from models.models.output_file import OutputFileInternal


def _get_connection_string():
    """Get connection string"""

    config = CredentialedDatabaseConfiguration.dev_config()

    # config = CredentialedDatabaseConfiguration(dbname='sm_dev', username='root')

    return config.get_connection_string()


async def get_analyses_without_fileid(connection):
    """Get analyses without fileid"""
    query = dedent(
        """
        SELECT a.id, a.output
        FROM analysis a
        LEFT JOIN analysis_outputs ao ON ao.analysis_id = a.id
        LEFT JOIN output_file f ON f.id = ao.file_id
        WHERE f.id IS NULL AND ao.output IS NULL
        """
    )
    print('Fetching...')
    rows = await connection.fetch_all(query=query)
    print(f'Found {len(rows)} analyses without file_id and output fields set.')

    return rows


async def execute(connection, query, inserts):
    """Executes inserts"""
    await connection.execute(query, inserts)


async def get_file_info(path: str, client: Client) -> Dict:
    """Get file dict"""
    print('Extracting file dict')
    file_obj = AnyPath(path)

    file_info = await OutputFileInternal.get_file_info(file_obj=file_obj, client=client)

    if not file_info:
        return None

    return {
        'path': path,
        'basename': file_info['basename'],
        'dirname': file_info['dirname'],
        'nameroot': file_info['nameroot'],
        'nameext': file_info['nameext'],
        'file_checksum': file_info['checksum'],
        'valid': file_info['valid'],
        'size': file_info['size'],
    }


def extract_file_paths(input_str):
    """Extract file paths from JSON-like string as well as plain strings into a dict"""
    # Check if the input string matches the JSON-like pattern
    json_pattern = r'^\{.*\}$'
    if re.match(json_pattern, input_str):
        try:
            # Attempt to parse the modified JSON string
            pattern = r"GSPath\('([^']+)'\)"

            matches = re.findall(pattern, input_str)
            file_paths = dict(zip(matches[::2], matches[1::2]))
            return file_paths
        except json.JSONDecodeError:
            print('JSON Error')

    # Treat input as a plain file path
    return {'plain_file_path': input_str}


async def prepare_files(analyses):
    """Serialize files for insertion"""
    files = []
    client = Client()
    print(f'Preparing files...{len(analyses)} analyses to process.')
    for analysis in analyses:
        path = analysis['output']
        if path is None:
            print('Path is None')
            continue

        path_dict = extract_file_paths(path)
        print(path_dict)
        if path_dict:
            print('Found path dict')
            for _, path in path_dict.items():
                print(path)
                files.append((
                    analysis['id'],
                    await get_file_info(path=path, client=client)
                ))
                print('Extracted and added.')
    return files


async def insert_files(connection, files):
    """Insert files"""
    query = dedent(
        """INSERT INTO file (path, basename, dirname, nameroot, nameext, file_checksum, size, valid)
        VALUES (:path, :basename, :dirname, :nameroot, :nameext, :file_checksum, :size, :valid)
        RETURNING id"""
    )
    af_query = dedent(
        """
        INSERT INTO analysis_outputs (analysis_id, file_id, output, json_structure) VALUES (:analysis_id, :file_id, :output, :json_structure)
        """
    )
    for analysis_id, file in files:
        print('Inserting...')
        file_id = await connection.fetch_val(
            query,
            file,
        )
        if not file_id:
            join_inserts = {'analysis_id': analysis_id, 'file_id': None, 'output': file.get('path'), 'json_structure': None}
        else:
            join_inserts = {'analysis_id': analysis_id, 'file_id': file_id, 'output': None, 'json_structure': None}
        await execute(
            connection=connection,
            query=af_query,
            inserts=join_inserts,
        )
    print(f'Inserted {len(files)} files')


@click.command()
# @click.option('--dry-run/--no-dry-run', default=True)
@click.option('--connection-string', default=None)
# @click.argument('author', default='sequencing-group-migration')
def main_sync(connection_string: str = None):
    """Run synchronisation"""
    asyncio.get_event_loop().run_until_complete(
        main(connection_string=connection_string)
    )


async def main(connection_string: str = None):
    """Run synchronisation"""
    connection = Database(connection_string or _get_connection_string(), echo=True)
    await connection.connect()
    async with connection.transaction():
        analyses = await get_analyses_without_fileid(connection=connection)
        files = await prepare_files(analyses)
        await insert_files(connection=connection, files=files)
    await connection.disconnect()


if __name__ == '__main__':
    main_sync()  # pylint: disable=no-value-for-parameter
