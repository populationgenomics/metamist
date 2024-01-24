import asyncio
import json
import re
from textwrap import dedent
from typing import Dict

import click
from databases import Database

from models.models.file import FileInternal


def _get_connection_string():
    from db.python.connect import CredentialedDatabaseConfiguration

    config = CredentialedDatabaseConfiguration.dev_config()

    # config = CredentialedDatabaseConfiguration(dbname='sm_dev', username='root')

    return config.get_connection_string()


async def get_analyses_without_fileid(connection):
    """Get analyses without fileid"""
    query = dedent(
        """
        SELECT a.id, a.output
        FROM analysis a
        LEFT JOIN file f ON f.analysis_id = a.id
        WHERE f.analysis_id IS NULL
        """
    )
    print('Fetching...')
    rows = await connection.fetch_all(query=query)
    print(f'Found {len(rows)} analyses without fileid')

    return rows


async def execute_many(connection, query, inserts):
    """Executes many inserts"""
    print(f'Inserting {len(inserts)} with query: {query}')

    await connection.execute_many(query, inserts)


def get_file_dict(path: str, analysis_id: int) -> Dict:
    """Get file dict"""
    print('Extracting file dict')
    return {
        'analysis_id': analysis_id,
        'path': path,
        'basename': FileInternal.get_basename(path),
        'dirname': FileInternal.get_dirname(path),
        'nameroot': FileInternal.get_nameroot(path),
        'nameext': FileInternal.get_extension(path),
        'checksum': FileInternal.get_checksum(path),
        'size': FileInternal.get_size(path),
        'secondary_files': '[]',
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
            pass  # Continue to treat as a plain file path

    # Treat input as a plain file path
    return {'plain_file_path': input_str}


async def prepare_files(analyses):
    """Serialize files for insertion"""
    files = []
    for analysis in analyses:
        path = analysis['output']
        if path is None:
            continue

        path_dict = extract_file_paths(path)
        if path_dict:
            for _, path in path_dict.items():
                print(path)
                files.append(
                    get_file_dict(path=path, analysis_id=analysis['id'])
                )
                print('Extracted and added.')
    return files


async def insert_files(connection, files):
    """Insert files"""
    query = dedent(
        """INSERT INTO file (path, analysis_id, basename, dirname, nameroot, nameext, checksum, size, secondary_files)
        VALUES (:path, :analysis_id, :basename, :dirname, :nameroot, :nameext, :checksum, :size, :secondary_files)
        RETURNING id"""
    )
    print('Inserting...')
    await execute_many(
        connection=connection,
        query=query,
        inserts=files,
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
