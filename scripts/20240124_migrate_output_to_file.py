import asyncio
import json
import re
from textwrap import dedent
from typing import Dict

import click
from databases import Database

from models.models.file import FileInternal


def _get_connection_string():
    from db.python.connect import \
        CredentialedDatabaseConfiguration  # pylint: disable=C0415

    config = CredentialedDatabaseConfiguration.dev_config()

    # config = CredentialedDatabaseConfiguration(dbname='sm_dev', username='root')

    return config.get_connection_string()


async def get_analyses_without_fileid(connection):
    """Get analyses without fileid"""
    query = dedent(
        """
        SELECT a.id, a.output
        FROM analysis a
        LEFT JOIN analysis_file af ON af.analysis_id = a.id
        LEFT JOIN file f ON f.id = af.file_id
        WHERE f.id IS NULL
        """
    )
    print('Fetching...')
    rows = await connection.fetch_all(query=query)
    print(f'Found {len(rows)} analyses without fileid')

    return rows


async def execute(connection, query, inserts):
    """Executes inserts"""
    await connection.execute(query, inserts)


def get_file_info(path: str) -> Dict:
    """Get file dict"""
    print('Extracting file dict')
    return {
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
                files.append((
                    analysis['id'],
                    get_file_info(path=path)
                ))
                print('Extracted and added.')
    return files


async def insert_files(connection, files):
    """Insert files"""
    query = dedent(
        """INSERT INTO file (path, basename, dirname, nameroot, nameext, checksum, size, secondary_files)
        VALUES (:path, :basename, :dirname, :nameroot, :nameext, :checksum, :size, :secondary_files)
        RETURNING id"""
    )
    af_query = dedent(
        """
        INSERT INTO analysis_file (analysis_id, file_id) VALUES (:analysis_id, :file_id)
        """
    )
    for analysis_id, file in files:
        print('Inserting...')
        file_id = await connection.fetch_val(
            query,
            file,
        )
        await execute(
            connection=connection,
            query=af_query,
            inserts={'analysis_id': analysis_id, 'file_id': file_id},
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
