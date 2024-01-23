import asyncio
from textwrap import dedent
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
        """SELECT id, output FROM analysis WHERE output_file_id IS NULL"""
    )
    print('Fetching...')
    rows = await connection.fetch_all(query=query)
    print(f'Found {len(rows)} analyses without fileid')

    return rows


async def execute_many(connection, query, inserts):
    """Executes many inserts"""
    print(f'Inserting {len(inserts)} with query: {query}')

    await connection.execute_many(query, inserts)


async def prepare_files(analyses):
    """Serialize files for insertion"""
    files = []
    for analysis in analyses:
        output = analysis['output']
        if output is None:
            continue

        # TODO support JSON output that has multiple file outputs instead of single file outputs

        file_to_insert = FileInternal.from_path(output)
        files.append(
            {
                'path': file_to_insert.path,
                'basename': file_to_insert.basename,
                'dirname': file_to_insert.dirname,
                'nameroot': file_to_insert.nameroot,
                'nameext': file_to_insert.nameext,
                'checksum': file_to_insert.checksum,
                'size': file_to_insert.size,
            }
        )
    return files


async def insert_files(connection, files):
    """Insert files"""
    query = dedent(
        """INSERT INTO file (path, basename, dirname, nameroot, nameext, checksum, size, secondary_files)
        VALUES (:path, :basename, :dirname, :nameroot, :nameext, :checksum, :size, :secondary_files)
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
@click.option('--dry-run/--no-dry-run', default=True)
@click.option('--connection-string', default=None)
@click.argument('author', default='sequencing-group-migration')
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
        print(await prepare_files(analyses))
    await connection.disconnect()


if __name__ == '__main__':
    main_sync()  # pylint: disable=no-value-for-parameter
