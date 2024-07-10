# TODO Add Analysis Output Migration

from textwrap import dedent

from databases import Database

from db.python.connect import (
    Connection,
    CredentialedDatabaseConfiguration,
    SMConnections,
)
from db.python.tables.output_file import OutputFileTable


def _get_connection_string():
    """Get connection string"""

    config = CredentialedDatabaseConfiguration.dev_config()

    # config = CredentialedDatabaseConfiguration(dbname='sm_dev', username='root')

    # return config.get_connection_string()
    return config


async def get_analyses_without_fileid(connection: Database):
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


if __name__ == '__main__':
    import asyncio

    async def main():
        """Go through all analysis objects and create output file objects where possible"""
        connection_string = _get_connection_string()
        database = Database(connection_string.get_connection_string(), echo=True)

        sm_db = SMConnections.make_connection(
            config=connection_string,
        )
        await sm_db.connect()
        formed_connection = Connection(
            connection=sm_db,
            author='yash',
            project_id_map={},
            project_name_map={},
            on_behalf_of=None,
            ar_guid=None,
            project=None,
        )
        oft = OutputFileTable(formed_connection)
        analyses = await get_analyses_without_fileid(database)
        for analyis in analyses:
            await oft.process_output_for_analysis(
                analyis['id'], analyis['output'], None
            )
        print(analyses)

    asyncio.run(main())
