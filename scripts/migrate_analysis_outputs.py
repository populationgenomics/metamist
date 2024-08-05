import os
from collections import defaultdict
from textwrap import dedent
from typing import Any

from databases import Database
from google.auth.credentials import AnonymousCredentials
from google.cloud.storage import Client

from db.python.connect import Connection, SMConnections
from db.python.tables.output_file import OutputFileTable
from models.models import OutputFileInternal


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
        # connection_string = SMConnections._get_config()

        # sm_db = SMConnections.make_connection(
        #     config=connection_string,
        # )
        sm_db = await SMConnections.get_made_connection()
        # await sm_db.connect()
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
        if os.environ.get('SM_ENVIRONMENT', 'local').lower() in (
            # 'local',
            'test',
        ):
            client = Client(
                credentials=AnonymousCredentials(),
                project='test',
                # Alternatively instead of using the global env STORAGE_EMULATOR_HOST. You can define it here.
                # This will set this client object to point to the local google cloud storage.
                client_options={'api_endpoint': 'http://localhost:4443'},
            )
        else:
            # if project:
            #     client = Client(project=project)
            # else:
            client = Client()

        analyses = await get_analyses_without_fileid(sm_db)
        print(f'Found {len(analyses)} analysis outputs to be migrated')

        analyses_group_by_bucket: dict[
            str, dict[str, list[dict[str, Any]]]
        ] = defaultdict(lambda: defaultdict(list))

        analyses_local = {}
        bucket_params: dict[str, Any] = {}
        for analysis in analyses:
            if analysis['output'] is None:
                continue

            file_path = analysis['output']

            # If the output is not a GSPath, it's a local file, process it right away.
            if not file_path.startswith('gs://'):
                analyses_local[analysis['id']] = file_path
            else:
                params = {
                    'bucket': file_path.split('/')[2],
                    'prefix': '/'.join(file_path.split('/')[3:-1]) + '/',
                    'delimiter': '/',
                }
                if params['bucket'] not in analyses_group_by_bucket:
                    analyses_group_by_bucket[params['bucket']] = {}
                    if (
                        params['prefix']
                        not in analyses_group_by_bucket[params['bucket']]
                    ):
                        analyses_group_by_bucket[params['bucket']][
                            params['prefix']
                        ] = []
                        analyses_group_by_bucket[params['bucket']][
                            params['prefix']
                        ].append(analysis)
                    else:
                        analyses_group_by_bucket[params['bucket']][
                            params['prefix']
                        ].append(analysis)
                elif (
                    params['prefix']
                    not in analyses_group_by_bucket[params['bucket']]
                ):
                    analyses_group_by_bucket[params['bucket']][
                        params['prefix']
                    ] = []
                    analyses_group_by_bucket[params['bucket']][
                        params['prefix']
                    ].append(analysis)
                else:
                    analyses_group_by_bucket[params['bucket']][
                        params['prefix']
                    ].append(analysis)

                if params['bucket'] not in bucket_params:
                    bucket_params[params['bucket']] = {}
                    bucket_params[params['bucket']][params['prefix']] = params
                else:
                    bucket_params[params['bucket']][params['prefix']] = params

        for bucket, prefixes in analyses_group_by_bucket.items():
            for prefix, analyses in prefixes.items():
                params = bucket_params[bucket][prefix]
                blobs = await OutputFileInternal.list_blobs(
                    bucket=bucket,
                    prefix=params['prefix'],
                    delimiter=params['delimiter'],
                    client=client,
                    versions=False,
                )
                print(
                    f'Processing bucket {bucket}/{prefix} with {len(analyses)} analyses'
                )
                for analysis in analyses:
                    await oft.process_output_for_analysis(
                        analysis_id=analysis['id'],
                        output=analysis['output'],
                        outputs=None,
                        blobs=blobs,
                        client=client,
                    )
                    # Print progress
                    print(
                        f'Processed {analyses.index(analysis) + 1} of {len(analyses)} in this bucket'
                    )

        for analysis_id, file_path in analyses_local.items():
            print(f'Processing local file {id}')
            await oft.process_output_for_analysis(
                analysis_id=analysis_id,
                output=file_path,
                outputs=None,
                blobs=None,
            )

    asyncio.run(main())
