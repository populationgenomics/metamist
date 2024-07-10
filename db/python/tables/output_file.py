import os
import warnings
from textwrap import dedent

from cloudpathlib import AnyPath, GSClient
from google.auth.credentials import AnonymousCredentials
from google.cloud.storage import Client

from db.python.tables.base import DbBase
from models.models.output_file import OutputFileInternal, RecursiveDict


class OutputFileTable(DbBase):
    """
    Capture Analysis table operations and queries
    """

    table_name = 'output_file'

    async def process_output_for_analysis(
        self, analysis_id: int, output: str | None, outputs: str | RecursiveDict | None
    ):
        """
        Process output for analysis
        """
        if output and outputs:
            warnings.warn(
                'The output field is deprecated, using outputs instead since it was passed in..',
                PendingDeprecationWarning,
                2,
            )
        if output and not outputs:
            warnings.warn(
                'The output field is deprecated, please use outputs instead',
                PendingDeprecationWarning,
                2,
            )

        if outputs and isinstance(outputs, str):
            warnings.warn(
                'The outputs field should be a dictionary, passing a str will be deprecated soon.',
                PendingDeprecationWarning,
                2,
            )

        output_data = outputs or output

        if output_data:
            await self.create_or_update_analysis_output_files_from_output(
                analysis_id=analysis_id, json_dict=output_data
            )

    async def create_or_update_output_file(
        self,
        path: str,
        parent_id: int | None = None,
        client: Client | None = None,
    ) -> int | None:
        """
        Create a new file, and add it to database
        """
        file_obj = AnyPath(path, client=GSClient(storage_client=client))

        if not file_obj or not client:
            raise ValueError('Invalid file path or client')

        file_info = await OutputFileInternal.get_file_info(
            file_obj=file_obj, client=client
        )

        if not file_info or not file_info.get('valid'):
            return None

        kv_pairs = [
            ('path', path),
            ('basename', file_info['basename']),
            ('dirname', file_info['dirname']),
            ('nameroot', file_info['nameroot']),
            ('nameext', file_info['nameext']),
            ('file_checksum', file_info['checksum']),
            ('size', file_info['size']),
            ('valid', file_info['valid']),
            ('parent_id', parent_id),
        ]

        kv_pairs = [(k, v) for k, v in kv_pairs if v is not None]
        keys = [k for k, _ in kv_pairs]
        cs_keys = ', '.join(keys)
        cs_id_keys = ', '.join(f':{k}' for k in keys)
        non_pk_keys = [k for k in keys if k != 'path']
        update_clause = ', '.join(
            [f'{k} = VALUES({k})' for k in non_pk_keys]
        )  # ON DUPLICATE KEY UPDATE {update_clause}

        async with self.connection.transaction():
            _query = dedent(
                f"""INSERT INTO output_file ({cs_keys}) VALUES ({cs_id_keys}) ON DUPLICATE KEY UPDATE {update_clause} RETURNING id"""
            )
            id_of_new_file = await self.connection.fetch_val(
                _query,
                dict(kv_pairs),
            )

        return id_of_new_file

    async def add_output_file_to_analysis(
        self,
        analysis_id: int,
        file_id: int | None,
        json_structure: str | None = None,
        output: str | None = None,
    ):
        """Add file to an analysis (through the join table)"""
        _query = dedent(
            """
            INSERT INTO analysis_outputs
                (analysis_id, file_id, json_structure, output)
            VALUES (:analysis_id, :file_id, :json_structure, :output)
        """
        )
        await self.connection.execute(
            _query,
            {
                'analysis_id': analysis_id,
                'file_id': file_id,
                'json_structure': json_structure,
                'output': output,
            },
        )

    async def create_or_update_analysis_output_files_from_output(
        self,
        analysis_id: int,
        json_dict: RecursiveDict | str,
    ) -> None:
        """
        Create analysis files from JSON
        """
        files = await self.find_files_from_dict(json_dict=json_dict)
        file_ids: list[int] = []

        if os.environ.get('SM_ENVIRONMENT', 'development').lower() in (
            'development',
            'local',
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
            client = Client()

        async with self.connection.transaction():
            if 'main_files' in files:
                for primary_file in files['main_files']:
                    parent_file_id = await self.create_or_update_output_file(
                        path=primary_file['basename'], client=client
                    )
                    await self.add_output_file_to_analysis(
                        analysis_id,
                        parent_file_id,
                        json_structure=primary_file['json_path'],
                        # If the file couldnt be created, we just pass the basename as the output
                        output=None if parent_file_id else primary_file['basename'],
                    )
                    if files.get('secondary_files_grouped'):
                        secondary_files = files['secondary_files_grouped']
                        if primary_file['basename'] in secondary_files:
                            for secondary_file in secondary_files[
                                primary_file['basename']
                            ]:
                                secondary_file_id = (
                                    await self.create_or_update_output_file(
                                        path=secondary_file['basename'],
                                        parent_id=parent_file_id,
                                        client=client,
                                    )
                                )
                                await self.add_output_file_to_analysis(
                                    analysis_id,
                                    secondary_file_id,
                                    json_structure=secondary_file['json_path'],
                                    # If the file couldnt be created, we just pass the basename as the output
                                    output=(
                                        None
                                        if secondary_file_id
                                        else secondary_file['basename']
                                    ),
                                )
                                file_ids.append(secondary_file_id)
                    file_ids.append(parent_file_id)

        client.close()
        # check that only the files in this json_dict should be in the analysis. Remove what isn't in this dict.
        _update_query = dedent(
            """
            DELETE ao FROM analysis_outputs ao
            WHERE (ao.analysis_id = :analysis_id)
            AND (ao.file_id NOT IN :file_ids)
            """
        )

        if file_ids:
            await self.connection.execute(
                _update_query, {'analysis_id': analysis_id, 'file_ids': file_ids}
            )

    async def find_files_from_dict(
        self, json_dict, json_path=None, collected=None
    ) -> dict:
        """Retrieve filepaths from a dict of outputs"""
        if collected is None:
            collected = {'main_files': [], 'secondary_files_grouped': {}}

        if json_path is None:
            json_path = []  # Initialize path for tracking key path

        if isinstance(json_dict, str):
            # If the data is a plain string, return it as the basename with None as its keypath
            collected['main_files'].append({'json_path': None, 'basename': json_dict})
            return collected

        if isinstance(json_dict, dict):
            # Check if current dict contains 'basename'
            if 'basename' in json_dict:
                # Add current item to main_files
                collected['main_files'].append(
                    {
                        'json_path': '.'.join(json_path),
                        'basename': json_dict['basename'],
                    }
                )
                current_basename = json_dict[
                    'basename'
                ]  # Keep track of current basename for secondary files

                # Handle secondary files if present
                if 'secondary_files' in json_dict:
                    secondary = json_dict['secondary_files']
                    if current_basename not in collected['secondary_files_grouped']:
                        collected['secondary_files_grouped'][current_basename] = []
                    for key, value in secondary.items():
                        # Append each secondary file to the list in secondary_files under its parent basename
                        collected['secondary_files_grouped'][current_basename].append(
                            {
                                'json_path': '.'.join(
                                    json_path + ['secondary_files', key]
                                ),
                                'basename': value['basename'],
                            }
                        )

            else:
                for key, value in json_dict.items():
                    # Recur for each sub-dictionary, updating the path
                    await self.find_files_from_dict(value, json_path + [key], collected)

        elif isinstance(json_dict, list):
            # Recur for each item in the list, without updating the path (as lists don't contribute to JSON path)
            for item in json_dict:
                await self.find_files_from_dict(item, json_path, collected)

        return collected
