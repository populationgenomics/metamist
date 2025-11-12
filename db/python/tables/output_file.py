# pylint: disable=too-many-nested-blocks
import logging
from textwrap import dedent

from fastapi.concurrency import run_in_threadpool
from google.cloud.storage import Blob

from db.python.tables.base import DbBase
from models.models.output_file import OutputFileInternal, RecursiveDict

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class OutputFileTable(DbBase):
    """
    Capture Analysis table operations and queries
    """

    table_name = 'output_file'

    async def process_output_for_analysis(
        self,
        analysis_id: int,
        output: str | None,
        outputs: str | RecursiveDict | None,
        blobs: list[Blob] | None = None,
    ):
        """
        Process output for analysis
        """
        if output and outputs:
            logger.warning(
                'output and outputs both provided, using outputs instead..',
                stacklevel=2,
            )
        if output and not outputs:
            logger.warning(
                'The output field is going to be deprecated soon, please use outputs instead',
                stacklevel=2,
            )

        if outputs and isinstance(outputs, str):
            logger.warning(
                'The outputs field should be a dictionary, passing a str will be deprecated soon.',
                stacklevel=2,
            )

        output_data = outputs or output

        if output_data:
            await self.create_or_update_analysis_output_files_from_output(
                analysis_id=analysis_id,
                json_dict=output_data,
                blobs=blobs,
            )

    async def create_or_update_output_file(
        self,
        path: str,
        parent_id: int | None = None,
        blobs: list[Blob] | None = None,
    ) -> int | None:
        """
        Create a new file, and add it to database
        """
        # file_obj = AnyPath(path, client=GSClient(storage_client=client))

        if not path:
            raise ValueError('Invalid cloud file path')

        file_obj = await run_in_threadpool(
            OutputFileInternal.get_file_info,
            path=path,
            blobs=blobs,
        )

        if not file_obj or not file_obj.valid:
            return None

        kv_pairs = [
            ('path', path),
            ('basename', file_obj.basename),
            ('dirname', file_obj.dirname),
            ('nameroot', file_obj.nameroot),
            ('nameext', file_obj.nameext),
            ('file_checksum', file_obj.file_checksum),
            ('size', file_obj.size),
            ('valid', file_obj.valid),
            ('parent_id', parent_id),
        ]

        kv_pairs = [(k, v) for k, v in kv_pairs if v is not None]
        keys = [k for k, _ in kv_pairs]
        cs_keys = ', '.join(keys)
        cs_id_keys = ', '.join(f':{k}' for k in keys)
        non_pk_keys = [k for k in keys if k != 'path']
        update_clause = ', '.join([f'{k} = VALUES({k})' for k in non_pk_keys])

        _query = dedent(
            f"""
            INSERT INTO output_file ({cs_keys}) VALUES ({cs_id_keys})
            ON DUPLICATE KEY UPDATE {update_clause} RETURNING id
            """
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

        # The IGNORE is to avoid duplicate entries if the same file is added multiple times
        # and we used this over ON DUPLICATE because there are reported deadlocks with that
        # syntax in high concurrency situations?
        _query = dedent(
            """
            INSERT IGNORE INTO analysis_outputs
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
        blobs: list[Blob] | None = None,
    ) -> None:
        """
        Create analysis files from JSON
        """
        files = await self.find_files_from_dict(json_dict=json_dict)  # type: ignore [arg-type]
        file_ids: list[int] = []
        outputs: list[str] = []

        async with self.connection.transaction():
            if 'main_files' in files:
                for primary_file in files['main_files']:
                    parent_file_id = await self.create_or_update_output_file(
                        path=primary_file['basename'],
                        blobs=blobs,
                    )
                    await self.add_output_file_to_analysis(
                        analysis_id,
                        parent_file_id,
                        json_structure=primary_file['json_path'],
                        # If the file couldnt be created, we just pass the basename as the output
                        output=(None if parent_file_id else primary_file['basename']),
                    )
                    secondary_files = files.get('secondary_files_grouped')
                    if secondary_files:
                        if primary_file['basename'] in secondary_files:
                            for secondary_file in secondary_files[
                                primary_file['basename']
                            ]:
                                secondary_file_id = (
                                    await self.create_or_update_output_file(
                                        path=secondary_file['basename'],
                                        parent_id=parent_file_id,
                                        blobs=blobs,
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
                                if secondary_file_id:
                                    file_ids.append(secondary_file_id)
                                else:
                                    outputs.append(secondary_file['basename'])
                    if parent_file_id:
                        file_ids.append(parent_file_id)
                    else:
                        outputs.append(primary_file['basename'])

            # check that only the files in this json_dict should be in the analysis. Remove what isn't in this dict.
            if not file_ids and not outputs:
                # If both file_ids and outputs are empty, don't execute the query
                pass

            _update_query = dedent(
                # Delete analysis outputs not in the current set of file_ids or outputs
                """
                DELETE ao FROM analysis_outputs ao
                WHERE ao.analysis_id = :analysis_id
                """
            )

            query_params: dict[str, int | list[int] | list[str]] = {
                'analysis_id': analysis_id
            }

            # Add the OR condition to include file_ids or outputs
            conditions = []

            # Add file_id condition if file_ids is not empty
            if file_ids:
                conditions.append(
                    'ao.file_id IS NOT NULL AND ao.file_id NOT IN :file_ids'
                )
                query_params['file_ids'] = file_ids  # Add file_ids to query parameters

            # Add output condition if outputs is not empty
            if outputs:
                conditions.append('ao.output IS NOT NULL AND ao.output NOT IN :outputs')
                query_params['outputs'] = outputs  # Add outputs to query parameters

            # Join the conditions with OR since either can be valid
            if conditions:
                _update_query += ' AND (' + ' OR '.join(conditions) + ')'

            # Execute the query only if either file_ids or outputs were provided
            await self.connection.execute(_update_query, query_params)

    async def find_files_from_dict(
        self,
        json_dict: dict,
        json_path: list[str] | None = None,
        collected: dict | None = None,
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
