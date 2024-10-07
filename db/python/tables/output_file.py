# pylint: disable=too-many-nested-blocks
from collections import defaultdict
from dataclasses import dataclass
import logging
from textwrap import dedent

from fastapi.concurrency import run_in_threadpool
from google.cloud.storage import Blob

from db.python.tables.base import DbBase
from models.models.output_file import OutputFileInternal, RecursiveDict

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


@dataclass
class Files:
    """Dataclass for storing filepaths during the find_files_from_dict method"""

    main_files: list[dict[str, str]]
    secondary_files_grouped: dict[str, list[dict[str, str]]]

    def to_dict(self) -> dict:
        """Convert Files dataclass to dictionary"""
        return {
            'main_files': self.main_files,
            'secondary_files_grouped': self.secondary_files_grouped,
        }


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

        def save_file_outputs(file_id, file):
            if file_id:
                file_ids.append(file_id)
            else:
                outputs.append(file)

        async with self.connection.transaction():
            for primary_file in files.get('main_files', []):
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

                # Add secondary files if they exist
                secondary_files = files.get('secondary_files_grouped', {}).get(
                    primary_file['basename'], []
                )
                for secondary_file in secondary_files:
                    secondary_file_id = await self.create_or_update_output_file(
                        path=secondary_file['basename'],
                        parent_id=parent_file_id,
                        blobs=blobs,
                    )
                    await self.add_output_file_to_analysis(
                        analysis_id,
                        secondary_file_id,
                        json_structure=secondary_file['json_path'],
                        # If the file couldnt be created, we just pass the basename as the output
                        output=(
                            None if secondary_file_id else secondary_file['basename']
                        ),
                    )
                    save_file_outputs(secondary_file_id, secondary_file['basename'])

                save_file_outputs(parent_file_id, primary_file['basename'])

            _update_query = dedent(
                """
                DELETE ao FROM analysis_outputs ao
                WHERE ao.analysis_id = :analysis_id
                """
            )

            query_params: dict[str, int | list[int] | list[str]] = {
                'analysis_id': analysis_id
            }

            # check that only the files in this json_dict should be in the analysis. Remove wha
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
            _update_query += ' AND (' + ' OR '.join(conditions) + ')'

            if not file_ids and not outputs:
                # Execute the query only if either file_ids or outputs were provided
                await self.connection.execute(_update_query, query_params)

    async def find_files_from_dict(
        self,
        json_dict: dict,
        json_path: list[str] | None = None,
        collected: Files = None,
    ) -> dict:
        """Retrieve filepaths from a dict of outputs"""
        if collected is None:
            collected = Files(main_files=[], secondary_files_grouped=defaultdict(list))

        if json_path is None:
            json_path = []  # Initialize path for tracking key path

        if isinstance(json_dict, str):
            # If the data is a plain string, return it as the basename with None as its keypath
            collected.main_files.append({'json_path': None, 'basename': json_dict})
            return collected.to_dict()

        if isinstance(json_dict, dict) and (basename := json_dict.get('basename')):
            # Add current item to main_files
            collected.main_files.append(
                {
                    'json_path': '.'.join(json_path),
                    'basename': basename,
                }
            )

            # Handle secondary files if present
            secondary = json_dict.get('secondary_files', {})
            for key, value in secondary.items():
                # Append each secondary file to the list in secondary_files under its parent basename
                collected.secondary_files_grouped[basename].append(
                    {
                        'json_path': '.'.join(json_path + ['secondary_files', key]),
                        'basename': value['basename'],
                    }
                )

        elif isinstance(json_dict, dict):
            for key, value in json_dict.items():
                # Recur for each sub-dictionary, updating the path
                await self.find_files_from_dict(value, json_path + [key], collected)

        elif isinstance(json_dict, list):
            # Recur for each item in the list, without updating the path (as lists don't contribute to JSON path)
            for item in json_dict:
                await self.find_files_from_dict(item, json_path, collected)

        return collected.to_dict()
