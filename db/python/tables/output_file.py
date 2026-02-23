import logging
from urllib.parse import urlparse

from fastapi.concurrency import run_in_threadpool
from google.cloud.storage import Blob
from psycopg import sql

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
        if not path:
            raise ValueError('Invalid cloud file path')

        if urlparse(path).scheme == '':
            raise ValueError('Output file path must contain a protocol prefix')

        file_obj = await run_in_threadpool(
            OutputFileInternal.get_file_info,
            path=path,
            blobs=blobs,
        )

        if not file_obj or not file_obj.valid:
            return None

        create_update_file = t"""
            INSERT INTO output_file
                (path, basename, dirname, nameroot, nameext, file_checksum, size, valid, parent_id)
            VALUES
                ({path},
                {file_obj.basename},
                {file_obj.dirname},
                {file_obj.nameroot},
                {file_obj.nameext},
                {file_obj.file_checksum},
                {file_obj.size},
                {file_obj.valid},
                {parent_id})
            ON CONFLICT (path) DO UPDATE SET
                basename = EXCLUDED.basename,
                dirname = EXCLUDED.dirname,
                nameroot = EXCLUDED.nameroot,
                nameext = EXCLUDED.nameext,
                file_checksum = EXCLUDED.file_checksum,
                size = EXCLUDED.size,
                valid = EXCLUDED.valid,
                parent_id = EXCLUDED.parent_id
            RETURNING id
            """

        cur = await self.connection.pg_connection.execute(create_update_file)
        id_of_new_file = await cur.fetchone()

        return id_of_new_file['id']

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
        add_analysis_output = t"""
            INSERT INTO analysis_outputs
                (analysis_id, file_id, json_structure, output)
            VALUES
                ({analysis_id}, {file_id}, {json_structure}, {output})
            ON CONFLICT DO NOTHING"""

        await self.connection.pg_connection.execute(add_analysis_output)

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
                    if secondary_files:  # noqa: SIM102
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

            # Delete analysis outputs not in the current set of file_ids or outputs
            update_analysis_outputs = t"""
                DELETE FROM analysis_outputs
                WHERE analysis_id = {analysis_id}"""

            # Add the OR condition to include file_ids or outputs
            conditions = []

            # Add file_id condition if file_ids is not empty
            if file_ids:
                # Add file_ids to query parameters
                conditions.append(t'file_id IS NOT NULL AND file_id <> ALL({file_ids})')

            # Add output condition if outputs is not empty
            if outputs:
                # Add outputs to query parameters
                conditions.append(t'output IS NOT NULL AND output <> ALL({outputs})')

            # Join the conditions with OR since either can be valid
            if conditions:
                update_analysis_outputs += (
                    t' AND ({sql.SQL(" OR ").join(conditions):q})'
                )

            # Execute the query only if either file_ids or outputs were provided
            await self.connection.pg_connection.execute(update_analysis_outputs)

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
