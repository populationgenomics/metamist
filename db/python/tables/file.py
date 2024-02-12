from textwrap import dedent
from typing import Optional, Union

from db.python.tables.base import DbBase
from models.models.file import FileInternal


class FileTable(DbBase):
    """
    Capture Analysis table operations and queries
    """

    table_name = 'file'

    async def create_analysis_output_file(
        self,
        path: str,
        analysis_id: int,
        json_structure: Optional[str]
    ) -> int:
        """Create a new file, and add it to an analysis via the join table"""

        id_of_new_file = await self.create_or_update_output_file(path=path)
        await self.add_output_file_to_analysis(analysis_id=analysis_id, file_id=id_of_new_file, json_structure=json_structure)

        return id_of_new_file

    async def create_or_update_output_file(
        self,
        path: str,
        parent_id: Optional[int] = None,
    ) -> int | None:
        """
        Create a new file, and add it to database
        """

        file_obj = await FileInternal.validate_path(path)
        if not file_obj:
            return None

        kv_pairs = [
            ('path', path),
            ('basename', FileInternal.get_basename(path)),
            ('dirname', FileInternal.get_dirname(path)),
            ('nameroot', FileInternal.get_nameroot(path)),
            ('nameext', FileInternal.get_extension(path)),
            ('file_checksum', await FileInternal.get_checksum(path)),
            ('size', FileInternal.get_size(path)),
            ('valid', file_obj),
            ('parent_id', parent_id)
        ]

        kv_pairs = [(k, v) for k, v in kv_pairs if v is not None]
        keys = [k for k, _ in kv_pairs]
        cs_keys = ', '.join(keys)
        cs_id_keys = ', '.join(f':{k}' for k in keys)
        non_pk_keys = [k for k in keys if k != 'path']
        update_clause = ', '.join([f'{k} = VALUES({k})' for k in non_pk_keys])  # ON DUPLICATE KEY UPDATE {update_clause}

        async with self.connection.transaction():
            _query = dedent(f"""INSERT INTO file ({cs_keys}) VALUES ({cs_id_keys}) ON DUPLICATE KEY UPDATE {update_clause} RETURNING id""")
            id_of_new_file = await self.connection.fetch_val(
                _query,
                dict(kv_pairs),
            )

        # _query = dedent(f"""INSERT INTO file ({cs_keys}) VALUES ({cs_id_keys}) RETURNING id""")

        # id_of_new_file = await self.connection.fetch_val(
        #     _query,
        #     dict(kv_pairs),
        # )

        return id_of_new_file

    async def add_output_file_to_analysis(self, analysis_id: int, file_id: int, json_structure: Optional[str] = None, output: Optional[str] = None):
        """Add file to an analysis (through the join table)"""
        _query = dedent("""
            INSERT INTO analysis_outputs
                (analysis_id, file_id, json_structure, output)
            VALUES (:analysis_id, :file_id, :json_structure, :output)
        """)
        await self.connection.execute(
                _query,
                {'analysis_id': analysis_id, 'file_id': file_id, 'json_structure': json_structure, 'output': output}
            )

    async def create_or_update_analysis_output_files_from_json(
        self,
        analysis_id: int,
        json_dict: Union[dict, str],
    ) -> None:
        """
        Create analysis files from JSON
        """
        files = await self.find_files_from_dict(json_dict=json_dict)
        file_ids : list[int] = []

        async with self.connection.transaction():
            if 'main_files' in files:
                for primary_file in files['main_files']:
                    parent_file_id = await self.create_or_update_output_file(path=primary_file['basename'])
                    await self.add_output_file_to_analysis(
                        analysis_id,
                        parent_file_id,
                        json_structure=primary_file['json_path'],
                        output=None if parent_file_id else primary_file['basename']
                        )
                    if 'secondary_files_grouped' in files:
                        secondary_files = files['secondary_files_grouped']
                        if primary_file['basename'] in secondary_files:
                            for secondary_file in secondary_files[primary_file['basename']]:
                                await self.create_or_update_output_file(path=secondary_file, parent_id=parent_file_id)
                        file_ids.append(parent_file_id)

        # check that only the files in this json_dict should be in the analysis. Remove what isn't in this dict.
        _update_query = dedent("""
            DELETE ao FROM analysis_outputs ao
            WHERE (ao.analysis_id = :analysis_id)
            AND (ao.file_id NOT IN :file_ids)
            """)

        await self.connection.execute(
            _update_query,
            {
                'analysis_id': analysis_id,
                'file_ids': file_ids
            }
        )

    async def find_files_from_dict(self, json_dict, json_path=None, collected=None) -> dict:
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
                collected['main_files'].append({'json_path': '.'.join(json_path), 'basename': json_dict['basename']})
                current_basename = json_dict['basename']  # Keep track of current basename for secondary files

                # Handle secondary files if present
                if 'secondaryFiles' in json_dict:
                    secondary = json_dict['secondaryFiles']
                    if current_basename not in collected['secondary_files_grouped']:
                        collected['secondary_files_grouped'][current_basename] = []
                    for _, value in secondary.items():
                        # Append each secondary file to the list in secondary_files under its parent basename
                        collected['secondary_files_grouped'][current_basename].append(value['basename'])

            else:
                for key, value in json_dict.items():
                    # Recur for each sub-dictionary, updating the path
                    await self.find_files_from_dict(value, json_path + [key], collected)

        elif isinstance(json_dict, list):
            # Recur for each item in the list, without updating the path (as lists don't contribute to JSON path)
            for item in json_dict:
                await self.find_files_from_dict(item, json_path, collected)

        return collected
