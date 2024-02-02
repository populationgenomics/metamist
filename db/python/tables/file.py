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
    ) -> int:
        """
        Create a new file, and add it to database
        """
        kv_pairs = [
            ('path', path),
            ('basename', FileInternal.get_basename(path)),
            ('dirname', FileInternal.get_dirname(path)),
            ('nameroot', FileInternal.get_nameroot(path)),
            ('nameext', FileInternal.get_extension(path)),
            ('checksum', FileInternal.get_checksum(path)),
            ('size', FileInternal.get_size(path)),
        ]

        kv_pairs = [(k, v) for k, v in kv_pairs if v is not None]
        keys = [k for k, _ in kv_pairs]
        cs_keys = ', '.join(keys)
        cs_id_keys = ', '.join(f':{k}' for k in keys)
        # non_pk_keys = [k for k in keys if k != 'path']
        # update_clause = ', '.join([f"{k} = VALUES({k})" for k in non_pk_keys])  # ON DUPLICATE KEY UPDATE {update_clause}

        _query = dedent(f"""INSERT INTO file ({cs_keys}) VALUES ({cs_id_keys}) RETURNING id""")

        id_of_new_file = await self.connection.fetch_val(
            _query,
            dict(kv_pairs),
        )

        return id_of_new_file

    async def add_output_file_to_analysis(self, analysis_id: int, file_id: int, json_structure: Optional[str] = None):
        """Add file to an analysis (through the join table)"""
        _query = dedent("""
            INSERT INTO analysis_file
                (analysis_id, file_id, json_structure)
            VALUES (:analysis_id, :file_id, :json_structure)
            ON DUPLICATE KEY UPDATE
            analysis_id = VALUES(analysis_id),
            file_id = VALUES(file_id)
        """)
        await self.connection.execute(
                _query,
                {'analysis_id': analysis_id, 'file_id': file_id, 'json_structure': json_structure}
            )

    async def create_or_update_analysis_output_files_from_json(
        self,
        analysis_id: int,
        json_dict: Union[dict, str],
    ) -> None:
        """
        Create analysis files from JSON
        """
        async with self.connection.transaction():
            for json_path, path in FileInternal.find_files_from_dict(json_dict=json_dict):
                file_id = await self.create_or_update_output_file(path=path)
                await self.add_output_file_to_analysis(analysis_id, file_id, json_structure='.'.join(json_path))
