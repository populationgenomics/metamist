from db.python.connect import DbBase
from models.models.file import File

from db.python.connect import NotFoundError


class FileTable(DbBase):
    """
    Capture File table operations and queries
    """

    table_name = 'file'

    async def get_file_by_id(self, file_id: int) -> File:
        _query = f"""
            SELECT *
            FROM {self.table_name}
            WHERE id = :file_id
        """
        values = {'file_id': file_id}
        record = await self.connection.fetch_one(_query, values)
        if not record:
            raise NotFoundError(f"File with id '{file_id}' not found")
        return File.from_db(**dict(record))

    async def get_files_by_ids(self, file_ids: list[int]) -> list[File]:
        _query = f"""
            SELECT *
            FROM {self.table_name}
            WHERE id IN :file_ids
        """

        values = {'file_ids': file_ids}
        records = {
            record["id"]: record
            for record in await self.connection.fetch_all(_query, values)
        }

        files: list[File] = []
        for file_id in file_ids:
            if file_id not in records:
                raise NotFoundError(f"File with id '{file_id}' not found")
            files.append(File.from_db(**dict(records[file_id])))

        return files

    async def get_file_by_path(self, path: str) -> File:
        _query = f"""
            SELECT *
            FROM {self.table_name}
            WHERE path = :path
        """
        values = {'path': path}
        record = await self.connection.fetch_one(_query, values)
        if not record:
            raise NotFoundError(f"File with path '{path}' not found")
        return File.from_db(**dict(record))

    async def get_files_by_paths(self, paths: list[str]) -> list[File]:
        _query = f"""
            SELECT *
            FROM {self.table_name}
            WHERE id IN :paths
        """

        values = {'paths': paths}
        records = {
            record["path"]: record
            for record in await self.connection.fetch_all(_query, values)
        }

        files: list[File] = []
        for path in paths:
            if path not in records:
                raise NotFoundError(f"File with path '{path}' not found")
            files.append(File.from_db(**dict(records[path])))

        return files

    # region CREATE

    async def create_files(self, files: list[File]):
        """
        Can insert and get by paths, will need to consider what
        should happen if files exist, eg should it:

        - Archive all old analysis / sequences at this location
            + notify user of this
        - Fail, and make this a precondition

        """
        _query = f"""
            SELECT *
            FROM {self.table_name}
            WHERE path IN :paths
        """
        values = {'paths': [file.path for file in files]}

        existing_files: dict[str, File] = {}
        for record in await self.connection.fetch_all(_query, values):
            file = File.from_db(**dict(record))
            existing_files[file.path] = file

        for file in files:
            if (
                file.path in existing_files
                and (file.checksum and existing_files[file.path].checksum)
                and file.checksum != existing_files[file.path].checksum
            ):
                raise ValueError(
                    f"File with path '{file.path}' already exists with different checksum"
                )

        values = [
            f"('{file.type.value}', '{file.path}', {file.size}, '{file.checksum}', {file.exists})"
            for file in files
        ]

        query = f"""
        INSERT INTO {self.table_name} (type, path, size, checksum, exists)
        VALUES :values
        """

        return await self.connection.execute(query, {'values': ', '.join(values)})

    # endregion CREATE
