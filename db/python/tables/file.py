from db.python.connect import DbBase
from models.models.file import File


class FileTable(DbBase):
    """
    Capture File table operations and queries
    """

    table_name = 'file'

    async def get_file_by_id(self, file_id: int) -> File:
        pass

    async def get_files_by_ids(self, file_ids: list[int]) -> list[File]:
        pass

    async def get_file_by_path(self, path: str) -> File:
        pass

    async def get_files_by_paths(self, paths: list[str]) -> list[File]:
        pass

    # region CREATE

    async def create_files(self, files: list[File]):
        """
        Can insert and get by paths, will need to consider what
        should happen if files exist, eg should it:

        - Archive all old analysis / sequences at this location
            + notify user of this
        - Fail, and make this a precondition

        """
        pass

    # endregion CREATE
