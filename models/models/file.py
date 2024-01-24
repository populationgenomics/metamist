import hashlib

from cloudpathlib.anypath import AnyPath

from models.base import SMBase


class File(SMBase):
    """File model for internal use"""

    id: int
    analysis_id: int
    path: str
    basename: str
    dirname: AnyPath
    nameroot: str
    nameext: str | None
    checksum: str | None
    size: int
    secondary_files: list[str] = []

    @staticmethod
    def from_db(**kwargs):
        """
        Convert from db keys, mainly converting id to id_
        """
        analysis_id = kwargs.get('analysis_id')
        path = kwargs.get('path')
        basename = kwargs.get('basename')
        dirname = kwargs.get('dirname')
        nameroot = kwargs.get('nameroot')
        nameext = kwargs.get('nameext')
        checksum = kwargs.get('checksum')
        size = kwargs.get('size')
        secondary_files = kwargs.get('secondary_files')

        return File(
            id=kwargs.pop('id'),
            analysis_id=analysis_id,
            path=path,
            basename=basename,
            dirname=dirname,
            nameroot=nameroot,
            nameext=nameext,
            checksum=checksum,
            size=size,
            secondary_files=secondary_files,
        )

    @staticmethod
    def get_basename(path: str) -> str:
        """Get file basename for file at given path"""
        return AnyPath(path).name  # pylint: disable=E1101

    @staticmethod
    def get_dirname(path: str) -> str:
        """Get file dirname for file at given path"""
        return str(AnyPath(path).parent)  # pylint: disable=E1101

    @staticmethod
    def get_nameroot(path: str) -> str:
        """Get file nameroot for file at given path"""
        return AnyPath(path).stem  # pylint: disable=E1101

    @staticmethod
    def get_extension(path: str) -> str:
        """Get file extension for file at given path"""
        return AnyPath(path).suffix  # pylint: disable=E1101

    @staticmethod
    def get_checksum(path: str) -> str:
        """Get checksum for file at given path"""
        try:
            return hashlib.sha256(AnyPath(path).read_bytes()).hexdigest()  # pylint: disable=E1101
        except FileNotFoundError:
            return None

    @staticmethod
    def get_size(path: str) -> int:
        """Get file size"""
        try:
            return AnyPath(path).stat().st_size  # pylint: disable=E1101
        except FileNotFoundError:
            return 0
