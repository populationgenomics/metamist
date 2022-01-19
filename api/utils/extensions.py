from enum import Enum
from typing import Optional


class FileExtension(Enum):
    """
    Wraps up common properties and allows for parameterisation
    of some table exports.
    """

    CSV = 'csv'
    TSV = 'tsv'
    PED = 'ped'

    @staticmethod
    def get_extension_map():
        """Get {FileExtension: ".ext"} map"""
        return {
            FileExtension.CSV: '.csv',
            FileExtension.TSV: '.tsv',
            FileExtension: '.ped',
        }

    @staticmethod
    def get_delimiter_map():
        """Get {FileExtension: "delimiter"} map"""
        return {
            FileExtension.CSV: ',',
            FileExtension.TSV: '\t',
            FileExtension.PED: '\t',
        }

    @staticmethod
    def get_mimetype_map():
        """Get {FileExtension: "text/mimetype"} map"""
        return {
            FileExtension.CSV: 'text/csv',
            FileExtension.TSV: 'text/tab-separated-values',
        }

    def get_extension(self):
        """Get extension (including .)"""
        return self.get_extension_map()[self]

    def get_delimiter(self):
        """Get delimiter (eg: ',' OR '\t')"""
        return self.get_delimiter_map()[self]

    def get_mime_type(self):
        """Get MIME type for response"""
        return self.get_mimetype_map()[self]


def guess_delimiter_by_filename(filename: str, default_delimiter: Optional[str] = None):
    """
    Guess the delimiter from a filename, as a convenience, allow specifying
    a default_delimiter, and do transformations based on that (eg: un-escaping backslash)
    """
    if default_delimiter:
        return default_delimiter.replace('\\t', '\t')

    filen = filename.lower()

    for ext_type, extension in FileExtension.get_extension_map().items():
        if filen.endswith(extension):
            return ext_type.get_delimiter()

    raise ValueError(
        f'Unable to determine the delimiter from the filename ({filen}), '
        'please explicitly specify a delimiter'
    )
