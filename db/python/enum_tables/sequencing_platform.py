from db.python.enum_tables.enums import EnumTable


class SequencingPlatformTable(EnumTable):
    """
    Replacement for SequencingPlatform enum
    """

    @classmethod
    def get_enum_name(cls):
        return 'sequencing_platform'
