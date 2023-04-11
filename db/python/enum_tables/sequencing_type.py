from db.python.enum_tables.enums import EnumTable


class SequencingTypeTable(EnumTable):
    """
    Replacement for SequencingType enum
    """

    @classmethod
    def get_enum_name(cls):
        return 'sequencing_type'
