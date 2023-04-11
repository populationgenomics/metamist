from db.python.tables.enums import EnumTable


class SequencingTypeTable(EnumTable):
    """
    Replacement for SequencingType enum
    """
    @classmethod
    def get_table_name(cls):
        return 'sequencing_type'
