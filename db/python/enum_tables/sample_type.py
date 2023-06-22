from db.python.enum_tables.enums import EnumTable


class SampleTypeTable(EnumTable):
    """
    Replacement for SampleType enum
    """

    @classmethod
    def get_enum_name(cls):
        return 'sample_type'
