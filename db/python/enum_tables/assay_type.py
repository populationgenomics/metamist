from db.python.enum_tables.enums import EnumTable


class AssayTypeTable(EnumTable):
    """
    Replacement for AssayType enum
    """

    @classmethod
    def get_enum_name(cls):
        return 'assay_type'
