from db.python.enum_tables.enums import EnumTable


class AnalysisTypeTable(EnumTable):
    """
    Replacement for AnalysisType enum
    """

    @classmethod
    def get_enum_name(cls):
        return 'analysis_type'
