from db.python.enum_tables.enums import EnumTable


class SequencingTechnologyTable(EnumTable):
    """
    Replacement for SequencingType enum
    """

    @classmethod
    def get_enum_name(cls):
        return 'sequencing_technology'

    @classmethod
    def get_pluralised_enum_name(cls):
        return 'sequencing_technologies'
