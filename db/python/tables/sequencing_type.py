from db.python.connect import DbBase
from async_lru import alru_cache


class SequencingTypeTable(DbBase):
    """
    Replacement for SequencingType enum
    """

    table_name = 'sequencing_type'

    @alru_cache(maxsize=1)
    async def get_sequencing_types(self) -> list[str]:
        """
        Get all sequencing types
        """
        _query = """
            SELECT DISTINCT type FROM sequencing_type
        """

        rows = await self.connection.fetch_all(_query)
        rows = [r['type'] for r in rows]

        return rows
