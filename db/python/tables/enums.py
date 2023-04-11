import re
import abc
from functools import lru_cache

from db.python.connect import DbBase
from async_lru import alru_cache

table_name_matcher = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


class EnumTable(DbBase):
    """Base for ENUM type tables with basic caching"""

    @abc.abstractmethod
    @classmethod
    def get_table_name(cls):
        raise NotImplementedError

    @lru_cache(maxsize=1)
    @classmethod
    def _get_table_name(cls):
        tn = cls.get_table_name()
        # validate table name meets mariadb table name regex
        matcher = table_name_matcher.match(tn)
        if not matcher:
            raise ValueError(f'The tablename {tn} is not valid (must match {table_name_matcher.pattern})')
        return tn

    @alru_cache(maxsize=1)
    async def get(self) -> list[str]:
        """
        Get all sequencing types
        """
        _query = f'SELECT DISTINCT type FROM {self._get_table_name()}'
        rows = await self.connection.fetch_all(_query)
        rows = [r['type'] for r in rows]

        return rows

    async def insert(self, value: str):
        """
        Insert a new type
        """
        _query = f"""
            INSERT INTO {self._get_table_name()} (id, value)
            VALUES (:value, :value)
        """

        await self.connection.execute(_query, {'value': value})
        # clear the cache so results are up-to-date
        self.get.cache_clear()
