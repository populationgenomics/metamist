import re
import abc
from functools import lru_cache
from async_lru import alru_cache

from db.python.connect import DbBase

table_name_matcher = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


class EnumTable(DbBase):
    """Base for ENUM type tables with basic caching"""

    @classmethod
    def get_table_name(cls):
        """Get name of enum table"""
        return cls.get_enum_name()

    @classmethod
    @abc.abstractmethod
    def get_enum_name(cls):
        """Get name of enum"""
        raise NotImplementedError

    @classmethod
    def get_pluralised_enum_name(cls):
        """Get name of enum"""
        return cls.get_enum_name() + 's'

    @classmethod
    @lru_cache(maxsize=1)
    def _get_table_name(cls):
        """Wrapped to check table name is valid"""
        tn = cls.get_table_name()
        # validate table name meets mariadb table name regex
        matcher = table_name_matcher.match(tn)
        if not matcher:
            raise ValueError(
                f'The tablename {tn} is not valid (must match {table_name_matcher.pattern})'
            )
        return tn

    @alru_cache(maxsize=1)
    async def get(self) -> list[str]:
        """
        Get all sequencing types
        """
        _query = f'SELECT DISTINCT name FROM {self._get_table_name()}'
        rows = await self.connection.fetch_all(_query)
        rows = [r['name'] for r in rows]

        return rows

    async def insert(self, value: str):
        """
        Insert a new type
        """
        _query = f"""
            INSERT INTO {self._get_table_name()} (id, name)
            VALUES (:name, :name)
        """

        await self.connection.execute(_query, {'name': value.lower()})
        # clear the cache so results are up-to-date
        self.get.cache_clear()  # pylint: disable
