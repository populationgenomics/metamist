from datetime import datetime
from enum import Enum
from typing import Any

from google.cloud import bigquery

from db.python.utils import GenericFilter, T


class GenericBQFilter(GenericFilter[T]):
    """
    Generic BigQuery filter is BQ specific filter class, based on GenericFilter
    """

    def __eq__(self, other):
        """Equality operator"""
        if not isinstance(other, GenericBQFilter):
            return False

        keys = ['eq', 'in_', 'nin', 'gt', 'gte', 'lt', 'lte']
        for att in keys:
            if getattr(self, att) != getattr(other, att):
                return False

        # all attributes are equal
        return True

    def to_sql(
        self, column: str, column_name: str = None
    ) -> tuple[str, dict[str, T | list[T] | Any | list[Any]]]:
        """
        Convert to SQL, and avoid SQL injection

        """
        conditionals = []
        values: dict[str, T | list[T] | Any | list[Any]] = {}
        _column_name = column_name or column

        if not isinstance(column, str):
            raise ValueError(f'Column {_column_name!r} must be a string')
        if self.eq is not None:
            k = self.generate_field_name(_column_name + '_eq')
            conditionals.append(f'{column} = {self._sql_cond_prep(k, self.eq)}')
            values[k] = self._sql_value_prep(k, self.eq)
        if self.in_ is not None:
            if not isinstance(self.in_, list):
                raise ValueError('IN filter must be a list')
            if len(self.in_) == 1:
                k = self.generate_field_name(_column_name + '_in_eq')
                conditionals.append(f'{column} = {self._sql_cond_prep(k, self.in_[0])}')
                values[k] = self._sql_value_prep(k, self.in_[0])
            else:
                k = self.generate_field_name(_column_name + '_in')
                conditionals.append(
                    f'{column} IN UNNEST({self._sql_cond_prep(k, self.in_)})'
                )
                values[k] = self._sql_value_prep(k, self.in_)
        if self.nin is not None:
            if not isinstance(self.nin, list):
                raise ValueError('NIN filter must be a list')
            k = self.generate_field_name(column + '_nin')
            conditionals.append(
                f'{column} NOT IN UNNEST({self._sql_cond_prep(k, self.nin)})'
            )
            values[k] = self._sql_value_prep(k, self.nin)
        if self.gt is not None:
            k = self.generate_field_name(column + '_gt')
            conditionals.append(f'{column} > {self._sql_cond_prep(k, self.gt)}')
            values[k] = self._sql_value_prep(k, self.gt)
        if self.gte is not None:
            k = self.generate_field_name(column + '_gte')
            conditionals.append(f'{column} >= {self._sql_cond_prep(k, self.gte)}')
            values[k] = self._sql_value_prep(k, self.gte)
        if self.lt is not None:
            k = self.generate_field_name(column + '_lt')
            conditionals.append(f'{column} < {self._sql_cond_prep(k, self.lt)}')
            values[k] = self._sql_value_prep(k, self.lt)
        if self.lte is not None:
            k = self.generate_field_name(column + '_lte')
            conditionals.append(f'{column} <= {self._sql_cond_prep(k, self.lte)}')
            values[k] = self._sql_value_prep(k, self.lte)

        return ' AND '.join(conditionals), values

    @staticmethod
    def _sql_cond_prep(key, value) -> str:
        """
        By default '@{key}' is used,
        but for datetime it has to be wrapped in TIMESTAMP(@{k})
        """
        if isinstance(value, datetime):
            return f'TIMESTAMP(@{key})'

        # otherwise as default
        return f'@{key}'

    @staticmethod
    def _sql_value_prep(key, value):
        """
        Overrides the default _sql_value_prep to handle BQ parameters
        """
        if isinstance(value, list):
            if value and isinstance(value[0], int):
                return bigquery.ArrayQueryParameter(key, 'INT64', value)
            if value and isinstance(value[0], float):
                return bigquery.ArrayQueryParameter(key, 'FLOAT64', value)

            # otherwise all list records as string
            return bigquery.ArrayQueryParameter(key, 'STRING', [str(v) for v in value])

        if isinstance(value, Enum):
            return bigquery.ScalarQueryParameter(key, 'STRING', value.value)
        if isinstance(value, int):
            return bigquery.ScalarQueryParameter(key, 'INT64', value)
        if isinstance(value, float):
            return bigquery.ScalarQueryParameter(key, 'FLOAT64', value)
        if isinstance(value, datetime):
            return bigquery.ScalarQueryParameter(
                key, 'STRING', value.strftime('%Y-%m-%d %H:%M:%S')
            )

        # otherwise as string parameter
        return bigquery.ScalarQueryParameter(key, 'STRING', value)
