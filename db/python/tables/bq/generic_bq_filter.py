from datetime import datetime
from enum import Enum
from typing import Any, TypeVar

from google.cloud import bigquery

from db.python.filters import GenericFilter

T = TypeVar('T')


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
        conditionals: list[str] = []
        values: dict[str, T | list[T] | Any | list[Any]] = {}
        _column_name = column_name or column

        if not isinstance(column, str):
            raise ValueError(f'Column {_column_name!r} must be a string')

        # IN conditions
        self._add_in_condition(self.in_, column, _column_name, conditionals, values)
        self._add_condition(
            'nin', column, _column_name, 'NOT IN UNNEST', conditionals, values
        )

        # Simple conditions
        self._add_condition('eq', column, _column_name, '=', conditionals, values)
        self._add_condition('gt', column, _column_name, '>', conditionals, values)
        self._add_condition('gte', column, _column_name, '>=', conditionals, values)
        self._add_condition('lt', column, _column_name, '<', conditionals, values)
        self._add_condition('lte', column, _column_name, '<=', conditionals, values)

        return ' AND '.join(conditionals), values

    def _add_condition(self, op, column, column_name, operator, conditionals, values):
        if attr := getattr(self, op) is not None:
            k = self.generate_field_name(column_name + '_' + op)
            conditionals.append(f'{column} {operator} {self._sql_cond_prep(k, attr)}')
            values[k] = self._sql_value_prep(k, attr)

    def _add_in_condition(self, attr, column, column_name, conditionals, values):
        if attr is not None:
            if not isinstance(attr, list):
                raise ValueError('IN filter must be a list')
            if len(attr) == 1:
                k = self.generate_field_name(column_name + '_in_eq')
                conditionals.append(f'{column} = {self._sql_cond_prep(k, attr[0])}')
                values[k] = self._sql_value_prep(k, attr[0])
            else:
                k = self.generate_field_name(column_name + '_in')
                conditionals.append(
                    f'{column} IN UNNEST({self._sql_cond_prep(k, attr)})'
                )
                values[k] = self._sql_value_prep(k, attr)

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
            return GenericBQFilter._sql_value_prep(key, value.value)
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
