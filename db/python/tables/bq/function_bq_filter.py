from datetime import datetime
from enum import Enum
from typing import Any

from google.cloud import bigquery

from models.models import BillingColumn


class FunctionBQFilter:
    """
    Function BigQuery filter where left site is a function call
    In such case we need to parameterised values on both side of SQL
    E.g.

    SELECT ...
    FROM ...
    WHERE getLabelValue(labels, 'batch_id') = '1234'

    In this case we have 2 string values which need to be parameterised
    """

    func_where = ''
    func_sql_parameters: list[
        bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter
    ] = []

    def __init__(self, name: str, implementation: str):
        self.func_name = name
        self.fun_implementation = implementation
        # param_id is a counter for parameterised values
        self._param_id = 0

    def to_sql(
        self,
        column_name: BillingColumn,
        func_params: str | list[Any] | dict[Any, Any],
        func_operator: str = None,
    ) -> tuple[str, list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter]]:
        """
        creates the left side of where :  FUN(column_name, @params)
        each of func_params convert to BQ parameter
        combined multiple calls with provided operator,
        if func_operator is None then AND is assumed by default
        """
        values = []
        conditionals = []

        if not isinstance(func_params, dict):
            # Ignore func_params which are not dictionary for the time being
            return '', []

        for param_key, param_value in func_params.items():
            # parameterised both param_key and param_value
            # e.g. this is raw SQL example:
            # getLabelValue(labels, {param_key}) = {param_value}
            self._param_id += 1
            key = f'param{self._param_id}'
            val = f'value{self._param_id}'
            # add param_key as parameterised BQ value
            values.append(FunctionBQFilter._sql_value_prep(key, param_key))

            # add param_value as parameterised BQ value
            values.append(FunctionBQFilter._sql_value_prep(val, param_value))

            # format as FUN(column_name, @param) = @value
            conditionals.append(
                (
                    f'{self.func_name}({column_name.value},@{key}) = '
                    f'{FunctionBQFilter._sql_cond_prep(val, param_value)}'
                )
            )

        if func_operator and func_operator == 'OR':
            condition = ' OR '.join(conditionals)
        else:
            condition = ' AND '.join(conditionals)

        # set the class variables for later use
        self.func_where = f'({condition})'
        self.func_sql_parameters = values
        return self.func_where, self.func_sql_parameters

    @staticmethod
    def _sql_cond_prep(key: str, value: Any) -> str:
        """
        By default '{key}' is used,
        but for datetime it has to be wrapped in TIMESTAMP({key})
        """
        if isinstance(value, datetime):
            return f'TIMESTAMP(@{key})'

        # otherwise as default
        return f'@{key}'

    @staticmethod
    def _sql_value_prep(key: str, value: Any) -> bigquery.ScalarQueryParameter:
        """ """
        if isinstance(value, Enum):
            return bigquery.ScalarQueryParameter(key, 'STRING', value.value)
        if isinstance(value, int):
            return bigquery.ScalarQueryParameter(key, 'INT64', value)
        if isinstance(value, float):
            return bigquery.ScalarQueryParameter(key, 'FLOAT64', value)
        if isinstance(value, datetime):
            return bigquery.ScalarQueryParameter(key, 'STRING', value)

        # otherwise as string parameter
        return bigquery.ScalarQueryParameter(key, 'STRING', value)
