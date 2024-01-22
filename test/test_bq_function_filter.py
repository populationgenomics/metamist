import dataclasses
import unittest
from datetime import datetime
from enum import Enum
from typing import Any

import google.cloud.bigquery as bq
import pytz

from db.python.tables.bq.function_bq_filter import FunctionBQFilter
from models.models import BillingColumn

"""

func_filter = FunctionBQFilter(
                name='getLabelValue',
                implementation="
                    CREATE TEMP FUNCTION getLabelValue(
                        labels ARRAY<STRUCT<key STRING, value STRING>>, label STRING
                    ) AS (
                        (SELECT value FROM UNNEST(labels) WHERE key = label LIMIT 1)
                    );
                ",
            )
            func_filter.to_sql(
                BillingColumn.LABELS,
                query.filters[BillingColumn.LABELS],
                query.filters_op,
            )
            return func_filter
            
            
            func_filter = self._prepare_labels_function(query)
        if func_filter:
            # extend where_str and query_parameters
            query_parameters.extend(func_filter.func_sql_parameters)

            # now join Prepared Where with Labels Function Where
            where_str = ' AND '.join([where_str, func_filter.func_where])
            
            

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

"""


class BGFunFilterTestEnum(str, Enum):
    """Simple Enum classs"""

    ID = 'id'
    VALUE = 'value'


class TestFunctionBQFilter(unittest.TestCase):
    """Test function filters SQL generation"""

    def test_empty_output(self):
        """Test that function filter converts to SQL as expected"""
        filter_ = FunctionBQFilter(
            name='test_sql_func',
            implementation='CREATE TEMP FUNCTION test_sql_func() AS (SELECT 1);',
        )

        # no params are present, should return empty SQL and Param list
        filter_.to_sql(BillingColumn.LABELS)

        self.assertEqual([], filter_.func_sql_parameters)
        self.assertEqual('', filter_.func_where)

    def test_one_param_output(self):
        """Test that function filter converts to SQL as expected"""
        filter_ = FunctionBQFilter(
            name='test_sql_func',
            implementation='CREATE TEMP FUNCTION test_sql_func() AS (SELECT 1);',
        )

        # no params are present, should return empty SQL and Param list
        filter_.to_sql(BillingColumn.LABELS, {'label_name': 'Some Value'})

        self.assertEqual(
            [
                bq.ScalarQueryParameter('param1', 'STRING', 'label_name'),
                bq.ScalarQueryParameter('value1', 'STRING', 'Some Value'),
            ],
            filter_.func_sql_parameters,
        )
        self.assertEqual(
            '(test_sql_func(labels,@param1) = @value1)', filter_.func_where
        )

    def test_two_param_default_operator_output(self):
        """Test that function filter converts to SQL as expected"""
        filter_ = FunctionBQFilter(
            name='test_sql_func',
            implementation='CREATE TEMP FUNCTION test_sql_func() AS (SELECT 1);',
        )

        # no params are present, should return empty SQL and Param list
        filter_.to_sql(BillingColumn.LABELS, {'label_name1': 10, 'label_name2': 123.45})

        self.assertEqual(
            [
                bq.ScalarQueryParameter('param1', 'STRING', 'label_name1'),
                bq.ScalarQueryParameter('value1', 'INT64', 10),
                bq.ScalarQueryParameter('param2', 'STRING', 'label_name2'),
                bq.ScalarQueryParameter('value2', 'FLOAT64', 123.45),
            ],
            filter_.func_sql_parameters,
        )
        self.assertEqual(
            (
                '(test_sql_func(labels,@param1) = @value1 '
                'AND test_sql_func(labels,@param2) = @value2)'
            ),
            filter_.func_where,
        )

    def test_two_param_operator_or_output(self):
        """Test that function filter converts to SQL as expected"""
        filter_ = FunctionBQFilter(
            name='test_sql_func',
            implementation='CREATE TEMP FUNCTION test_sql_func() AS (SELECT 1);',
        )

        # no params are present, should return empty SQL and Param list
        filter_.to_sql(
            BillingColumn.LABELS,
            {
                'label_name1': BGFunFilterTestEnum.ID,
                'label_name2': datetime(2024, 1, 1),
            },
            'OR',
        )

        self.assertEqual(
            [
                bq.ScalarQueryParameter('param1', 'STRING', 'label_name1'),
                bq.ScalarQueryParameter('value1', 'STRING', 'id'),
                bq.ScalarQueryParameter('param2', 'STRING', 'label_name2'),
                bq.ScalarQueryParameter('value2', 'STRING', '2024-01-01T00:00:00'),
            ],
            filter_.func_sql_parameters,
        )
        self.assertEqual(
            (
                '(test_sql_func(labels,@param1) = @value1 '
                'OR test_sql_func(labels,@param2) = TIMESTAMP(@value2))'
            ),
            filter_.func_where,
        )

    def test_two_param_datime_with_zone_output(self):
        """Test that function filter converts to SQL as expected"""
        filter_ = FunctionBQFilter(
            name='test_sql_func',
            implementation='CREATE TEMP FUNCTION test_sql_func() AS (SELECT 1);',
        )

        # no params are present, should return empty SQL and Param list
        NYC = pytz.timezone('America/New_York')
        SYD = pytz.timezone('Australia/Sydney')
        filter_.to_sql(
            BillingColumn.LABELS,
            {
                'label_name1': datetime(2024, 1, 1).astimezone(NYC),
                'label_name2': datetime(2024, 1, 1).astimezone(SYD),
            },
            'AND',
        )

        self.assertEqual(
            [
                bq.ScalarQueryParameter('param1', 'STRING', 'label_name1'),
                bq.ScalarQueryParameter(
                    'value1', 'STRING', '2023-12-31T08:00:00-05:00'
                ),
                bq.ScalarQueryParameter('param2', 'STRING', 'label_name2'),
                bq.ScalarQueryParameter(
                    'value2', 'STRING', '2024-01-01T00:00:00+11:00'
                ),
            ],
            filter_.func_sql_parameters,
        )
        self.assertEqual(
            (
                '(test_sql_func(labels,@param1) = TIMESTAMP(@value1) '
                'AND test_sql_func(labels,@param2) = TIMESTAMP(@value2))'
            ),
            filter_.func_where,
        )
