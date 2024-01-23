import unittest
from datetime import datetime
from enum import Enum

import google.cloud.bigquery as bq
import pytz

from db.python.tables.bq.function_bq_filter import FunctionBQFilter
from models.models import BillingColumn


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
                'label_name1': datetime(2024, 1, 1, tzinfo=pytz.UTC).astimezone(NYC),
                'label_name2': datetime(2024, 1, 1, tzinfo=pytz.UTC).astimezone(SYD),
            },
            'AND',
        )

        self.assertEqual(
            [
                bq.ScalarQueryParameter('param1', 'STRING', 'label_name1'),
                bq.ScalarQueryParameter(
                    'value1', 'STRING', '2023-12-31T19:00:00-05:00'
                ),
                bq.ScalarQueryParameter('param2', 'STRING', 'label_name2'),
                bq.ScalarQueryParameter(
                    'value2', 'STRING', '2024-01-01T11:00:00+11:00'
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
