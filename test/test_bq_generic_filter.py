import dataclasses
import unittest
from datetime import datetime
from enum import Enum
from typing import Any

from google.cloud import bigquery

from db.python.tables.bq.generic_bq_filter import GenericBQFilter
from db.python.tables.bq.generic_bq_filter_model import GenericBQFilterModel


@dataclasses.dataclass(kw_only=True)
class GenericBQFilterTest(GenericBQFilterModel):
    """Test model for GenericBQFilter"""

    test_string: GenericBQFilter[str] | None = None
    test_int: GenericBQFilter[int] | None = None
    test_float: GenericBQFilter[float] | None = None
    test_dt: GenericBQFilter[datetime] | None = None
    test_dict: dict[str, GenericBQFilter[str]] | None = None
    test_enum: GenericBQFilter[Enum] | None = None
    test_any: Any | None = None


class BGFilterTestEnum(str, Enum):
    """Simple Enum classs"""

    ID = 'id'
    VALUE = 'value'


class TestGenericBQFilter(unittest.TestCase):
    """Test generic filter SQL generation"""

    def test_basic_no_override(self):
        """Test that the basic filter converts to SQL as expected"""
        filter_ = GenericBQFilterTest(test_string=GenericBQFilter(eq='test'))
        sql, values = filter_.to_sql()

        self.assertEqual('test_string = @test_string_eq', sql)
        self.assertDictEqual(
            {
                'test_string_eq': bigquery.ScalarQueryParameter(
                    'test_string_eq', 'STRING', 'test'
                )
            },
            values,
        )

    def test_basic_override(self):
        """Test that the basic filter with an override converts to SQL as expected"""
        filter_ = GenericBQFilterTest(test_string=GenericBQFilter(eq='test'))
        sql, values = filter_.to_sql({'test_string': 't.string'})

        self.assertEqual('t.string = @t_string_eq', sql)
        self.assertDictEqual(
            {
                't_string_eq': bigquery.ScalarQueryParameter(
                    't_string_eq', 'STRING', 'test'
                )
            },
            values,
        )

    def test_single_string(self):
        """
        Test that a single value filtered using the "in" operator
        gets converted to an eq operation
        """
        filter_ = GenericBQFilterTest(test_string=GenericBQFilter(in_=['test']))
        sql, values = filter_.to_sql()

        self.assertEqual('test_string = @test_string_in_eq', sql)
        self.assertDictEqual(
            {
                'test_string_in_eq': bigquery.ScalarQueryParameter(
                    'test_string_in_eq', 'STRING', 'test'
                )
            },
            values,
        )

    def test_single_int(self):
        """
        Test that values filtered using the "in" operator convert as expected
        """
        value = 123
        filter_ = GenericBQFilterTest(test_int=GenericBQFilter(gt=value))
        sql, values = filter_.to_sql()

        self.assertEqual('test_int > @test_int_gt', sql)
        self.assertDictEqual(
            {
                'test_int_gt': bigquery.ScalarQueryParameter(
                    'test_int_gt', 'INT64', value
                )
            },
            values,
        )

    def test_single_float(self):
        """
        Test that values filtered using the "in" operator convert as expected
        """
        value = 123.456
        filter_ = GenericBQFilterTest(test_float=GenericBQFilter(gte=value))
        sql, values = filter_.to_sql()

        self.assertEqual('test_float >= @test_float_gte', sql)
        self.assertDictEqual(
            {
                'test_float_gte': bigquery.ScalarQueryParameter(
                    'test_float_gte', 'FLOAT64', value
                )
            },
            values,
        )

    def test_single_datetime(self):
        """
        Test that values filtered using the "in" operator convert as expected
        """
        datetime_str = '2021-10-08 01:02:03'
        value = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        filter_ = GenericBQFilterTest(test_dt=GenericBQFilter(lt=value))
        sql, values = filter_.to_sql()

        self.assertEqual('test_dt < TIMESTAMP(@test_dt_lt)', sql)
        self.assertDictEqual(
            {
                'test_dt_lt': bigquery.ScalarQueryParameter(
                    'test_dt_lt', 'STRING', datetime_str
                )
            },
            values,
        )

    def test_single_enum(self):
        """
        Test that values filtered using the "in" operator convert as expected
        """
        value = BGFilterTestEnum.ID
        filter_ = GenericBQFilterTest(test_enum=GenericBQFilter(lte=value))
        sql, values = filter_.to_sql()

        self.assertEqual('test_enum <= @test_enum_lte', sql)
        self.assertDictEqual(
            {
                'test_enum_lte': bigquery.ScalarQueryParameter(
                    'test_enum_lte', 'STRING', value.value
                )
            },
            values,
        )

    def test_in_multiple_int(self):
        """
        Test that values filtered using the "in" operator convert as expected
        """
        value = [1, 2]
        filter_ = GenericBQFilterTest(test_int=GenericBQFilter(in_=value))
        sql, values = filter_.to_sql()

        self.assertEqual('test_int IN UNNEST(@test_int_in)', sql)
        self.assertDictEqual(
            {
                'test_int_in': bigquery.ArrayQueryParameter(
                    'test_int_in', 'INT64', value
                )
            },
            values,
        )

    def test_in_multiple_float(self):
        """
        Test that values filtered using the "in" operator convert as expected
        """
        value = [1.0, 2.0]
        filter_ = GenericBQFilterTest(test_float=GenericBQFilter(in_=value))
        sql, values = filter_.to_sql()

        self.assertEqual('test_float IN UNNEST(@test_float_in)', sql)
        self.assertDictEqual(
            {
                'test_float_in': bigquery.ArrayQueryParameter(
                    'test_float_in', 'FLOAT64', value
                )
            },
            values,
        )

    def test_in_multiple_str(self):
        """
        Test that values filtered using the "in" operator convert as expected
        """
        value = ['A', 'B']
        filter_ = GenericBQFilterTest(test_string=GenericBQFilter(in_=value))
        sql, values = filter_.to_sql()

        self.assertEqual('test_string IN UNNEST(@test_string_in)', sql)
        self.assertDictEqual(
            {
                'test_string_in': bigquery.ArrayQueryParameter(
                    'test_string_in', 'STRING', value
                )
            },
            values,
        )

    def test_nin_multiple_str(self):
        """
        Test that values filtered using the "in" operator convert as expected
        """
        value = ['A', 'B']
        filter_ = GenericBQFilterTest(test_string=GenericBQFilter(nin=value))
        sql, values = filter_.to_sql()

        self.assertEqual('test_string NOT IN UNNEST(@test_string_nin)', sql)
        self.assertDictEqual(
            {
                'test_string_nin': bigquery.ArrayQueryParameter(
                    'test_string_nin', 'STRING', value
                )
            },
            values,
        )

    def test_in_and_eq_multiple_str(self):
        """
        Test that values filtered using the "in" operator convert as expected
        """
        value = ['A']
        filter_ = GenericBQFilterTest(test_string=GenericBQFilter(in_=value, eq='B'))
        sql, values = filter_.to_sql()

        self.assertEqual(
            'test_string = @test_string_eq AND test_string = @test_string_in_eq',
            sql,
        )
        self.assertDictEqual(
            {
                'test_string_eq': bigquery.ScalarQueryParameter(
                    'test_string_eq', 'STRING', 'B'
                ),
                'test_string_in_eq': bigquery.ScalarQueryParameter(
                    'test_string_in_eq', 'STRING', 'A'
                ),
            },
            values,
        )

    def test_fail_none_in_tuple(self):
        """
        Test that values filtered using the "in" operator convert as expected
        """
        value = (None,)

        # check if ValueError is raised
        with self.assertRaises(ValueError) as context:
            filter_ = GenericBQFilterTest(test_any=value)
            filter_.to_sql()

        self.assertTrue(
            'There is very likely a trailing comma on the end of '
            'GenericBQFilterTest.test_any. '
            'If you actually want a tuple of length one with the value = (None,), '
            'then use dataclasses.field(default_factory=lambda: (None,))'
            in str(context.exception)
        )
