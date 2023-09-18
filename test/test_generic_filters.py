import dataclasses
import unittest

from db.python.utils import GenericFilter, GenericFilterModel


@dataclasses.dataclass(kw_only=True)
class GenericFilterTest(GenericFilterModel):
    """Test model for GenericFilter"""

    test_string: GenericFilter[str] | None = None
    test_dict: dict[str, GenericFilter[str]] | None = None


class TestGenericFilters(unittest.TestCase):
    """Test generic filters SQL generation"""

    def test_basic_no_override(self):
        """Test that the basic filter converts to SQL as expected"""
        filter_ = GenericFilterTest(test_string=GenericFilter(eq='test'))
        sql, values = filter_.to_sql()

        self.assertEqual('test_string = :test_string_eq', sql)
        self.assertDictEqual({'test_string_eq': 'test'}, values)

    def test_basic_override(self):
        """Test that the basic filter with an override converts to SQL as expected"""
        filter_ = GenericFilterTest(test_string=GenericFilter(eq='test'))
        sql, values = filter_.to_sql({'test_string': 't.string'})

        self.assertEqual('t.string = :t_string_eq', sql)
        self.assertDictEqual({'t_string_eq': 'test'}, values)
