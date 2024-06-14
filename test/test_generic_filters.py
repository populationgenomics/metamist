import dataclasses
import unittest

from db.python.filters import GenericFilter, GenericFilterModel


@dataclasses.dataclass(kw_only=True)
class GenericFilterTest(GenericFilterModel):
    """Test model for GenericFilter"""

    test_string: GenericFilter[str] | None = None
    test_int: GenericFilter[int] | None = None
    test_dict: dict[str, GenericFilter[str]] | None = None


class TestGenericFilters(unittest.TestCase):
    """Test generic filters SQL generation"""

    def test_basic_no_override(self):
        """Test that the basic filter converts to SQL as expected"""
        filter_ = GenericFilterTest(test_string=GenericFilter(eq='test'))
        sql, values = filter_.to_sql()

        self.assertEqual('test_string = :test_string_eq', sql)
        self.assertDictEqual({'test_string_eq': 'test'}, values)

    def test_contains_case_sensitive(self):
        """Test that the basic filter converts to SQL as expected"""
        filter_ = GenericFilterTest(test_string=GenericFilter(contains='test'))
        sql, values = filter_.to_sql()

        self.assertEqual('test_string LIKE :test_string_contains', sql)
        self.assertDictEqual({'test_string_contains': '%test%'}, values)

    def test_malicious_contains(self):
        """Test that a contains-% filter converts to SQL by appropriately encoding _ and %"""
        filter_ = GenericFilterTest(test_string=GenericFilter(contains='per%ce_nt'))
        sql, values = filter_.to_sql()

        self.assertEqual('test_string LIKE :test_string_contains', sql)
        self.assertDictEqual({'test_string_contains': r'%per\%ce\_nt%'}, values)

    def test_icontains_is_not_case_sensitive(self):
        """Test that the basic filter converts to SQL as expected"""
        filter_ = GenericFilterTest(test_string=GenericFilter(icontains='test'))
        sql, values = filter_.to_sql()

        self.assertEqual('LOWER(test_string) LIKE LOWER(:test_string_icontains)', sql)
        self.assertDictEqual({'test_string_icontains': '%test%'}, values)

    def test_malicious_icontains(self):
        """Test that an icontains-% filter converts to SQL by appropriately encoding _ and %"""
        filter_ = GenericFilterTest(test_string=GenericFilter(icontains='per%ce_nt'))
        sql, values = filter_.to_sql()

        self.assertEqual('LOWER(test_string) LIKE LOWER(:test_string_icontains)', sql)
        self.assertDictEqual({'test_string_icontains': r'%per\%ce\_nt%'}, values)

    def test_basic_override(self):
        """Test that the basic filter with an override converts to SQL as expected"""
        filter_ = GenericFilterTest(test_string=GenericFilter(eq='test'))
        sql, values = filter_.to_sql({'test_string': 't.string'})

        self.assertEqual('t.string = :t_string_eq', sql)
        self.assertDictEqual({'t_string_eq': 'test'}, values)

    def test_in_single(self):
        """
        Test that a single value filtered using the "in" operator
        gets converted to an eq operation
        """
        filter_ = GenericFilterTest(test_string=GenericFilter(in_=['test']))
        sql, values = filter_.to_sql()

        self.assertEqual('test_string = :test_string_in_eq', sql)
        self.assertDictEqual({'test_string_in_eq': 'test'}, values)

    def test_in_multiple(self):
        """
        Test that values filtered using the "in" operator convert as expected
        """
        value = [1, 2]
        filter_ = GenericFilterTest(test_int=GenericFilter(in_=value))
        sql, values = filter_.to_sql()

        self.assertEqual('test_int IN :test_int_in', sql)
        self.assertDictEqual({'test_int_in': value}, values)

    def test_gt_single(self):
        """
        Test that a single value filtered using the "gt" operator
        """
        filter_ = GenericFilterTest(test_int=GenericFilter(gt=123))
        sql, values = filter_.to_sql()

        self.assertEqual('test_int > :test_int_gt', sql)
        self.assertDictEqual({'test_int_gt': 123}, values)

    def test_gte_single(self):
        """
        Test that a single value filtered using the "gte" operator
        """
        filter_ = GenericFilterTest(test_int=GenericFilter(gte=123))
        sql, values = filter_.to_sql()

        self.assertEqual('test_int >= :test_int_gte', sql)
        self.assertDictEqual({'test_int_gte': 123}, values)

    def test_lt_single(self):
        """
        Test that a single value filtered using the "lt" operator
        """
        filter_ = GenericFilterTest(test_int=GenericFilter(lt=123))
        sql, values = filter_.to_sql()

        self.assertEqual('test_int < :test_int_lt', sql)
        self.assertDictEqual({'test_int_lt': 123}, values)

    def test_lte_single(self):
        """
        Test that a single value filtered using the "lte" operator
        """
        filter_ = GenericFilterTest(test_int=GenericFilter(lte=123))
        sql, values = filter_.to_sql()

        self.assertEqual('test_int <= :test_int_lte', sql)
        self.assertDictEqual({'test_int_lte': 123}, values)

    def test_not_in_multiple(self):
        """
        Test that values filtered using the "nin" operator convert as expected
        """
        value = [1, 2]
        filter_ = GenericFilterTest(test_int=GenericFilter(nin=value))
        sql, values = filter_.to_sql()

        self.assertEqual('test_int NOT IN :test_int_nin', sql)
        self.assertDictEqual({'test_int_nin': value}, values)
