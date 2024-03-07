import unittest
from datetime import date, datetime

from api.utils.dates import (
    get_invoice_month_range,
    parse_date_only_string,
    reformat_datetime,
)


class TestApiUtils(unittest.TestCase):
    """Test API utils functions"""

    def test_parse_date_only_string(self):
        """
        Test parse_date_only_string function
        """
        result_none = parse_date_only_string(None)
        self.assertEqual(None, result_none)

        result_date = parse_date_only_string('2021-01-10')
        self.assertEqual(2021, result_date.year)
        self.assertEqual(1, result_date.month)
        self.assertEqual(10, result_date.day)

        # test exception
        invalid_date_str = '123456789'
        with self.assertRaises(ValueError) as context:
            parse_date_only_string(invalid_date_str)

        self.assertTrue(
            f'Date could not be converted: {invalid_date_str}' in str(context.exception)
        )

    def test_get_invoice_month_range(self):
        """
        Test get_invoice_month_range function
        """
        jan_2021 = datetime.strptime('2021-01-10', '%Y-%m-%d').date()
        res_jan_2021 = get_invoice_month_range(jan_2021)

        # there is 3 (INVOICE_DAY_DIFF) days difference between invoice month st and end
        self.assertEqual(
            (date(2020, 12, 29), date(2021, 2, 3)),
            res_jan_2021,
        )

        dec_2021 = datetime.strptime('2021-12-10', '%Y-%m-%d').date()
        res_dec_2021 = get_invoice_month_range(dec_2021)

        # there is 3 (INVOICE_DAY_DIFF) days difference between invoice month st and end
        self.assertEqual(
            (date(2021, 11, 28), date(2022, 1, 3)),
            res_dec_2021,
        )

    def test_reformat_datetime(self):
        """
        Test reformat_datetime function
        """
        in_format = '%Y-%m-%d'
        out_format = '%d/%m/%Y'

        result_none = reformat_datetime(None, in_format, out_format)
        self.assertEqual(None, result_none)

        result_formatted = reformat_datetime('2021-11-09', in_format, out_format)
        self.assertEqual('09/11/2021', result_formatted)

        # test exception
        invalid_date_str = '123456789'
        with self.assertRaises(ValueError) as context:
            reformat_datetime(invalid_date_str, in_format, out_format)

        self.assertTrue(
            f'Date could not be converted: {invalid_date_str}' in str(context.exception)
        )
