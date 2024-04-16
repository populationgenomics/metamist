# pylint: disable=protected-access
import datetime
from test.testbase import run_as_sync
from test.testbqbase import BqTest
from typing import Any
from unittest import mock

import google.cloud.bigquery as bq

from db.python.tables.bq.billing_ar_batch import BillingArBatchTable
from db.python.tables.bq.billing_filter import BillingFilter
from db.python.tables.bq.generic_bq_filter import GenericBQFilter
from db.python.utils import InternalError
from models.models import BillingColumn, BillingTotalCostQueryModel


class TestBillingArBatchTable(BqTest):
    """Test BillingArBatchTable and its methods"""

    def setUp(self):
        super().setUp()

        # setup table object
        self.table_obj = BillingArBatchTable(self.connection)

    def test_query_to_partitioned_filter(self):
        """Test query to partitioned filter conversion"""

        # given
        start_date = '2023-01-01'
        end_date = '2024-01-01'
        filters: dict[BillingColumn, str | list[Any] | dict[Any, Any]] = {
            BillingColumn.TOPIC: 'TEST_TOPIC'
        }

        # expected
        expected_filter = BillingFilter(
            day=GenericBQFilter(
                gte=datetime.datetime(2023, 1, 1, 0, 0),
                lte=datetime.datetime(2024, 1, 1, 0, 0),
            ),
            topic=GenericBQFilter(eq='TEST_TOPIC'),
        )

        query = BillingTotalCostQueryModel(
            fields=[],  # not relevant for this test, but can't be null generally
            start_date=start_date,
            end_date=end_date,
            filters=filters,
        )
        filter_ = BillingArBatchTable._query_to_partitioned_filter(query)

        # BillingFilter has __eq__ method, so we can compare them directly
        self.assertEqual(expected_filter, filter_)

    def test_error_no_connection(self):
        """Test No connection exception"""

        with self.assertRaises(InternalError) as context:
            BillingArBatchTable(None)

        self.assertTrue(
            'No connection was provided to the table \'BillingArBatchTable\''
            in str(context.exception)
        )

    def test_get_table_name(self):
        """Test get_table_name"""

        # table name is set in the class
        given_table_name = 'TEST_TABLE_NAME'

        # set table name
        self.table_obj.table_name = given_table_name

        # test get table name function
        table_name = self.table_obj.get_table_name()

        self.assertEqual(given_table_name, table_name)

    @run_as_sync
    async def test_get_batches_by_ar_guid_no_data(self):
        """Test get_batches_by_ar_guid"""

        ar_guid = '1234567890'

        # mock BigQuery result
        self.bq_result.result.return_value = []

        # test get_batches_by_ar_guid function
        (start_day, end_day, batch_ids) = await self.table_obj.get_batches_by_ar_guid(
            ar_guid
        )

        self.assertEqual(None, start_day)
        self.assertEqual(None, end_day)
        self.assertEqual([], batch_ids)

    @run_as_sync
    async def test_get_batches_by_ar_guid_one_record(self):
        """Test get_batches_by_ar_guid"""

        ar_guid = '1234567890'
        given_start_day = datetime.datetime(2023, 1, 1, 0, 0)
        given_end_day = datetime.datetime(2023, 1, 1, 2, 3)

        # mock BigQuery result
        self.bq_result.result.return_value = [
            mock.MagicMock(
                spec=bq.Row,
                start_day=given_start_day,
                end_day=given_end_day,
                batch_id='Batch1234',
            )
        ]

        # test get_batches_by_ar_guid function
        (start_day, end_day, batch_ids) = await self.table_obj.get_batches_by_ar_guid(
            ar_guid
        )

        self.assertEqual(given_start_day, start_day)
        # end day is the last day + 1
        self.assertEqual(given_end_day + datetime.timedelta(days=1), end_day)
        self.assertEqual(['Batch1234'], batch_ids)

    @run_as_sync
    async def test_get_batches_by_ar_guid_two_record(self):
        """Test get_batches_by_ar_guid"""

        ar_guid = '1234567890'
        given_start_day = datetime.datetime(2023, 1, 1, 0, 0)
        given_end_day = datetime.datetime(2023, 1, 1, 2, 3)

        # mock BigQuery result
        self.bq_result.result.return_value = [
            mock.MagicMock(
                spec=bq.Row,
                start_day=given_start_day,
                end_day=given_start_day,
                batch_id='FirstBatch',
            ),
            mock.MagicMock(
                spec=bq.Row,
                start_day=given_start_day,
                end_day=given_end_day,
                batch_id='SecondBatch',
            ),
        ]

        # test get_batches_by_ar_guid function
        (start_day, end_day, batch_ids) = await self.table_obj.get_batches_by_ar_guid(
            ar_guid
        )

        self.assertEqual(given_start_day, start_day)
        # end day is the last day + 1
        self.assertEqual(given_end_day + datetime.timedelta(days=1), end_day)
        self.assertEqual(['FirstBatch', 'SecondBatch'], batch_ids)

    @run_as_sync
    async def test_get_ar_guid_by_batch_id_no_data(self):
        """Test get_ar_guid_by_batch_id"""

        batch_id = '1234567890'

        # mock BigQuery result
        self.bq_result.result.return_value = []

        # test get_ar_guid_by_batch_id function
        _, _, ar_guid = await self.table_obj.get_ar_guid_by_batch_id(batch_id)

        self.assertEqual(None, ar_guid)

    @run_as_sync
    async def test_get_ar_guid_by_batch_id_one_rec(self):
        """Test get_ar_guid_by_batch_id"""

        batch_id = '1234567890'
        expected_ar_guid = 'AR_GUID_1234'

        # mock BigQuery result
        self.bq_result.result.return_value = [
            {
                'ar_guid': expected_ar_guid,
                'start_day': datetime.datetime(2024, 1, 1, 0, 0),
                'end_day': datetime.datetime(2024, 1, 2, 0, 0),
            }
        ]

        # test get_ar_guid_by_batch_id function
        _, _, ar_guid = await self.table_obj.get_ar_guid_by_batch_id(batch_id)

        self.assertEqual(expected_ar_guid, ar_guid)
