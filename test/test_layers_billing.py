# pylint: disable=protected-access
import datetime
from test.testbase import run_as_sync
from test.testbqbase import BqTest
from unittest import mock

import google.cloud.bigquery as bq

from db.python.layers.billing import BillingLayer
from models.enums import BillingSource
from models.models import BillingColumn, BillingTotalCostQueryModel


class TestBillingLayer(BqTest):
    """Test BillingLayer and its methods"""

    def test_table_factory(self):
        """Test table_factory"""

        layer = BillingLayer(self.connection)

        # test BillingSource types
        table_obj = layer.table_factory()
        self.assertEqual('BillingDailyTable', table_obj.__class__.__name__)

        table_obj = layer.table_factory(source=BillingSource.GCP_BILLING)
        self.assertEqual('BillingGcpDailyTable', table_obj.__class__.__name__)

        table_obj = layer.table_factory(source=BillingSource.RAW)
        self.assertEqual('BillingRawTable', table_obj.__class__.__name__)

        table_obj = layer.table_factory(source=BillingSource.AGGREGATE)
        self.assertEqual('BillingDailyTable', table_obj.__class__.__name__)

        # base columns
        table_obj = layer.table_factory(
            source=BillingSource.AGGREGATE, fields=[BillingColumn.TOPIC]
        )
        self.assertEqual('BillingDailyTable', table_obj.__class__.__name__)

        table_obj = layer.table_factory(
            source=BillingSource.AGGREGATE, filters={BillingColumn.TOPIC: 'TOPIC1'}
        )
        self.assertEqual('BillingDailyTable', table_obj.__class__.__name__)

        # columns from extended view
        table_obj = layer.table_factory(
            source=BillingSource.AGGREGATE, fields=[BillingColumn.AR_GUID]
        )
        self.assertEqual('BillingDailyExtendedTable', table_obj.__class__.__name__)

        table_obj = layer.table_factory(
            source=BillingSource.AGGREGATE, filters={BillingColumn.AR_GUID: 'AR_GUID1'}
        )
        self.assertEqual('BillingDailyExtendedTable', table_obj.__class__.__name__)

    @run_as_sync
    async def test_get_gcp_projects(self):
        """Test get_gcp_projects"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_gcp_projects()
        self.assertEqual([], records)

        # mockup BQ results
        self.bq_result.result.return_value = [
            {'gcp_project': 'PROJECT1'},
            {'gcp_project': 'PROJECT2'},
        ]

        records = await layer.get_gcp_projects()
        self.assertEqual(['PROJECT1', 'PROJECT2'], records)

    @run_as_sync
    async def test_get_topics(self):
        """Test get_topics"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_topics()
        self.assertEqual([], records)

        # mockup BQ results
        self.bq_result.result.return_value = [
            {'topic': 'TOPIC1'},
            {'topic': 'TOPIC2'},
        ]

        records = await layer.get_topics()
        self.assertEqual(['TOPIC1', 'TOPIC2'], records)

    @run_as_sync
    async def test_get_cost_categories(self):
        """Test get_cost_categories"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_cost_categories()
        self.assertEqual([], records)

        # mockup BQ results
        self.bq_result.result.return_value = [
            {'cost_category': 'CAT1'},
            {'cost_category': 'CAT2'},
        ]

        records = await layer.get_cost_categories()
        self.assertEqual(['CAT1', 'CAT2'], records)

    @run_as_sync
    async def test_get_skus(self):
        """Test get_skus"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_skus()
        self.assertEqual([], records)

        # mockup BQ results
        self.bq_result.result.return_value = [
            {'sku': 'SKU1'},
            {'sku': 'SKU2'},
        ]

        records = await layer.get_skus()
        self.assertEqual(['SKU1', 'SKU2'], records)

    @run_as_sync
    async def test_get_datasets(self):
        """Test get_datasets"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_datasets()
        self.assertEqual([], records)

        # mockup BQ results
        self.bq_result.result.return_value = [
            {'dataset': 'DATA1'},
            {'dataset': 'DATA2'},
        ]

        records = await layer.get_datasets()
        self.assertEqual(['DATA1', 'DATA2'], records)

    @run_as_sync
    async def test_get_stages(self):
        """Test get_stages"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_stages()
        self.assertEqual([], records)

        # mockup BQ results
        self.bq_result.result.return_value = [
            {'stage': 'STAGE1'},
            {'stage': 'STAGE2'},
        ]

        records = await layer.get_stages()
        self.assertEqual(['STAGE1', 'STAGE2'], records)

    @run_as_sync
    async def test_get_sequencing_types(self):
        """Test get_sequencing_types"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_sequencing_types()
        self.assertEqual([], records)

        # mockup BQ results
        self.bq_result.result.return_value = [
            {'sequencing_type': 'SEQ1'},
            {'sequencing_type': 'SEQ2'},
        ]

        records = await layer.get_sequencing_types()
        self.assertEqual(['SEQ1', 'SEQ2'], records)

    @run_as_sync
    async def test_get_sequencing_groups(self):
        """Test get_sequencing_groups"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_sequencing_groups()
        self.assertEqual([], records)

        # mockup BQ results
        self.bq_result.result.return_value = [
            {'sequencing_group': 'GRP1'},
            {'sequencing_group': 'GRP2'},
        ]

        records = await layer.get_sequencing_groups()
        self.assertEqual(['GRP1', 'GRP2'], records)

    @run_as_sync
    async def test_get_compute_categories(self):
        """Test get_compute_categories"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_compute_categories()
        self.assertEqual([], records)

        # mockup BQ results
        self.bq_result.result.return_value = [
            {'compute_category': 'CAT1'},
            {'compute_category': 'CAT2'},
        ]

        records = await layer.get_compute_categories()
        self.assertEqual(['CAT1', 'CAT2'], records)

    @run_as_sync
    async def test_get_cromwell_sub_workflow_names(self):
        """Test get_cromwell_sub_workflow_names"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_cromwell_sub_workflow_names()
        self.assertEqual([], records)

        # mockup BQ results
        self.bq_result.result.return_value = [
            {'cromwell_sub_workflow_name': 'CROM1'},
            {'cromwell_sub_workflow_name': 'CROM2'},
        ]

        records = await layer.get_cromwell_sub_workflow_names()
        self.assertEqual(['CROM1', 'CROM2'], records)

    @run_as_sync
    async def test_get_wdl_task_names(self):
        """Test get_wdl_task_names"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_wdl_task_names()
        self.assertEqual([], records)

        # mockup BQ results
        self.bq_result.result.return_value = [
            {'wdl_task_name': 'WDL1'},
            {'wdl_task_name': 'WDL2'},
        ]

        records = await layer.get_wdl_task_names()
        self.assertEqual(['WDL1', 'WDL2'], records)

    @run_as_sync
    async def test_get_invoice_months(self):
        """Test get_invoice_months"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_invoice_months()
        self.assertEqual([], records)

        # mockup BQ results
        self.bq_result.result.return_value = [
            {'invoice_month': '202301'},
            {'invoice_month': '202302'},
        ]

        records = await layer.get_invoice_months()
        self.assertEqual(['202301', '202302'], records)

    @run_as_sync
    async def test_get_namespaces(self):
        """Test get_namespaces"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_namespaces()
        self.assertEqual([], records)

        # mockup BQ results
        self.bq_result.result.return_value = [
            {'namespace': 'NAME1'},
            {'namespace': 'NAME2'},
        ]

        records = await layer.get_namespaces()
        self.assertEqual(['NAME1', 'NAME2'], records)

    @run_as_sync
    async def test_get_total_cost(self):
        """Test get_total_cost"""

        layer = BillingLayer(self.connection)

        # test inparams exceptions:
        query = BillingTotalCostQueryModel(fields=[], start_date='', end_date='')

        with self.assertRaises(ValueError) as context:
            await layer.get_total_cost(query)

        self.assertTrue('Date and Fields are required' in str(context.exception))

        # test with no muckup data, should be empty
        query = BillingTotalCostQueryModel(
            fields=[BillingColumn.TOPIC], start_date='2024-01-01', end_date='2024-01-03'
        )
        records = await layer.get_total_cost(query)
        self.assertEqual([], records)

        # get_total_cost with mockup data is tested in test/test_bq_billing_base.py
        # BillingLayer is just wrapper for BQ tables

    @run_as_sync
    async def test_get_running_cost(self):
        """Test get_running_cost"""

        layer = BillingLayer(self.connection)

        # test inparams exceptions:
        with self.assertRaises(ValueError) as context:
            await layer.get_running_cost(
                field=BillingColumn.TOPIC, invoice_month=None, source=None
            )
        self.assertTrue('Invalid invoice month' in str(context.exception))

        with self.assertRaises(ValueError) as context:
            await layer.get_running_cost(
                field=BillingColumn.TOPIC, invoice_month='2024', source=None
            )
        self.assertTrue('Invalid invoice month' in str(context.exception))

        # test with no muckup data, should be empty
        records = await layer.get_running_cost(
            field=BillingColumn.TOPIC, invoice_month='202401', source=None
        )
        self.assertEqual([], records)

        # get_running_cost with mockup data is tested in test/test_bq_billing_base.py
        # BillingLayer is just wrapper for BQ tables

    @run_as_sync
    async def test_get_cost_by_ar_guid(self):
        """
        Test get_cost_by_ar_guid
        This test only paths in the layer,
        the logic and processing is tested in test/test_bq_billing_base.py
        """

        layer = BillingLayer(self.connection)

        # ar_guid as None, return empty results
        records = await layer.get_cost_by_ar_guid(ar_guid=None)

        # return empty record
        self.assertEqual([], records)

        # dummy ar_guid, no mockup data, return empty results
        dummy_ar_guid = '12345678'
        records = await layer.get_cost_by_ar_guid(ar_guid=dummy_ar_guid)

        # return empty record
        self.assertEqual(
            [],
            records,
        )

        # mock BigQuery first query result
        given_start_day = datetime.datetime(2023, 1, 1, 0, 0)
        given_end_day = datetime.datetime(2023, 1, 1, 2, 3)
        dummy_batch_id = '12345'

        mock_rows = mock.MagicMock(spec=bq.table.RowIterator)
        mock_rows.total_rows = 1
        mock_rows.__iter__.return_value = [
            mock.MagicMock(
                spec=bq.Row,
                batch_id=dummy_batch_id,
                start_day=given_start_day,
                end_day=given_end_day,
            ),
        ]
        self.bq_result.result.return_value = mock_rows

        records = await layer.get_cost_by_ar_guid(ar_guid=dummy_ar_guid)
        # returns empty list as those were not mocked up
        # we do not need to test cost calculation here,
        # as those are tested in test/test_bq_billing_base.py
        self.assertEqual(
            [],
            records,
        )

    @run_as_sync
    async def test_get_cost_by_batch_id(self):
        """
        Test get_cost_by_batch_id
        This test only paths in the layer,
        the logic and processing is tested in test/test_bq_billing_base.py
        """

        layer = BillingLayer(self.connection)

        # ar_guid as None, return empty results
        records = await layer.get_cost_by_batch_id(batch_id=None)

        # return empty record
        self.assertEqual([], records)

        # dummy ar_guid, no mockup data, return empty results
        dummy_batch_id = '12345'
        records = await layer.get_cost_by_batch_id(batch_id=dummy_batch_id)

        # return empty record
        self.assertEqual(
            [],
            records,
        )

        # dummy batch_id, mockup ar_guid

        # mock BigQuery result
        given_start_day = datetime.datetime(2023, 1, 1, 0, 0)
        given_end_day = datetime.datetime(2023, 1, 1, 2, 3)
        dummy_batch_id = '12345'
        dummy_ar_guid = '12345678'

        mock_rows = mock.MagicMock(spec=bq.table.RowIterator)
        mock_rows.total_rows = 1
        mock_rows.__iter__.return_value = [
            mock.MagicMock(
                spec=bq.Row,
                ar_guid=dummy_ar_guid,
                batch_id=dummy_batch_id,
                start_day=given_start_day,
                end_day=given_end_day,
            ),
        ]
        self.bq_result.result.return_value = mock_rows

        records = await layer.get_cost_by_batch_id(batch_id=dummy_batch_id)
        # returns elmpty list as those were not mocked up
        # we do not need to test cost calculation here,
        # as those are tested in test/test_bq_billing_base.py
        self.assertEqual(
            [],
            records,
        )
