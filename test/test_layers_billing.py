# pylint: disable=protected-access
import datetime
from test.testbase import run_as_sync
from test.testbqbase import BqTest
from typing import Any
from unittest import mock

import google.cloud.bigquery as bq

from db.python.layers.billing import BillingLayer
from db.python.tables.bq.billing_ar_batch import BillingArBatchTable
from db.python.tables.bq.billing_filter import BillingFilter
from db.python.tables.bq.generic_bq_filter import GenericBQFilter
from db.python.utils import InternalError
from models.models import BillingColumn, BillingTotalCostQueryModel


class TestBillingLayer(BqTest):
    """Test BillingLayer and its methods"""

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
