# pylint: disable=protected-access too-many-public-methods
import datetime
import json
from test.testbase import run_as_sync
from test.testbqbase import BqTest
from unittest.mock import patch

from api.routes import billing
from models.models import (
    AnalysisCostRecord,
    BillingColumn,
    BillingCostBudgetRecord,
    BillingTotalCostQueryModel,
    BillingTotalCostRecord,
)

TEST_API_BILLING_USER = 'test_user'


class TestApiBilling(BqTest):
    """
    Test API Billing routes
    Billing routes are only calling layer functions and returning data it received
    This set of tests only checks if all routes code has been called
    and if data returned is the same as data received from layer functions
    It does not check all possible combination of parameters as
    Billing Layer and BQ Tables should be testing those
    """

    def setUp(self):
        super().setUp()

        # make billing enabled by default for all the calls
        patcher = patch('api.routes.billing.is_billing_enabled', return_value=True)
        self.mockup_is_billing_enabled = patcher.start()

    @run_as_sync
    @patch('api.routes.billing.is_billing_enabled', return_value=False)
    async def test_get_gcp_projects_no_billing(self, _mockup_is_billing_enabled):
        """
        Test get_gcp_projects function
        This function should raise ValueError if billing is not enabled
        """
        with self.assertRaises(ValueError) as context:
            await billing.get_gcp_projects('test_user')

        self.assertTrue('Billing is not enabled' in str(context.exception))

    @run_as_sync
    @patch('api.routes.billing._get_billing_layer_from')
    @patch('db.python.layers.billing.BillingLayer.get_cost_by_ar_guid')
    async def test_get_cost_by_ar_guid(
        self, mock_get_cost_by_ar_guid, mock_get_billing_layer
    ):
        """
        Test get_cost_by_ar_guid function
        """
        ar_guid = 'test_ar_guid'
        mockup_record_json = {
            'total': {
                'ar_guid': ar_guid,
                'cost': 0.0,
                'usage_end_time': None,
                'usage_start_time': None,
            },
            'topics': [],
            'categories': [],
            'batches': [],
            'skus': [],
            'seq_groups': [],
            'wdl_tasks': [],
            'cromwell_sub_workflows': [],
            'cromwell_workflows': [],
            'dataproc': [],
        }

        mockup_record = [AnalysisCostRecord.from_dict(mockup_record_json)]
        mock_get_billing_layer.return_value = self.layer
        mock_get_cost_by_ar_guid.return_value = mockup_record
        response = await billing.get_cost_by_ar_guid(
            ar_guid, author=TEST_API_BILLING_USER
        )
        resp_json = json.loads(response.body.decode('utf-8'))
        self.assertEqual(1, len(resp_json))

        self.assertDictEqual(mockup_record_json, resp_json[0])

    @run_as_sync
    @patch('api.routes.billing._get_billing_layer_from')
    @patch('db.python.layers.billing.BillingLayer.get_cost_by_batch_id')
    async def test_get_cost_by_batch_id(
        self, mock_get_cost_by_batch_id, mock_get_billing_layer
    ):
        """
        Test get_cost_by_batch_id function
        """
        ar_guid = 'test_ar_guid'
        batch_id = 'test_batch_id'
        mockup_record_json = {
            'total': {
                'ar_guid': ar_guid,
                'cost': 0.0,
                'usage_end_time': None,
                'usage_start_time': None,
            },
            'topics': [],
            'categories': [],
            'batches': [
                {
                    'batch_id': batch_id,
                    'batch_name': None,
                    'cost': 0.0,
                    'usage_start_time': datetime.datetime.now().isoformat(),
                    'usage_end_time': datetime.datetime.now().isoformat(),
                    'jobs_cnt': 0,
                    'skus': [],
                    'jobs': [],
                    'seq_groups': [],
                }
            ],
            'skus': [],
            'seq_groups': [],
            'wdl_tasks': [],
            'cromwell_sub_workflows': [],
            'cromwell_workflows': [],
            'dataproc': [],
        }

        mockup_record = [AnalysisCostRecord.from_dict(mockup_record_json)]
        mock_get_billing_layer.return_value = self.layer
        mock_get_cost_by_batch_id.return_value = mockup_record
        response = await billing.get_cost_by_batch_id(
            batch_id, author=TEST_API_BILLING_USER
        )
        resp_json = json.loads(response.body.decode('utf-8'))

        self.assertEqual(1, len(resp_json))
        self.assertDictEqual(mockup_record_json, resp_json[0])

    @run_as_sync
    @patch('api.routes.billing._get_billing_layer_from')
    @patch('db.python.layers.billing.BillingLayer.get_total_cost')
    async def test_get_total_cost(self, mock_get_total_cost, mock_get_billing_layer):
        """
        Test get_total_cost function
        """
        query = BillingTotalCostQueryModel(fields=[], start_date='', end_date='')
        mockup_record = [{'cost': 123.45}, {'cost': 123}]
        expected = [BillingTotalCostRecord.from_json(r) for r in mockup_record]
        mock_get_billing_layer.return_value = self.layer
        mock_get_total_cost.return_value = mockup_record
        records = await billing.get_total_cost(query, author=TEST_API_BILLING_USER)
        self.assertEqual(expected, records)

    @run_as_sync
    @patch('api.routes.billing._get_billing_layer_from')
    @patch('db.python.layers.billing.BillingLayer.get_running_cost')
    async def test_get_running_cost(
        self, mock_get_running_cost, mock_get_billing_layer
    ):
        """
        Test get_running_cost function
        """
        mockup_record = [
            BillingCostBudgetRecord.from_json(
                {'field': 'TOPIC1', 'total_monthly': 999.99, 'details': []}
            ),
            BillingCostBudgetRecord.from_json(
                {'field': 'TOPIC2', 'total_monthly': 123, 'details': []}
            ),
        ]
        mock_get_billing_layer.return_value = self.layer
        mock_get_running_cost.return_value = mockup_record
        records = await billing.get_running_costs(
            field=BillingColumn.TOPIC,
            invoice_month=None,
            source=None,
            author=TEST_API_BILLING_USER,
        )
        self.assertEqual(mockup_record, records)

    @patch('api.routes.billing._get_billing_layer_from')
    async def call_api_function(
        self,
        api_function,
        mock_layer_function,
        mock_get_billing_layer=None,
    ):
        """
        Common wrapper for all API calls, to avoid code duplication
        API function is called with author=TEST_API_BILLING_USER
        get_author function will be tested separately
        We only testing if routes are calling layer functions and
        returning data it received
        """
        mock_get_billing_layer.return_value = self.layer
        mockup_records = ['RECORD1', 'RECORD2']
        mock_layer_function.return_value = mockup_records
        records = await api_function(author=TEST_API_BILLING_USER)
        self.assertEqual(mockup_records, records)

    @run_as_sync
    @patch('db.python.layers.billing.BillingLayer.get_gcp_projects')
    async def test_get_gcp_projects(self, mock_get_gcp_projects):
        """
        Test get_gcp_projects function
        """
        await self.call_api_function(billing.get_gcp_projects, mock_get_gcp_projects)

    @run_as_sync
    @patch('db.python.layers.billing.BillingLayer.get_topics')
    async def test_get_topics(self, mock_get_topics):
        """
        Test get_topics function
        """
        await self.call_api_function(billing.get_topics, mock_get_topics)

    @run_as_sync
    @patch('db.python.layers.billing.BillingLayer.get_cost_categories')
    async def test_get_cost_categories(self, mock_get_cost_categories):
        """
        Test get_cost_categories function
        """
        await self.call_api_function(
            billing.get_cost_categories, mock_get_cost_categories
        )

    @run_as_sync
    @patch('db.python.layers.billing.BillingLayer.get_skus')
    async def test_get_skus(self, mock_get_skus):
        """
        Test get_skus function
        """
        await self.call_api_function(billing.get_skus, mock_get_skus)

    @run_as_sync
    @patch('db.python.layers.billing.BillingLayer.get_datasets')
    async def test_get_datasets(self, mock_get_datasets):
        """
        Test get_datasets function
        """
        await self.call_api_function(billing.get_datasets, mock_get_datasets)

    @run_as_sync
    @patch('db.python.layers.billing.BillingLayer.get_sequencing_types')
    async def test_get_sequencing_types(self, mock_get_sequencing_types):
        """
        Test get_sequencing_types function
        """
        await self.call_api_function(
            billing.get_sequencing_types, mock_get_sequencing_types
        )

    @run_as_sync
    @patch('db.python.layers.billing.BillingLayer.get_stages')
    async def test_get_stages(self, mock_get_stages):
        """
        Test get_stages function
        """
        await self.call_api_function(billing.get_stages, mock_get_stages)

    @run_as_sync
    @patch('db.python.layers.billing.BillingLayer.get_sequencing_groups')
    async def test_get_sequencing_groups(self, mock_get_sequencing_groups):
        """
        Test get_sequencing_groups function
        """
        await self.call_api_function(
            billing.get_sequencing_groups, mock_get_sequencing_groups
        )

    @run_as_sync
    @patch('db.python.layers.billing.BillingLayer.get_compute_categories')
    async def test_get_compute_categories(self, mock_get_compute_categories):
        """
        Test get_compute_categories function
        """
        await self.call_api_function(
            billing.get_compute_categories, mock_get_compute_categories
        )

    @run_as_sync
    @patch('db.python.layers.billing.BillingLayer.get_cromwell_sub_workflow_names')
    async def test_get_cromwell_sub_workflow_names(
        self, mock_get_cromwell_sub_workflow_names
    ):
        """
        Test get_cromwell_sub_workflow_names function
        """
        await self.call_api_function(
            billing.get_cromwell_sub_workflow_names,
            mock_get_cromwell_sub_workflow_names,
        )

    @run_as_sync
    @patch('db.python.layers.billing.BillingLayer.get_wdl_task_names')
    async def test_get_wdl_task_names(self, mock_get_wdl_task_names):
        """
        Test get_wdl_task_names function
        """
        await self.call_api_function(
            billing.get_wdl_task_names, mock_get_wdl_task_names
        )

    @run_as_sync
    @patch('db.python.layers.billing.BillingLayer.get_invoice_months')
    async def test_get_invoice_months(self, mock_get_invoice_months):
        """
        Test get_invoice_months function
        """
        await self.call_api_function(
            billing.get_invoice_months, mock_get_invoice_months
        )

    @run_as_sync
    @patch('db.python.layers.billing.BillingLayer.get_namespaces')
    async def test_get_namespaces(self, mock_get_namespaces):
        """
        Test get_namespaces function
        """
        await self.call_api_function(billing.get_namespaces, mock_get_namespaces)
