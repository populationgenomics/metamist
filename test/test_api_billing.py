import unittest
from test.testbase import run_as_sync
from unittest.mock import patch

import api.routes.billing as billing


class TestApiBilling(unittest.TestCase):
    """Test API Billing routes"""

    def test_is_billing_enabled(self):
        """
        Test is_billing_enabled function
        """
        result = billing.is_billing_enabled()
        self.assertEqual(False, result)

    @run_as_sync
    async def test_get_gcp_projects_no_billing(self):
        """
        Test get_gcp_projects function
        """
        with self.assertRaises(ValueError) as context:
            await billing.get_gcp_projects('test_user')

        self.assertTrue('Billing is not enabled' in str(context.exception))

    @run_as_sync
    @patch('api.routes.billing.is_billing_enabled')
    @patch('db.python.layers.billing.BillingLayer.get_gcp_projects')
    async def test_get_gcp_projects(
        self, mock_get_gcp_projects, mock_is_billing_enabled
    ):
        """
        Test get_gcp_projects function
        """

        mockup_records = ['PROJECT1', 'PROJECT2']

        mock_is_billing_enabled.return_value = True
        mock_get_gcp_projects.return_value = mockup_records

        records = await billing.get_gcp_projects(author=None)
        self.assertEqual(mockup_records, records)

    @run_as_sync
    @patch('api.routes.billing.is_billing_enabled')
    @patch('db.python.layers.billing.BillingLayer.get_topics')
    async def test_get_topics(self, mock_get_topics, mock_is_billing_enabled):
        """
        Test get_topics function
        """

        mockup_records = ['TOPIC1', 'TOPIC2']

        mock_is_billing_enabled.return_value = True
        mock_get_topics.return_value = mockup_records

        records = await billing.get_topics(author=None)
        self.assertEqual(mockup_records, records)

    @run_as_sync
    @patch('api.routes.billing.is_billing_enabled')
    @patch('db.python.layers.billing.BillingLayer.get_cost_categories')
    async def test_get_cost_categories(
        self, mock_get_cost_categories, mock_is_billing_enabled
    ):
        """
        Test get_cost_categories function
        """

        mockup_records = ['CAT1', 'CAT2']

        mock_is_billing_enabled.return_value = True
        mock_get_cost_categories.return_value = mockup_records

        records = await billing.get_cost_categories(author=None)
        self.assertEqual(mockup_records, records)

    @run_as_sync
    @patch('api.routes.billing.is_billing_enabled')
    @patch('db.python.layers.billing.BillingLayer.get_skus')
    async def test_get_skus(self, mock_get_skus, mock_is_billing_enabled):
        """
        Test get_skus function
        """

        mockup_records = ['SKU1', 'SKU2', 'SKU3']

        mock_is_billing_enabled.return_value = True
        mock_get_skus.return_value = mockup_records

        records = await billing.get_skus(author=None)
        self.assertEqual(mockup_records, records)

    @run_as_sync
    @patch('api.routes.billing.is_billing_enabled')
    @patch('db.python.layers.billing.BillingLayer.get_datasets')
    async def test_get_datasets(self, mock_get_datasets, mock_is_billing_enabled):
        """
        Test get_datasets function
        """

        mockup_records = ['DS1', 'DS2', 'DS3']

        mock_is_billing_enabled.return_value = True
        mock_get_datasets.return_value = mockup_records

        records = await billing.get_datasets(author=None)
        self.assertEqual(mockup_records, records)

    @run_as_sync
    @patch('api.routes.billing.is_billing_enabled')
    @patch('db.python.layers.billing.BillingLayer.get_sequencing_types')
    async def test_get_sequencing_types(
        self, mock_get_sequencing_types, mock_is_billing_enabled
    ):
        """
        Test get_sequencing_types function
        """

        mockup_records = ['SEQ1', 'SEQ2']

        mock_is_billing_enabled.return_value = True
        mock_get_sequencing_types.return_value = mockup_records

        records = await billing.get_sequencing_types(author=None)
        self.assertEqual(mockup_records, records)

    @run_as_sync
    @patch('api.routes.billing.is_billing_enabled')
    @patch('db.python.layers.billing.BillingLayer.get_stages')
    async def test_get_stages(self, mock_get_stages, mock_is_billing_enabled):
        """
        Test get_stages function
        """

        mockup_records = ['STAGE1', 'STAGE2']

        mock_is_billing_enabled.return_value = True
        mock_get_stages.return_value = mockup_records

        records = await billing.get_stages(author=None)
        self.assertEqual(mockup_records, records)

    @run_as_sync
    @patch('api.routes.billing.is_billing_enabled')
    @patch('db.python.layers.billing.BillingLayer.get_sequencing_groups')
    async def test_get_sequencing_groups(
        self, mock_get_sequencing_groups, mock_is_billing_enabled
    ):
        """
        Test get_sequencing_groups function
        """

        mockup_records = ['SG1', 'SG2']

        mock_is_billing_enabled.return_value = True
        mock_get_sequencing_groups.return_value = mockup_records

        records = await billing.get_sequencing_groups(author=None)
        self.assertEqual(mockup_records, records)

    @run_as_sync
    @patch('api.routes.billing.is_billing_enabled')
    @patch('db.python.layers.billing.BillingLayer.get_compute_categories')
    async def test_get_compute_categories(
        self, mock_get_compute_categories, mock_is_billing_enabled
    ):
        """
        Test get_compute_categories function
        """

        mockup_records = ['CAT1', 'CAT2']

        mock_is_billing_enabled.return_value = True
        mock_get_compute_categories.return_value = mockup_records

        records = await billing.get_compute_categories(author=None)
        self.assertEqual(mockup_records, records)
