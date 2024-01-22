import unittest
from test.testbase import run_as_sync

from api.routes.billing import get_gcp_projects, is_billing_enabled


class TestApiBilling(unittest.TestCase):
    """Test API Billing routes"""

    def test_is_billing_enabled(self):
        """
        Test is_billing_enabled function
        """
        result = is_billing_enabled()
        self.assertEqual(False, result)

    @run_as_sync
    async def test_get_gcp_projects(self):
        """
        Test get_gcp_projects function
        """
        with self.assertRaises(ValueError) as context:
            await get_gcp_projects('test_user')

        self.assertTrue('Billing is not enabled' in str(context.exception))
