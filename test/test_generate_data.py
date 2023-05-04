import unittest
from test.data.generate_data import QUERY_ENUMS, QUERY_SG_ID
from metamist.graphql import validate, configure_sync_client
from api.graphql.schema import schema  # type: ignore


class ValidateGenerateDataQueries(unittest.TestCase):
    """Validate generate data queries"""

    def test_validate_queries(self):
        """Validate queries"""
        client = configure_sync_client(schema=schema.as_str(), auth_token='FAKE')
        validate(QUERY_ENUMS, client=client)
        validate(QUERY_SG_ID, client=client)
