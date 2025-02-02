import unittest

from metamist.graphql import configure_sync_client, validate

from api.graphql.schema import schema  # type: ignore
from test.data.generate_data import QUERY_ENUMS, QUERY_SG_ID


class ValidateGenerateDataQueries(unittest.TestCase):
    """Validate generate data queries"""

    def test_validate_queries(self):
        """Validate queries"""
        client = configure_sync_client(schema=schema.as_str(), auth_token='FAKE')
        validate(QUERY_ENUMS, client=client)
        validate(QUERY_SG_ID, client=client)
