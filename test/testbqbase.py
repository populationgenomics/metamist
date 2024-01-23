import unittest
from typing import Any

# from unittest import mock
import google.cloud.bigquery as bq

from db.python.gcp_connect import BqConnection


class BqTest(unittest.TestCase):
    """Base class for Big Query integration tests"""

    # store connections here, so they can be created PER-CLASS
    # and don't get recreated per test.

    # author and grp_project are not used in the BQ tests, but are required
    # so some dummy values are preset
    author: str = 'Author'
    gcp_project: str = 'GCP_PROJECT'

    bq_result: bq.job.QueryJob
    bq_client: bq.Client
    connection: BqConnection
    table_obj: Any | None = None

    def setUp(self) -> None:
        super().setUp()

        # Mockup BQ results
        self.bq_result = unittest.mock.MagicMock(spec=bq.job.QueryJob)

        # mock BigQuery client
        self.bq_client = unittest.mock.MagicMock(spec=bq.Client)
        self.bq_client.query.return_value = self.bq_result

        # Mock BqConnection
        self.connection = unittest.mock.MagicMock(spec=BqConnection)
        self.connection.gcp_project = self.gcp_project
        self.connection.connection = self.bq_client
        self.connection.author = self.author

        # overwrite table object in inhereted tests:
        self.table_obj = None
