import google.cloud.bigquery as bq

from db.python.gcp_connect import BqConnection
from db.python.layers.billing import BillingLayer


class MockResult:
    """Mimics the RowIterator returned by google.cloud.bigquery.job.query.QueryJob.result()."""

    def __init__(self, rows=None, total_rows=None):
        self._rows = rows or []
        self.total_rows = total_rows if total_rows is not None else len(self._rows)

    def __iter__(self):
        return iter(self._rows)


class MockQueryJob:
    """Mimics google.cloud.bigquery.job.query.QueryJob class."""

    def __init__(self, rows=None):
        self._rows = rows or MockResult()
        self.total_bytes_processed = 0

    def result(self):
        return self._rows


class MockBqClient(bq.Client):
    """
    Mock  google.cloud.bigquery.client.Client class.
    """

    def __init__(self):
        super().__init__()
        self.mock_query_job = MockQueryJob()
        self.executed_queries = []

    def query(self, query: str, *_args, **_kwargs) -> MockQueryJob:
        self.executed_queries.append(query)
        return self.mock_query_job


class MockBqConnection(BqConnection):
    """
    Mock BqConnection class.
    """

    def __init__(self, gcp_project: str, author: str, client: MockBqClient):
        super().__init__(author)
        self.gcp_project = gcp_project
        self.author = author
        self.connection = client


class BqTest:
    """Base class for Big Query integration tests"""

    # author and grp_project are not used in the BQ tests, but are required
    # so some dummy values are preset
    author: str = 'Author'
    gcp_project: str = 'GCP_PROJECT'

    def set_up(self):
        self.table_obj = None

        # mock BigQuery client
        self.bq_client = MockBqClient()

        # Mockup BQ results
        self.bq_result = self.bq_client.mock_query_job

        # Mock BqConnection
        self.connection = MockBqConnection(
            gcp_project=self.gcp_project, author=self.author, client=self.bq_client
        )

        # Mockup BillingLayer
        self.layer = BillingLayer(self.connection)
