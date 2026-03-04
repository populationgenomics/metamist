import google.cloud.bigquery as bq

from db.python.gcp_connect import BqConnection
from db.python.layers.billing import BillingLayer


class MockResult:
    """Mimics the RowIterator returned by MockQueryJob.result()."""

    def __init__(self, rows=None, total_rows=None):
        self._rows = rows or []
        self.total_rows = total_rows if total_rows is not None else len(self._rows)

    def __iter__(self):
        return iter(self._rows)


class MockQueryJob:
    """Mimics the BigQuery Job"""

    def __init__(self, rows=None):
        self._rows = rows or MockResult()
        self.total_bytes_processed = 0

    def result(self):
        return self._rows


class MockBqClient(bq.Client):
    """
    Mock BigQuery Client class.
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

    author: str = 'Author'
    gcp_project: str = 'GCP_PROJECT'

    def set_up(self):
        self.table_obj = None

        self.bq_client = MockBqClient()
        self.bq_result = self.bq_client.mock_query_job
        self.connection = MockBqConnection(
            gcp_project=self.gcp_project, author=self.author, client=self.bq_client
        )

        self.layer = BillingLayer(self.connection)
