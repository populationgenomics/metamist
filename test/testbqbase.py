import google.cloud.bigquery as bq

from db.python.gcp_connect import BqConnection
from db.python.layers.billing import BillingLayer


class MockRow:
    """Mimics a BigQuery Row that supports both dict and attribute access."""

    def __init__(self, data: dict):
        self._data = data

    def __getattr__(self, name):
        try:
            return self._data[name]
        except KeyError as err:
            raise AttributeError(f"'MockRow' object has no attribute '{name}'") from err

    def __getitem__(self, key):
        return self._data[key]

    def keys(self):
        return self._data.keys()


class MockResult:
    """Mimics the RowIterator returned by google.cloud.bigquery.job.query.QueryJob.result()."""

    def __init__(self, rows=None, total_rows=None):
        # Wrap dicts in MockRow for attribute access support
        self._rows = [MockRow(r) if isinstance(r, dict) else r for r in (rows or [])]
        self.total_rows = total_rows if total_rows is not None else len(self._rows)

    def __iter__(self):
        return iter(self._rows)

    def __bool__(self):
        return len(self._rows) > 0

    def __len__(self):
        return len(self._rows)

    def __getitem__(self, index):
        return self._rows[index]


class MockQueryJob:
    """Mimics google.cloud.bigquery.job.query.QueryJob class."""

    def __init__(self, rows=None):
        self._rows = rows or MockResult()
        self.total_bytes_processed = 0

    def result(self):
        return self._rows

    def set_rows(self, rows):
        """Set rows for the mock result"""
        self._rows = MockResult(rows)

    def __iter__(self):
        return iter(self.result())


class MockBqClient(bq.Client):
    """
    Mock  google.cloud.bigquery.client.Client class.
    """

    def __init__(self):
        self.mock_query_job = MockQueryJob()
        self.executed_queries = []
        self._query_responses: list[list[dict]] = []

    def query(self, query: str, *_args, **_kwargs) -> MockQueryJob:  # pyright: ignore [reportIncompatibleMethodOverride]
        self.executed_queries.append(query)
        # Check if this is a dry run (used for cost calculation)
        # Dry runs don't need actual results, so don't consume from queue
        job_config = _kwargs.get('job_config')
        is_dry_run = job_config and getattr(job_config, 'dry_run', False)

        if not is_dry_run and self._query_responses:
            # Only consume responses for actual queries, not dry runs
            next_response = self._query_responses.pop(0)
            self.mock_query_job.set_rows(next_response)
        # If no queued responses, use whatever is currently set in mock_query_job
        # (allows backward compatibility with tests that use set_rows directly)
        return self.mock_query_job

    def set_query_responses(self, responses: list[list[dict]]):
        """
        Set a queue of responses for sequential queries.
        Each actual (non-dry-run) query() call will consume the next response in the queue.
        """
        self._query_responses = list(responses)


class MockBqConnection(BqConnection):
    """
    Mock BqConnection class.
    """

    def __init__(self, gcp_project: str, author: str, client: MockBqClient):
        self.gcp_project = gcp_project
        self.author = author
        self.connection = client
        self._cost = 0


class BqTest:
    """Base class for Big Query integration tests"""

    # author and grp_project are not used in the BQ tests, but are required
    # so some dummy values are preset
    author: str = 'Author'
    gcp_project: str = 'GCP_PROJECT'

    def base_set_up(self):

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

        # overwrite table object in inherited tests:
        self.table_obj = None
