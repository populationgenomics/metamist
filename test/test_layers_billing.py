import datetime

import pytest

from db.python.layers.billing import BillingLayer
from models.enums import BillingSource
from models.models import BillingColumn, BillingTotalCostQueryModel
from test.testbqbase import BqTest


class TestBillingLayer:
    """Test BillingLayer and its methods"""

    @pytest.fixture(autouse=True)
    def setup_bq_test(self):
        self.bq_test = BqTest()
        self.bq_test.set_up()
        self.connection = self.bq_test.connection
        self.bq_result = self.bq_test.bq_result
        self.bq_client = self.bq_test.bq_client

    def test_table_factory(self):
        """Test table_factory"""

        layer = BillingLayer(self.connection)

        # test BillingSource types
        table_obj = layer.table_factory()
        assert table_obj.__class__.__name__ == 'BillingDailyTable'

        table_obj = layer.table_factory(source=BillingSource.GCP_BILLING)
        assert table_obj.__class__.__name__ == 'BillingGcpDailyTable'

        table_obj = layer.table_factory(source=BillingSource.RAW)
        assert table_obj.__class__.__name__ == 'BillingRawTable'

        table_obj = layer.table_factory(source=BillingSource.AGGREGATE)
        assert table_obj.__class__.__name__ == 'BillingDailyTable'

        # base columns
        table_obj = layer.table_factory(
            source=BillingSource.AGGREGATE, fields=[BillingColumn.TOPIC]
        )
        assert table_obj.__class__.__name__ == 'BillingDailyTable'

        table_obj = layer.table_factory(
            source=BillingSource.AGGREGATE, filters={BillingColumn.TOPIC: 'TOPIC1'}
        )
        assert table_obj.__class__.__name__ == 'BillingDailyTable'

        # columns from extended view
        table_obj = layer.table_factory(
            source=BillingSource.AGGREGATE, fields=[BillingColumn.AR_GUID]
        )
        assert table_obj.__class__.__name__ == 'BillingDailyExtendedTable'

        table_obj = layer.table_factory(
            source=BillingSource.AGGREGATE, filters={BillingColumn.AR_GUID: 'AR_GUID1'}
        )
        assert table_obj.__class__.__name__ == 'BillingDailyExtendedTable'

    @pytest.mark.asyncio
    async def test_get_gcp_projects(self):
        """Test get_gcp_projects"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_gcp_projects()
        assert records == []

        # mockup BQ results
        self.bq_result.set_rows(
            [
                {'gcp_project': 'PROJECT1'},
                {'gcp_project': 'PROJECT2'},
            ]
        )

        records = await layer.get_gcp_projects()
        assert records == ['PROJECT1', 'PROJECT2']

    @pytest.mark.asyncio
    async def test_get_topics(self):
        """Test get_topics"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_topics()
        assert records == []

        # mockup BQ results
        self.bq_result.set_rows(
            [
                {'topic': 'TOPIC1'},
                {'topic': 'TOPIC2'},
            ]
        )

        records = await layer.get_topics()
        assert records == ['TOPIC1', 'TOPIC2']

    @pytest.mark.asyncio
    async def test_get_cost_categories(self):
        """Test get_cost_categories"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_cost_categories()
        assert records == []

        # mockup BQ results
        self.bq_result.set_rows(
            [
                {'cost_category': 'CAT1'},
                {'cost_category': 'CAT2'},
            ]
        )

        records = await layer.get_cost_categories()
        assert records == ['CAT1', 'CAT2']

    @pytest.mark.asyncio
    async def test_get_skus(self):
        """Test get_skus"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_skus()
        assert records == []

        # mockup BQ results
        self.bq_result.set_rows(
            [
                {'sku': 'SKU1'},
                {'sku': 'SKU2'},
            ]
        )

        records = await layer.get_skus()
        assert records == ['SKU1', 'SKU2']

    @pytest.mark.asyncio
    async def test_get_datasets(self):
        """Test get_datasets"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_datasets()
        assert records == []

        # mockup BQ results
        self.bq_result.set_rows(
            [
                {'dataset': 'DATA1'},
                {'dataset': 'DATA2'},
            ]
        )

        records = await layer.get_datasets()
        assert records == ['DATA1', 'DATA2']

    @pytest.mark.asyncio
    async def test_get_stages(self):
        """Test get_stages"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_stages()
        assert records == []

        # mockup BQ results
        self.bq_result.set_rows(
            [
                {'stage': 'STAGE1'},
                {'stage': 'STAGE2'},
            ]
        )

        records = await layer.get_stages()
        assert records == ['STAGE1', 'STAGE2']

    @pytest.mark.asyncio
    async def test_get_sequencing_types(self):
        """Test get_sequencing_types"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_sequencing_types()
        assert records == []

        # mockup BQ results
        self.bq_result.set_rows(
            [
                {'sequencing_type': 'SEQ1'},
                {'sequencing_type': 'SEQ2'},
            ]
        )

        records = await layer.get_sequencing_types()
        assert records == ['SEQ1', 'SEQ2']

    @pytest.mark.asyncio
    async def test_get_sequencing_groups(self):
        """Test get_sequencing_groups"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_sequencing_groups()
        assert records == []

        # mockup BQ results
        self.bq_result.set_rows(
            [
                {'sequencing_group': 'GRP1'},
                {'sequencing_group': 'GRP2'},
            ]
        )

        records = await layer.get_sequencing_groups()
        assert records == ['GRP1', 'GRP2']

    @pytest.mark.asyncio
    async def test_get_compute_categories(self):
        """Test get_compute_categories"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_compute_categories()
        assert records == []

        # mockup BQ results
        self.bq_result.set_rows(
            [
                {'compute_category': 'CAT1'},
                {'compute_category': 'CAT2'},
            ]
        )

        records = await layer.get_compute_categories()
        assert records == ['CAT1', 'CAT2']

    @pytest.mark.asyncio
    async def test_get_cromwell_sub_workflow_names(self):
        """Test get_cromwell_sub_workflow_names"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_cromwell_sub_workflow_names()
        assert records == []

        # mockup BQ results
        self.bq_result.set_rows(
            [
                {'cromwell_sub_workflow_name': 'CROM1'},
                {'cromwell_sub_workflow_name': 'CROM2'},
            ]
        )

        records = await layer.get_cromwell_sub_workflow_names()
        assert records == ['CROM1', 'CROM2']

    @pytest.mark.asyncio
    async def test_get_wdl_task_names(self):
        """Test get_wdl_task_names"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_wdl_task_names()
        assert records == []

        # mockup BQ results
        self.bq_result.set_rows(
            [
                {'wdl_task_name': 'WDL1'},
                {'wdl_task_name': 'WDL2'},
            ]
        )

        records = await layer.get_wdl_task_names()
        assert records == ['WDL1', 'WDL2']

    @pytest.mark.asyncio
    async def test_get_invoice_months(self):
        """Test get_invoice_months"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_invoice_months()
        assert records == []

        # mockup BQ results
        self.bq_result.set_rows(
            [
                {'invoice_month': '202301'},
                {'invoice_month': '202302'},
            ]
        )

        records = await layer.get_invoice_months()
        assert records == ['202301', '202302']

    @pytest.mark.asyncio
    async def test_get_namespaces(self):
        """Test get_namespaces"""

        layer = BillingLayer(self.connection)

        # test with no muckup data, should be empty
        records = await layer.get_namespaces()
        assert records == []

        # mockup BQ results
        self.bq_result.set_rows(
            [
                {'namespace': 'NAME1'},
                {'namespace': 'NAME2'},
            ]
        )

        records = await layer.get_namespaces()
        assert records == ['NAME1', 'NAME2']

    @pytest.mark.asyncio
    async def test_get_total_cost(self):
        """Test get_total_cost"""

        layer = BillingLayer(self.connection)

        # test inparams exceptions:
        query = BillingTotalCostQueryModel(fields=[], start_date='', end_date='')

        with pytest.raises(ValueError) as context:
            await layer.get_total_cost(query, connection=None)

        assert 'Date and Fields are required' in str(context.value)

        # test with no muckup data, should be empty
        query = BillingTotalCostQueryModel(
            fields=[BillingColumn.TOPIC], start_date='2024-01-01', end_date='2024-01-03'
        )
        records = await layer.get_total_cost(query, connection=None)
        assert records == []

        # get_total_cost with mockup data is tested in test/test_bq_billing_base.py
        # BillingLayer is just wrapper for BQ tables

    @pytest.mark.asyncio
    async def test_get_running_cost(self):
        """Test get_running_cost"""

        layer = BillingLayer(self.connection)

        # test inparams exceptions:
        with pytest.raises(ValueError) as context:
            await layer.get_running_cost(
                field=BillingColumn.TOPIC, invoice_month=None, source=None
            )
        assert 'Invalid invoice month' in str(context.value)

        with pytest.raises(ValueError) as context:
            await layer.get_running_cost(
                field=BillingColumn.TOPIC, invoice_month='2024', source=None
            )
        assert 'Invalid invoice month' in str(context.value)

        # test with no muckup data, should be empty
        records = await layer.get_running_cost(
            field=BillingColumn.TOPIC, invoice_month='202401', source=None
        )
        assert records == []

        # get_running_cost with mockup data is tested in test/test_bq_billing_base.py
        # BillingLayer is just wrapper for BQ tables

    @pytest.mark.asyncio
    async def test_get_cost_by_ar_guid(self):
        """
        Test get_cost_by_ar_guid
        This test only paths in the layer,
        the logic and processing is tested in test/test_bq_billing_base.py
        """

        layer = BillingLayer(self.connection)

        # ar_guid as None, return empty results
        records = await layer.get_cost_by_ar_guid(ar_guid=None)

        # return empty record
        assert records == []

        # dummy ar_guid, no mockup data, return empty results
        dummy_ar_guid = '12345678'
        records = await layer.get_cost_by_ar_guid(ar_guid=dummy_ar_guid)

        # return empty record
        assert records == []

        # mock BigQuery queries - first returns batch info, second returns empty (cost summary)
        given_start_day = datetime.datetime(2023, 1, 1, 0, 0)
        given_end_day = datetime.datetime(2023, 1, 1, 2, 3)
        dummy_batch_id = '12345'

        # Set up sequential responses: first query returns batches, second returns empty
        self.bq_client.set_query_responses(
            [
                # First query: get_batches_by_ar_guid
                [
                    {
                        'batch_id': dummy_batch_id,
                        'start_day': given_start_day,
                        'end_day': given_end_day,
                    }
                ],
                # Second query: get_batch_cost_summary - returns empty
                [],
            ]
        )

        records = await layer.get_cost_by_ar_guid(ar_guid=dummy_ar_guid)
        # returns empty list as cost summary was not mocked up
        # we do not need to test cost calculation here,
        # as those are tested in test/test_bq_billing_base.py
        assert records == []

    @pytest.mark.asyncio
    async def test_get_cost_by_batch_id(self):
        """
        Test get_cost_by_batch_id
        This test only paths in the layer,
        the logic and processing is tested in test/test_bq_billing_base.py
        """

        layer = BillingLayer(self.connection)

        # ar_guid as None, return empty results
        records = await layer.get_cost_by_batch_id(batch_id=None)

        # return empty record
        assert records == []

        # dummy ar_guid, no mockup data, return empty results
        dummy_batch_id = '12345'
        records = await layer.get_cost_by_batch_id(batch_id=dummy_batch_id)

        # return empty record
        assert records == []

        # dummy batch_id, mockup ar_guid

        # mock BigQuery queries
        given_start_day = datetime.datetime(2023, 1, 1, 0, 0)
        given_end_day = datetime.datetime(2023, 1, 1, 2, 3)
        dummy_batch_id = '12345'
        dummy_ar_guid = '12345678'

        # Set up sequential responses for the query chain:
        # 1. get_ar_guid_by_batch_id - returns ar_guid info
        # 2. get_batches_by_ar_guid - returns batches (since ar_guid != batch_id)
        # 3. get_batch_cost_summary - returns empty
        self.bq_client.set_query_responses(
            [
                # First query: get_ar_guid_by_batch_id
                [
                    {
                        'ar_guid': dummy_ar_guid,
                        'batch_id': dummy_batch_id,
                        'start_day': given_start_day,
                        'end_day': given_end_day,
                    }
                ],
                # Second query: get_batches_by_ar_guid
                [
                    {
                        'batch_id': dummy_batch_id,
                        'start_day': given_start_day,
                        'end_day': given_end_day,
                    }
                ],
                # Third query: get_batch_cost_summary - returns empty
                [],
            ]
        )

        records = await layer.get_cost_by_batch_id(batch_id=dummy_batch_id)
        # returns empty list as cost summary was not mocked up
        # we do not need to test cost calculation here,
        # as those are tested in test/test_bq_billing_base.py
        assert records == []
