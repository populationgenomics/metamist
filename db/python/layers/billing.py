from db.python.layers.bq_base import BqBaseLayer
from db.python.tables.bq.billing_ar_batch import BillingArBatchTable
from db.python.tables.bq.billing_daily import BillingDailyTable
from db.python.tables.bq.billing_daily_extended import BillingDailyExtendedTable
from db.python.tables.bq.billing_gcp_daily import BillingGcpDailyTable
from db.python.tables.bq.billing_raw import BillingRawTable
from models.enums import BillingSource
from models.models import (
    AnalysisCostRecord,
    BillingColumn,
    BillingCostBudgetRecord,
    BillingTotalCostQueryModel,
)


class BillingLayer(BqBaseLayer):
    """Billing layer"""

    def table_factory(
        self,
        source: BillingSource | None = None,
        fields: list[BillingColumn] | None = None,
        filters: dict[BillingColumn, str | list | dict] | None = None,
    ) -> (
        BillingDailyTable
        | BillingDailyExtendedTable
        | BillingGcpDailyTable
        | BillingRawTable
    ):
        """Get billing table object based on source and fields"""
        if source == BillingSource.GCP_BILLING:
            return BillingGcpDailyTable(self.connection)
        if source == BillingSource.RAW:
            return BillingRawTable(self.connection)
        if source == BillingSource.EXTENDED:
            return BillingDailyExtendedTable(self.connection)

        # check if any of the fields is in the extended columns
        if fields:
            used_extended_cols = [
                f
                for f in fields
                if f in BillingColumn.extended_cols() and BillingColumn.can_group_by(f)
            ]
            if used_extended_cols:
                # there is a field from extended daily table
                return BillingDailyExtendedTable(self.connection)

        # check if any of the filters is in the extended columns
        if filters:
            used_extended_cols = [
                f
                for f in filters
                if f in BillingColumn.extended_cols() and BillingColumn.can_group_by(f)
            ]
            if used_extended_cols:
                # there is a field from extended daily table
                return BillingDailyExtendedTable(self.connection)

        # by default look at the daily table
        return BillingDailyTable(self.connection)

    async def get_gcp_projects(
        self,
    ) -> list[str] | None:
        """
        Get All GCP projects in database
        """
        billing_table = BillingGcpDailyTable(self.connection)
        return await billing_table.get_gcp_projects()

    async def get_topics(
        self,
    ) -> list[str] | None:
        """
        Get All topics in database
        """
        billing_table = BillingDailyTable(self.connection)
        return await billing_table.get_topics()

    async def get_cost_categories(
        self,
    ) -> list[str] | None:
        """
        Get All service description / cost categories in database
        """
        billing_table = BillingDailyTable(self.connection)
        return await billing_table.get_cost_categories()

    async def get_skus(
        self,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[str] | None:
        """
        Get All SKUs in database
        """
        billing_table = BillingDailyTable(self.connection)
        return await billing_table.get_skus(limit, offset)

    async def get_datasets(
        self,
    ) -> list[str] | None:
        """
        Get All datasets in database
        """
        billing_table = BillingDailyExtendedTable(self.connection)
        return await billing_table.get_extended_values('dataset')

    async def get_stages(
        self,
    ) -> list[str] | None:
        """
        Get All stages in database
        """
        billing_table = BillingDailyExtendedTable(self.connection)
        return await billing_table.get_extended_values('stage')

    async def get_sequencing_types(
        self,
    ) -> list[str] | None:
        """
        Get All sequencing_types in database
        """
        billing_table = BillingDailyExtendedTable(self.connection)
        return await billing_table.get_extended_values('sequencing_type')

    async def get_sequencing_groups(
        self,
    ) -> list[str] | None:
        """
        Get All sequencing_groups in database
        """
        billing_table = BillingDailyExtendedTable(self.connection)
        return await billing_table.get_extended_values('sequencing_group')

    async def get_compute_categories(
        self,
    ) -> list[str] | None:
        """
        Get All compute_category values in database
        """
        billing_table = BillingDailyExtendedTable(self.connection)
        return await billing_table.get_extended_values('compute_category')

    async def get_cromwell_sub_workflow_names(
        self,
    ) -> list[str] | None:
        """
        Get All cromwell_sub_workflow_name values in database
        """
        billing_table = BillingDailyExtendedTable(self.connection)
        return await billing_table.get_extended_values('cromwell_sub_workflow_name')

    async def get_wdl_task_names(
        self,
    ) -> list[str] | None:
        """
        Get All wdl_task_name values in database
        """
        billing_table = BillingDailyExtendedTable(self.connection)
        return await billing_table.get_extended_values('wdl_task_name')

    async def get_invoice_months(
        self,
    ) -> list[str] | None:
        """
        Get All invoice months in database
        """
        billing_table = BillingDailyTable(self.connection)
        return await billing_table.get_invoice_months()

    async def get_namespaces(
        self,
    ) -> list[str] | None:
        """
        Get All namespaces values in database
        """
        billing_table = BillingDailyExtendedTable(self.connection)
        return await billing_table.get_extended_values('namespace')

    async def get_total_cost(
        self,
        query: BillingTotalCostQueryModel,
    ) -> list[dict] | None:
        """
        Get Total cost of selected fields for requested time interval
        """
        billing_table = self.table_factory(query.source, query.fields, query.filters)
        return await billing_table.get_total_cost(query)

    async def get_running_cost(
        self,
        field: BillingColumn,
        invoice_month: str | None = None,
        source: BillingSource | None = None,
    ) -> list[BillingCostBudgetRecord]:
        """
        Get Running costs including monthly budget
        """
        billing_table = self.table_factory(source, [field])
        return await billing_table.get_running_cost(field, invoice_month)

    async def get_cost_by_ar_guid(
        self,
        ar_guid: str | None = None,
    ) -> list[AnalysisCostRecord]:
        """
        Get Costs by AR GUID
        """
        ar_batch_lookup_table = BillingArBatchTable(self.connection)

        # First get all batches and the min/max day to use for the query
        (
            start_day,
            end_day,
            batches,
        ) = await ar_batch_lookup_table.get_batches_by_ar_guid(ar_guid)

        if not batches:
            return []

        billing_table = BillingDailyExtendedTable(self.connection)
        results = await billing_table.get_batch_cost_summary(
            start_day, end_day, batches, ar_guid
        )
        return results

    async def get_cost_by_batch_id(
        self,
        batch_id: str | None = None,
    ) -> list[AnalysisCostRecord]:
        """
        Get Costs by Batch ID
        """
        ar_batch_lookup_table = BillingArBatchTable(self.connection)

        # First get all batches and the min/max day to use for the query
        (
            start_day,
            end_day,
            ar_guid,
        ) = await ar_batch_lookup_table.get_ar_guid_by_batch_id(batch_id)

        if ar_guid is None:
            return []

        if ar_guid != batch_id:
            # found ar_guid for the batch_id
            # The get all batches for the ar_guid
            (
                start_day,
                end_day,
                batches,
            ) = await ar_batch_lookup_table.get_batches_by_ar_guid(ar_guid)

            if not batches:
                return []
        else:
            # ar_guid is not present, so use the batch_id
            batches = [batch_id]
            ar_guid = None

        billing_table = BillingDailyExtendedTable(self.connection)
        results = await billing_table.get_batch_cost_summary(
            start_day, end_day, batches, ar_guid
        )
        return results
