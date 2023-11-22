from db.python.layers.billing_db import BillingDb
from db.python.layers.bq_base import BqBaseLayer
from db.python.tables.billing import BillingFilter
from models.models import (
    BillingColumn,
    BillingCostBudgetRecord,
    BillingRowRecord,
    BillingTotalCostQueryModel,
    BillingTotalCostRecord,
)


class BillingLayer(BqBaseLayer):
    """Billing layer"""

    async def get_gcp_projects(
        self,
    ) -> list[str] | None:
        """
        Get All GCP projects in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_gcp_projects()

    async def get_topics(
        self,
    ) -> list[str] | None:
        """
        Get All topics in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_topics()

    async def get_cost_categories(
        self,
    ) -> list[str] | None:
        """
        Get All service description / cost categories in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_cost_categories()

    async def get_skus(
        self,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[str] | None:
        """
        Get All SKUs in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_skus(limit, offset)

    async def get_datasets(
        self,
    ) -> list[str] | None:
        """
        Get All datasets in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_extended_values('dataset')

    async def get_stages(
        self,
    ) -> list[str] | None:
        """
        Get All stages in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_extended_values('stage')

    async def get_sequencing_types(
        self,
    ) -> list[str] | None:
        """
        Get All sequencing_types in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_extended_values('sequencing_type')

    async def get_sequencing_groups(
        self,
    ) -> list[str] | None:
        """
        Get All sequencing_groups in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_extended_values('sequencing_group')

    async def get_compute_categories(
        self,
    ) -> list[str] | None:
        """
        Get All compute_category values in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_extended_values('compute_category')

    async def get_cromwell_sub_workflow_names(
        self,
    ) -> list[str] | None:
        """
        Get All cromwell_sub_workflow_name values in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_extended_values('cromwell_sub_workflow_name')

    async def get_wdl_task_names(
        self,
    ) -> list[str] | None:
        """
        Get All wdl_task_name values in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_extended_values('wdl_task_name')

    async def get_invoice_months(
        self,
    ) -> list[str] | None:
        """
        Get All invoice months in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_invoice_months()

    async def query(
        self,
        _filter: BillingFilter,
        limit: int = 10,
    ) -> list[BillingRowRecord] | None:
        """
        Get Billing record for the given gilter
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.query(_filter, limit)

    async def get_total_cost(
        self,
        query: BillingTotalCostQueryModel,
    ) -> list[BillingTotalCostRecord] | None:
        """
        Get Total cost of selected fields for requested time interval
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_total_cost(query)

    async def get_running_cost(
        self,
        field: BillingColumn,
        invoice_month: str | None = None,
        source: str | None = None,
    ) -> list[BillingCostBudgetRecord]:
        """
        Get Running costs including monthly budget
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_running_cost(field, invoice_month, source)
