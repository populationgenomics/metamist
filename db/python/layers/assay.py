# pylint: disable=too-many-arguments
from typing import Any

from db.python.utils import NoOpAenter
from db.python.layers.base import BaseLayer, Connection
from db.python.tables.assay import AssayTable, AssayFilter
from db.python.tables.sample import SampleTable
from models.models.assay import AssayInternal, AssayUpsertInternal


class AssayLayer(BaseLayer):
    """Layer for more complex sample logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.seqt: AssayTable = AssayTable(connection)
        self.sampt: SampleTable = SampleTable(connection)

    # GET
    async def query(self, filter_: AssayFilter = None, check_project_id=True):
        """Query for samples"""

        projects, assays = await self.seqt.query(filter_)

        if not assays:
            return []

        if check_project_id:
            await self.ptable.check_access_to_project_ids(
                user=self.author, project_ids=projects, readonly=True
            )

        return assays

    async def get_assay_by_id(
        self, assay_id: int, check_project_id=True
    ) -> AssayInternal:
        """Get assay by ID"""
        project, assay = await self.seqt.get_assay_by_id(assay_id)

        if check_project_id:
            await self.ptable.check_access_to_project_id(
                self.author, project, readonly=True
            )

        return assay

    async def get_assay_by_external_id(
        self, external_assay_id: str, project: int = None
    ):
        """
        Get assay from an external ID you must have a project specified on the
        connection (or supplied as an argument) to use this method.
        """

        assay = await self.seqt.get_assay_by_external_id(
            external_assay_id, project=project
        )
        return assay

    async def get_assays_for_sequencing_group_ids(
        self, sequencing_group_ids: list[int], check_project_ids=True
    ) -> dict[int, list[AssayInternal]]:
        """Get assays for a list of sequencing group IDs"""
        projects, assays = await self.seqt.get_assays_for_sequencing_group_ids(
            sequencing_group_ids=sequencing_group_ids,
        )

        if not assays:
            return {}

        if check_project_ids:
            await self.ptable.check_access_to_project_ids(
                self.author, projects, readonly=True
            )

        return assays

    async def get_assays_by(
        self,
        sample_ids: list[int] = None,
        assay_ids: list[int] = None,
        external_assay_ids: list[str] = None,
        assay_meta: dict[str, Any] = None,
        sample_meta: dict[str, Any] = None,
        project_ids=None,
        assay_types: list[str] = None,
        active=True,
    ):
        """Get sequences by some criteria"""
        if not sample_ids and not assay_ids and not project_ids:
            raise ValueError(
                'Must specify one of "project_ids", "sample_ids" or "assay_ids"'
            )

        projs, seqs = await self.seqt.get_assays_by(
            assay_ids=assay_ids,
            external_assay_ids=external_assay_ids,
            sample_ids=sample_ids,
            assay_types=assay_types,
            assay_meta=assay_meta,
            sample_meta=sample_meta,
            project_ids=project_ids,
            active=active,
        )

        if not project_ids:
            # if we didn't specify a project, we need to check access
            # to the projects we got back
            await self.ptable.check_access_to_project_ids(
                self.author, projs, readonly=True
            )

        return seqs

    # region UPSERTs

    async def upsert_assay(
        self, assay: AssayUpsertInternal, check_project_id=True, open_transaction=True
    ) -> AssayUpsertInternal:
        """Upsert a single assay"""

        if not assay.id:
            if not assay.sample_id:
                raise ValueError('Must specify sample_id when inserting an assay')

            project_ids = await self.sampt.get_project_ids_for_sample_ids(
                [assay.sample_id]
            )
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=False
            )

            seq_id = await self.seqt.insert_assay(
                sample_id=assay.sample_id,
                assay_type=assay.type,
                meta=assay.meta,
                external_ids=assay.external_ids,
                open_transaction=open_transaction,
            )
            assay.id = seq_id
        else:
            if check_project_id:
                # can check the project id of the assay we're updating
                project_ids = await self.seqt.get_projects_by_assay_ids([assay.id])
                await self.ptable.check_access_to_project_ids(
                    self.author, project_ids, readonly=False
                )
            # Otherwise update
            await self.seqt.update_assay(
                assay.id,
                meta=assay.meta,
                assay_type=assay.type,
                sample_id=assay.sample_id,
                external_ids=assay.external_ids,
                open_transaction=open_transaction,
            )
        return assay

    async def upsert_assays(
        self,
        assays: list[AssayUpsertInternal],
        check_project_ids: bool = True,
        open_transaction=True,
    ) -> list[AssayUpsertInternal]:
        """Upsert multiple sequences to the given sample (sid)"""

        if check_project_ids:
            sample_ids = set(s.sample_id for s in assays)
            st = SampleTable(self.connection)
            project_ids = await st.get_project_ids_for_sample_ids(list(sample_ids))
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=False
            )

        with_function = (
            self.connection.connection.transaction if open_transaction else NoOpAenter
        )
        async with with_function():
            for a in assays:
                await self.upsert_assay(
                    a, check_project_id=False, open_transaction=False
                )

        return assays

    # endregion UPDATES

    # region UPSERTS

    # endregion UPSERTS
