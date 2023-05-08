import asyncio
from typing import List, Optional

from db.python.connect import NotFoundError
from db.python.layers.base import BaseLayer, Connection
from db.python.tables.family import FamilyTable
from db.python.tables.participant import ParticipantTable
from db.python.tables.project import ProjectPermissionsTable
from db.python.tables.sample import SampleTable
from db.python.tables.sequencing_group import SequencingGroupTable
from models.enums.search import SearchResponseType
from models.models.sample import (
    sample_id_format,
    sample_id_transform_to_raw,
)
from models.models.search import (
    SearchResponse,
    SampleSearchResponseData,
    ParticipantSearchResponseData,
    FamilySearchResponseData,
)


class SearchLayer(BaseLayer):
    """Layer for search logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.pt = ProjectPermissionsTable(connection.connection)
        self.connection = connection

    @staticmethod
    def try_get_sample_id_from_query(query: str) -> Optional[int]:
        """
        Try to get internal CPG sample Identifier from string,
        otherwise return None (helper to catch exception)"""
        try:
            return sample_id_transform_to_raw(query, strict=False)
        except ValueError:
            return None

    async def _get_search_result_for_sample(
        self, sample_id: int, project_ids: list[int]
    ) -> SearchResponse | None:
        stable = SampleTable(self.connection)
        ptable = ParticipantTable(self.connection)
        ftable = FamilyTable(self.connection)

        cpg_id = sample_id_format(sample_id)

        try:
            project, sample = await stable.get_sample_by_id(sample_id)
        except NotFoundError:
            return None

        sample_eids = [sample.external_id]
        participant_id = int(sample.participant_id) if sample.participant_id else None
        participant_eids = []
        family_eids = []

        title = cpg_id
        id_field = cpg_id
        if project not in project_ids:
            # or should it maybe say "no-access" or something
            title = f'{cpg_id} (no access to project)'
            id_field = None

        if participant_id:
            p_eids_map, f_eids_map = await asyncio.gather(
                ptable.get_external_ids_by_participant([participant_id]),
                ftable.get_family_external_ids_by_participant_ids([participant_id]),
            )
            if participant_id in p_eids_map:
                participant_eids = p_eids_map.get(participant_id) or []
            if participant_id in f_eids_map:
                family_eids = f_eids_map.get(participant_id) or []

        return SearchResponse(
            title=title,
            type=SearchResponseType.SAMPLE,
            data=SampleSearchResponseData(
                project=project,
                id=id_field,
                sample_external_ids=sample_eids,
                family_external_ids=family_eids,
                participant_external_ids=participant_eids,
            ),
        )

    async def search(self, query: str, project_ids: list[int]) -> List[SearchResponse]:
        """
        Search metamist for some string, get some set of SearchResponses
        """
        # this is the only place where a sample ID can get it
        if not query:
            return []

        query = query.strip()

        if cpg_sample_id := self.try_get_sample_id_from_query(query):
            # just get the sample
            response = await self._get_search_result_for_sample(
                cpg_sample_id, project_ids=project_ids
            )

            return [response] if response else []

        ftable = FamilyTable(self.connection)
        ptable = ParticipantTable(self.connection)
        stable = SampleTable(self.connection)
        sgtable = SequencingGroupTable(self.connection)

        sample_rows, participant_rows, family_rows, sg_rows = await asyncio.gather(
            stable.search(query, project_ids=project_ids, limit=5),
            ptable.search(query, project_ids=project_ids, limit=5),
            ftable.search(query, project_ids=project_ids, limit=5),
            sgtable.search(query, project_ids=project_ids, limit=5),
        )
        print(sg_rows)

        sample_participant_ids = [s[2] for s in sample_rows]
        all_participant_ids = list(
            set(sample_participant_ids + [p[1] for p in participant_rows])
        )

        sample_participant_eids, participant_family_eids = await asyncio.gather(
            ptable.get_external_ids_by_participant(sample_participant_ids),
            ftable.get_family_external_ids_by_participant_ids(all_participant_ids),
        )

        samples = [
            SearchResponse(
                title=sample_id_format(s_id),
                type=SearchResponseType.SAMPLE,
                data=SampleSearchResponseData(
                    project=project,
                    id=sample_id_format(s_id),
                    family_external_ids=participant_family_eids.get(p_id) or []
                    if p_id
                    else [],
                    participant_external_ids=sample_participant_eids.get(p_id) or []
                    if p_id
                    else [],
                    sample_external_ids=[s_eid],
                ),
            )
            for project, s_id, p_id, s_eid in sample_rows
        ]

        participants = [
            SearchResponse(
                title=p_eid,
                type=SearchResponseType.PARTICIPANT,
                data=ParticipantSearchResponseData(
                    project=project,
                    id=p_id,
                    family_external_ids=participant_family_eids.get(p_id) or [],
                    participant_external_ids=[p_eid],
                ),
            )
            for project, p_id, p_eid in participant_rows
        ]

        families = [
            SearchResponse(
                title=f_eid,
                type=SearchResponseType.FAMILY,
                data=FamilySearchResponseData(
                    project=project,
                    id=f_id,
                    family_external_ids=[f_eid],
                ),
            )
            for project, f_id, f_eid in family_rows
        ]

        return [*families, *samples, *participants]
