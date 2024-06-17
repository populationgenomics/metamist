from typing import TYPE_CHECKING

import strawberry
from strawberry.types import Info
from typing_extensions import Annotated

from api.graphql.types import (
    CustomJSON,
    GetSamplesCriteria,
    SampleUpsertInput,
    SampleUpsertType,
)
from db.python.layers.sample import SampleLayer
from db.python.tables.project import ProjectPermissionsTable
from models.models.sample import SampleUpsertInternal
from models.utils.sample_id_format import (
    sample_id_format,
    sample_id_transform_to_raw,
    sample_id_transform_to_raw_list,
)

if TYPE_CHECKING:
    from ..schema import GraphQLSample


@strawberry.type
class SampleMutations:
    """Sample mutations"""

    # region CREATES

    @strawberry.mutation
    async def create_sample(
        self,
        sample: SampleUpsertInput,
        info: Info,
    ) -> str | None:
        """Creates a new sample, and returns the internal sample ID"""
        connection = info.context['connection']
        st = SampleLayer(connection)
        sample_upsert = SampleUpsertInternal.from_dict(strawberry.asdict(sample))
        internal_sid = await st.upsert_sample(sample_upsert)
        if internal_sid.id:
            return sample_id_format(internal_sid.id)
        return None

    @strawberry.mutation
    async def upsert_samples(
        self,
        samples: list[SampleUpsertInput],
        info: Info,
    ) -> list[SampleUpsertType] | None:
        """
        Upserts a list of samples with sequencing-groups,
        and returns the list of internal sample IDs
        """

        # Table interfaces
        connection = info.context['connection']
        st = SampleLayer(connection)

        internal_samples = [
            SampleUpsertInternal.from_dict(strawberry.asdict(sample))
            for sample in samples
        ]
        upserted = await st.upsert_samples(internal_samples)

        return [SampleUpsertType.from_upsert_internal(s) for s in upserted]

    # endregion CREATES

    # region GETS
    # TODO: The gets here should really be queries instead of mutations
    @strawberry.mutation
    async def get_sample_id_map_by_external(
        self,
        external_ids: list[str],
        allow_missing: bool,
        info: Info,
    ) -> CustomJSON:
        """Get map of sample IDs, { [externalId]: internal_sample_id }"""
        connection = info.context['connection']
        st = SampleLayer(connection)
        result = await st.get_sample_id_map_by_external_ids(
            external_ids, allow_missing=(allow_missing or False)
        )
        return CustomJSON({k: sample_id_format(v) for k, v in result.items()})

    @strawberry.mutation
    async def get_sample_id_map_by_internal(
        self,
        internal_ids: list[str],
        info: Info,
    ) -> CustomJSON:
        """
        Get map of sample IDs, { [internal_id]: external_sample_id }
        Without specifying a project, you might see duplicate external identifiers
        """
        # TODO: Reconfigure connection permissions as per `routes``
        connection = info.context['connection']
        st = SampleLayer(connection)
        internal_ids_raw = sample_id_transform_to_raw_list(internal_ids)
        result = await st.get_internal_to_external_sample_id_map(internal_ids_raw)
        return CustomJSON({sample_id_format(k): v for k, v in result.items()})

    @strawberry.mutation
    async def get_all_sample_id_map_by_internal(
        self,
        info: Info,
    ) -> CustomJSON:
        """Get map of ALL sample IDs, { [internal_id]: external_sample_id }"""
        # TODO: Reconfigure connection permissions as per `routes`
        connection = info.context['connection']
        st = SampleLayer(connection)
        assert connection.project
        result = await st.get_all_sample_id_map_by_internal_ids(
            project=connection.project
        )
        return CustomJSON({sample_id_format(k): v for k, v in result.items()})

    @strawberry.mutation
    async def get_samples(
        self,
        criteria: GetSamplesCriteria,
        info: Info,
    ) -> list[Annotated['GraphQLSample', strawberry.lazy('..schema')]]:
        """
        Get list of samples (dict) by some mixture of (AND'd) criteria
        """
        # TODO: Reconfigure connection permissions as per `routes`
        connection = info.context['connection']
        st = SampleLayer(connection)

        pt = ProjectPermissionsTable(connection)
        pids: list[int] | None = None
        if criteria.project_ids:
            pids = await pt.get_project_ids_from_names_and_user(
                connection.author, criteria.project_ids, readonly=True
            )

        sample_ids_raw = (
            sample_id_transform_to_raw_list(criteria.sample_ids)
            if criteria.sample_ids
            else None
        )

        result = await st.get_samples_by(
            sample_ids=sample_ids_raw,
            meta=criteria.meta,
            participant_ids=criteria.participant_ids,
            project_ids=pids,
            active=criteria.active,
            check_project_ids=True,
        )
        #  pylint: disable=no-member
        return [
            Annotated['GraphQLSample', strawberry.lazy('..schema')].from_internal(r)  # type: ignore [attr-defined]
            for r in result
        ]

    @strawberry.mutation
    async def update_sample(
        self,
        id_: str,
        sample: SampleUpsertInput,
        info: Info,
    ) -> SampleUpsertType | None:
        """Update sample with id"""
        # TODO: Reconfigure connection permissions as per `routes`
        connection = info.context['connection']
        st = SampleLayer(connection)
        sample.id = id_
        upserted = await st.upsert_sample(
            SampleUpsertInternal.from_dict(strawberry.asdict(sample))
        )
        return SampleUpsertType.from_upsert_internal(upserted)

    @strawberry.mutation
    async def get_samples_create_date(
        self,
        sample_ids: list[str],
        info: Info,
    ) -> CustomJSON | None:
        """Get full history of sample from internal ID"""
        # TODO: Reconfigure connection permissions as per `routes`
        connection = info.context['connection']
        st = SampleLayer(connection)

        # Check access permissions
        sample_ids_raw = (
            sample_id_transform_to_raw_list(sample_ids) if sample_ids else None
        )

        # Convert to raw ids and query the start dates for all of them
        if sample_ids_raw:
            result = await st.get_samples_create_date(sample_ids_raw)

            return CustomJSON({sample_id_format(k): v for k, v in result.items()})
        return None

    # endregion GETS

    # region OTHER

    @strawberry.mutation
    async def merge_samples(
        self,
        id_keep: str,
        id_merge: str,
        info: Info,
    ) -> Annotated['GraphQLSample', strawberry.lazy('..schema')]:
        """
        Merge one sample into another, this function achieves the merge
        by rewriting all sample_ids of {id_merge} with {id_keep}. You must
        carefully consider if analysis objects need to be deleted, or other
        implications BEFORE running this method.
        """
        connection = info.context['connection']
        st = SampleLayer(connection)
        result = await st.merge_samples(
            id_keep=sample_id_transform_to_raw(id_keep),
            id_merge=sample_id_transform_to_raw(id_merge),
        )
        #  pylint: disable=no-member
        return Annotated['GraphQLSample', strawberry.lazy('..schema')].from_internal(result)  # type: ignore [attr-defined]

    @strawberry.mutation
    async def get_history_of_sample(
        self, id_: str, info: Info
    ) -> list[Annotated['GraphQLSample', strawberry.lazy('..schema')]]:
        """Get full history of sample from internal ID"""
        connection = info.context['connection']
        st = SampleLayer(connection)
        internal_id = sample_id_transform_to_raw(id_)
        result = await st.get_history_of_sample(internal_id)

        #  pylint: disable=no-member
        return Annotated['GraphQLSample', strawberry.lazy('..schema')].from_internal(result)  # type: ignore [attr-defined]

    # endregion OTHER
