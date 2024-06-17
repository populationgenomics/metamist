from typing import TYPE_CHECKING

import strawberry
from strawberry.types import Info
from typing_extensions import Annotated

from api.graphql.types import SampleUpsertInput, SampleUpsertType
from db.python.layers.sample import SampleLayer
from models.models.sample import SampleUpsertInternal
from models.utils.sample_id_format import sample_id_format, sample_id_transform_to_raw

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

    # endregion CREATES

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

    # endregion OTHER
