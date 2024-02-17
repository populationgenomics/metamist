import strawberry

from api.graphql.types import SampleUpsertInput, SampleUpsertType
from db.python.layers.sample import SampleLayer
from db.python.tables.project import ProjectPermissionsTable
from models.utils.sample_id_format import (  # Sample,
    sample_id_format,
    sample_id_transform_to_raw,
    sample_id_transform_to_raw_list,
)


@strawberry.type
class SampleMutations:
    @strawberry.mutation
    async def create_sample(
        sample: SampleUpsertInput, info: strawberry.types.Info
    ) -> str:
        """Creates a new sample, and returns the internal sample ID"""
        connection = info.context['connection']
        st = SampleLayer(connection)
        internal_sid = await st.upsert_sample(sample.to_internal())
        return sample_id_format(internal_sid.id)

    @strawberry.mutation
    async def upsert_samples(
        samples: list[SampleUpsertInput],
        info: strawberry.types.Info,
    ) -> list[SampleUpsertType]:
        """
        Upserts a list of samples with sequencing-groups,
        and returns the list of internal sample IDs
        """

        # Table interfaces
        connection = info.context['connection']
        st = SampleLayer(connection)

        internal_samples = [sample.to_internal() for sample in samples]
        upserted = await st.upsert_samples(internal_samples)

        return [s.to_external() for s in upserted]
