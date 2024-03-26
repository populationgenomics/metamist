import strawberry

from api.graphql.types import SampleUpsertInput, SampleUpsertType
from db.python.layers.sample import SampleLayer
from models.models.sample import SampleInternal
from models.utils.sample_id_format import sample_id_format


@strawberry.type
class SampleMutations:
    """Sample mutations"""

    @strawberry.mutation
    async def create_sample(
        self,
        sample: SampleUpsertInput,
        info: strawberry.types.Info,
    ) -> str:
        """Creates a new sample, and returns the internal sample ID"""
        connection = info.context['connection']
        st = SampleLayer(connection)
        sample_internal = SampleInternal.from_db(strawberry.asdict(sample))
        internal_sid = await st.upsert_sample(sample_internal)
        return sample_id_format(internal_sid.id)

    @strawberry.mutation
    async def upsert_samples(
        self,
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

        internal_samples = [SampleInternal.from_db(strawberry.asdict(sample)) for sample in samples]
        upserted = await st.upsert_samples(internal_samples)

        return [s.to_external() for s in upserted]
