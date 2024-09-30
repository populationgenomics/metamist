# pylint: disable=redefined-builtin, import-outside-toplevel

from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from api.graphql.types import SampleUpsertType
from db.python.layers.comment import CommentLayer
from db.python.layers.sample import SampleLayer
from models.models.comment import CommentEntityType
from models.models.sample import SampleUpsert, SampleUpsertInternal
from models.utils.sample_id_format import (  # Sample,
    sample_id_format,
    sample_id_transform_to_raw,
)

if TYPE_CHECKING:
    from api.graphql.schema import GraphQLComment, GraphQLSample


@strawberry.type
class SampleMutations:
    """Sample mutations"""

    @strawberry.mutation
    async def add_comment(
        self,
        content: str,
        id: str,
        info: Info[GraphQLContext, 'SampleMutations'],
    ) -> Annotated['GraphQLComment', strawberry.lazy('api.graphql.schema')]:
        """Add a comment to a sample"""
        from api.graphql.schema import GraphQLComment

        connection = info.context['connection']
        cl = CommentLayer(connection)
        result = await cl.add_comment_to_entity(
            entity=CommentEntityType.sample,
            entity_id=sample_id_transform_to_raw(id),
            content=content,
        )
        return GraphQLComment.from_internal(result)

    # region CREATES

    @strawberry.mutation
    async def create_sample(
        self,
        sample: SampleUpsert,
        info: Info[GraphQLContext, 'SampleMutations'],
    ) -> str | None:
        """Creates a new sample, and returns the internal sample ID"""
        connection = info.context['connection']
        st = SampleLayer(connection)
        internal_sid = await st.upsert_sample(sample.to_internal())
        if internal_sid.id:
            return sample_id_format(internal_sid.id)
        return None

    @strawberry.mutation
    async def upsert_samples(
        self,
        samples: list[SampleUpsert],
        info: Info[GraphQLContext, 'SampleMutations'],
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

    # region OTHER

    @strawberry.mutation
    async def update_sample(
        self,
        id_: str,
        sample: SampleUpsert,
        info: Info[GraphQLContext, 'SampleMutations'],
    ) -> SampleUpsertType:
        """Update sample with id"""
        connection = info.context['connection']
        st = SampleLayer(connection)
        sample.id = id_
        await st.upsert_sample(sample.to_internal())
        return SampleUpsertType.from_upsert_internal(sample.to_internal())

    @strawberry.mutation
    async def merge_samples(
        self,
        id_keep: str,
        id_merge: str,
        info: Info[GraphQLContext, 'SampleMutations'],
    ) -> Annotated['GraphQLSample', strawberry.lazy('api.graphql.schema')]:
        """
        Merge one sample into another, this function achieves the merge
        by rewriting all sample_ids of {id_merge} with {id_keep}. You must
        carefully consider if analysis objects need to be deleted, or other
        implications BEFORE running this method.
        """
        from api.graphql.schema import GraphQLSample

        connection = info.context['connection']
        st = SampleLayer(connection)
        result = await st.merge_samples(
            id_keep=sample_id_transform_to_raw(id_keep),
            id_merge=sample_id_transform_to_raw(id_merge),
        )
        return GraphQLSample.from_internal(result)

    # endregion OTHER
