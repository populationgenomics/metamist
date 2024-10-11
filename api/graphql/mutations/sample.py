# pylint: disable=redefined-builtin, import-outside-toplevel

from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from api.graphql.mutations.assay import AssayUpsertInput
from api.graphql.mutations.sequencing_group import SequencingGroupUpsertInput
from api.graphql.types import SampleUpsertType
from db.python.connect import Connection
from db.python.layers.comment import CommentLayer
from db.python.layers.sample import SampleLayer
from models.models.comment import CommentEntityType
from models.models.sample import SampleUpsert
from models.utils.sample_id_format import (  # Sample,
    sample_id_format,
    sample_id_transform_to_raw,
)

if TYPE_CHECKING:
    from api.graphql.schema import GraphQLComment
    from api.graphql.mutations.project import ProjectMutations


@strawberry.input  # type: ignore [misc]
class SampleUpsertInput:
    """Sample upsert input"""

    id: str | None = None
    external_ids: strawberry.scalars.JSON | None = None
    meta: strawberry.scalars.JSON | None = None
    project: int | None = None
    type: str | None = None
    participant_id: int | None = None
    active: bool | None = None
    sequencing_groups: list[SequencingGroupUpsertInput] | None = None
    non_sequencing_assays: list[AssayUpsertInput] | None = None


@strawberry.type
class SampleMutations:
    """Sample mutations"""

    project_id: strawberry.Private[int]

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
        sample: SampleUpsertInput,
        info: Info[GraphQLContext, 'SampleMutations'],
        root: 'ProjectMutations',
    ) -> str | None:
        """Creates a new sample, and returns the internal sample ID"""
        connection: Connection = info.context['connection']
        st = SampleLayer(connection)

        sample_upsert = SampleUpsert.from_dict(strawberry.asdict(sample))
        internal_sid = await st.upsert_sample(
            sample_upsert.to_internal(), project=root.project_id
        )

        if internal_sid.id:
            return sample_id_format(internal_sid.id)
        return None

    @strawberry.mutation
    async def upsert_samples(
        self,
        samples: list[SampleUpsertInput],
        info: Info[GraphQLContext, 'SampleMutations'],
        root: 'ProjectMutations',
    ) -> list[SampleUpsertType] | None:
        """
        Upserts a list of samples with sequencing-groups,
        and returns the list of internal sample IDs
        """
        connection: Connection = info.context['connection']
        st = SampleLayer(connection)

        internal_samples = [
            SampleUpsert.from_dict(strawberry.asdict(sample)).to_internal()
            for sample in samples
        ]
        upserted = await st.upsert_samples(internal_samples, project=root.project_id)

        return [SampleUpsertType.from_upsert_internal(s) for s in upserted]

    # endregion CREATES

    # region OTHER

    @strawberry.mutation
    async def update_sample(
        self,
        id_: str,
        sample: SampleUpsertInput,
        info: Info[GraphQLContext, 'SampleMutations'],
    ) -> SampleUpsertType:
        """Update sample with id"""
        connection = info.context['connection']
        st = SampleLayer(connection)

        sample.id = id_
        upserted = await st.upsert_sample(
            SampleUpsert.from_dict(strawberry.asdict(sample)).to_internal()
        )
        return SampleUpsertType.from_upsert_internal(upserted)

    # endregion OTHER
