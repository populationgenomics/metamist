from typing import Annotated

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from api.graphql.mutations.assay import AssayUpsertInput
from api.graphql.mutations.sequencing_group import SequencingGroupUpsertInput
from api.graphql.types.comments import GraphQLComment
from api.graphql.types.sample import GraphQLSample
from db.python.connect import Connection
from db.python.layers.comment import CommentLayer
from db.python.layers.sample import SampleLayer
from models.models.comment import CommentEntityType
from models.models.project import FullWriteAccessRoles
from models.models.sample import SampleUpsert
from models.utils.sample_id_format import (  # Sample,
    sample_id_transform_to_raw,
)


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

    @strawberry.mutation
    async def add_comment(
        self,
        content: str,
        sample_id: str,
        info: Info[GraphQLContext, 'SampleMutations'],
    ) -> Annotated[GraphQLComment, strawberry.lazy('api.graphql.schema')]:
        """Add a comment to a sample"""
        connection = info.context['connection']
        cl = CommentLayer(connection)
        result = await cl.add_comment_to_entity(
            entity=CommentEntityType.sample,
            entity_id=sample_id_transform_to_raw(sample_id),
            content=content,
        )
        return GraphQLComment.from_internal(result)

    # region CREATES

    @strawberry.mutation
    async def create_sample(
        self,
        project: str,
        sample: SampleUpsertInput,
        info: Info[GraphQLContext, 'SampleMutations'],
    ) -> Annotated[GraphQLSample, strawberry.lazy('api.graphql.schema')]:
        """Creates a new sample, and returns the internal sample ID"""
        connection: Connection = info.context['connection']
        (target_project,) = connection.get_and_check_access_to_projects_for_names(
            [project], FullWriteAccessRoles
        )
        slayer = SampleLayer(connection)

        sample_upsert = SampleUpsert.from_dict(strawberry.asdict(sample))
        internal_sid = await slayer.upsert_sample(
            sample_upsert.to_internal(), project=target_project.id
        )
        created_sample = await slayer.get_sample_by_id(internal_sid.id)  # type: ignore [arg-type]

        return GraphQLSample.from_internal(created_sample)

    @strawberry.mutation
    async def upsert_samples(
        self,
        project: str,
        samples: list[SampleUpsertInput],
        info: Info[GraphQLContext, 'SampleMutations'],
    ) -> list[Annotated[GraphQLSample, strawberry.lazy('api.graphql.schema')]] | None:
        """
        Upserts a list of samples with sequencing-groups,
        and returns the list of internal sample IDs
        """
        connection: Connection = info.context['connection']
        (target_project,) = connection.get_and_check_access_to_projects_for_names(
            [project], FullWriteAccessRoles
        )
        slayer = SampleLayer(connection)

        internal_samples = [
            SampleUpsert.from_dict(strawberry.asdict(sample)).to_internal()
            for sample in samples
        ]
        upserted = await slayer.upsert_samples(
            internal_samples, project=target_project.id
        )
        upserted_samples = await slayer.get_samples_by(
            sample_ids=[s.id for s in upserted]  # type: ignore [arg-type]
        )

        return [GraphQLSample.from_internal(s) for s in upserted_samples]

    # endregion CREATES

    # region OTHER

    @strawberry.mutation
    async def update_sample(
        self,
        sample: SampleUpsertInput,
        info: Info[GraphQLContext, 'SampleMutations'],
    ) -> Annotated[GraphQLSample, strawberry.lazy('api.graphql.schema')]:
        """Update sample with id"""
        connection = info.context['connection']
        slayer = SampleLayer(connection)
        upserted = await slayer.upsert_sample(
            SampleUpsert.from_dict(strawberry.asdict(sample)).to_internal()
        )
        updated_sample = await slayer.get_sample_by_id(upserted.id)  # type: ignore [arg-type]

        return GraphQLSample.from_internal(updated_sample)

    # endregion OTHER
