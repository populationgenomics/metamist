import dataclasses
from typing import TypeVar, Generic, Callable, Any
import strawberry

from db.python.tables.sample import SampleFilter
from db.python.tables.sequencing_group import SequencingGroupFilter
from db.python.utils import GenericFilter, ProjectId
from models.utils.sample_id_format import sample_id_transform_to_raw
from models.utils.sequencing_group_id_format import sequencing_group_id_transform_to_raw

T = TypeVar("T")


@strawberry.input(description="Filter for GraphQL queries")
class GraphQLFilter(Generic[T]):
    eq: T | None = None
    in_: list[T] | None = None
    nin: list[T] | None = None

    def all_values(self):
        v = []
        if self.eq:
            v.append(self.eq)
        if self.in_:
            v.extend(self.in_)
        if self.nin:
            v.extend(self.nin)

        return v

    def to_internal_filter(self, callable: Callable[[T], Any] = None):

        if callable:
            return GenericFilter(
                eq=callable(self.eq) if self.eq else None,
                in_=[callable(i) for i in self.in_] if self.in_ else None,
                nin=[callable(i) for i in self.nin] if self.nin else None,
            )

        return GenericFilter(eq=self.eq, in_=self.in_, nin=self.nin)

GraphQLMetaFilter = strawberry.scalars.JSON

@strawberry.input(description="Simple filter for Samples")
class GraphQLSampleFromProjectSpecificFilter:
    type: GraphQLFilter[str] | None = None
    external_id: GraphQLFilter[str] | None = None
    meta: GraphQLMetaFilter | None = None
    def to_internal(self):
        return SampleFilter(
            type=self.type.to_internal_filter() if self.type else None,
            external_id=self.external_id.to_internal_filter() if self.external_id else None,
            meta=self.meta,
            # project gets set in the loader
            # project=None
        )


@strawberry.input(description="Filter for GraphQL queries")
class GraphQLSampleFilter:
    project: GraphQLFilter[str]
    id: GraphQLFilter[str] | None = None
    type: GraphQLFilter[str] | None = None
    meta: GraphQLMetaFilter | None = None
    external_id: GraphQLFilter[str] | None = None
    participant_id: GraphQLFilter[int] | None = None
    active: GraphQLFilter[bool] | None = dataclasses.field(default_factory=lambda: GraphQLFilter(eq=True))

    def to_internal(self, project_id_map: dict[str, int]):
        return SampleFilter(
            id=self.id.to_internal_filter(sample_id_transform_to_raw) if self.id else None,
            type=self.type.to_internal_filter() if self.type else None,
            meta=self.meta,
            external_id=self.external_id.to_internal_filter() if self.external_id else None,
            participant_id=self.participant_id.to_internal_filter() if self.participant_id else None,
            project=self.project.to_internal_filter(lambda pname: project_id_map[pname]) if self.project else None,
            active=self.active.to_internal_filter() if self.active else None,
        )


@strawberry.input(description="Filter for SequencingGroups in GraphQL")
class GraphQLSequencingGroupNonSpecificFilter:
    type: GraphQLFilter[str] | None = None
    technology: GraphQLFilter[str] | None = None
    platform: GraphQLFilter[str] | None = None
    active_only: GraphQLFilter[bool] | None = dataclasses.field(
        default_factory=lambda: GraphQLFilter(eq=True)
    )

    def to_internal_filter(self):
        return SequencingGroupFilter(
            type=self.type.to_internal_filter() if self.type else None,
            technology=self.technology.to_internal_filter()
            if self.technology
            else None,
            platform=self.platform.to_internal_filter() if self.platform else None,
            active_only=self.active_only.to_internal_filter()
            if self.active_only
            else None,
        )

@strawberry.input(description="Filter for SequencingGroups in GraphQL")
class GraphQLSequencingGroupFilter:
    id: GraphQLFilter[str] | None = None
    project: GraphQLFilter[str] | None = None
    sample_id: GraphQLFilter[str] | None = None
    type: GraphQLFilter[str] | None = None
    technology: GraphQLFilter[str] | None = None
    platform: GraphQLFilter[str] | None = None
    active_only: GraphQLFilter[bool] | None = dataclasses.field(
        default_factory=lambda: GraphQLFilter(eq=True)
    )

    def to_internal_filter(self, project_id_map: dict[str, int]):
        return SequencingGroupFilter(
            project=self.project.to_internal_filter(lambda val: project_id_map[val]) if self.project else None,
            sample_id=self.sample_id.to_internal_filter(sample_id_transform_to_raw)
            if self.sample_id
            else None,
            id=self.id.to_internal_filter(sequencing_group_id_transform_to_raw)
            if self.id
            else None,
            type=self.type.to_internal_filter() if self.type else None,
            technology=self.technology.to_internal_filter()
            if self.technology
            else None,
            platform=self.platform.to_internal_filter() if self.platform else None,
            active_only=self.active_only.to_internal_filter()
            if self.active_only
            else None,
        )
