# pylint: disable=reimported,import-outside-toplevel,wrong-import-position
import datetime
from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext, LoaderKeys

if TYPE_CHECKING:
    from api.graphql.schema import Query
    from api.graphql.types.project import GraphQLProject


@strawberry.type
class GraphQLProjectGroup:
    """GraphQL type for a project group."""

    id: int
    user_id: int
    group_id: int
    group_name: str
    description: str | None
    created_at: datetime.datetime
    updated_at: datetime.datetime

    project_ids: strawberry.Private[list[int]]

    @staticmethod
    def from_internal(internal) -> 'GraphQLProjectGroup':
        """Convert internal model to GraphQL type."""
        return GraphQLProjectGroup(
            id=internal.id,
            user_id=internal.user_id,
            group_id=internal.group_id,
            group_name=internal.group_name,
            project_ids=internal.project_ids,
            description=internal.description,
            created_at=internal.created_at,
            updated_at=internal.updated_at,
        )

    @strawberry.field
    async def projects(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLProjectGroup'
    ) -> Annotated[
        list['GraphQLProject'], strawberry.lazy('api.graphql.types.project')
    ]:
        """Load projects associated with this project group."""
        from api.graphql.types.project import GraphQLProject

        loader = info.context['loaders'][LoaderKeys.PROJECTS_FOR_IDS]
        projects = await loader.load(root.project_ids)
        return [GraphQLProject.from_internal(p) for p in projects]


from api.graphql.types.project import GraphQLProject
