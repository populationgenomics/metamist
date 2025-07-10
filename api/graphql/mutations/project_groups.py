from typing import Optional

import strawberry

from api.graphql.types.project_groups import GraphQLProjectGroup
from db.python.layers.project_groups import ProjectGroupsLayer


@strawberry.type
class ProjectGroupsMutations:
    """GraphQL mutations for managing project groups."""

    @strawberry.mutation
    async def create_project_group(
        self,
        info,
        user_id: int,
        group_id: int,
        group_name: str,
        description: Optional[str] = None,
    ) -> GraphQLProjectGroup:
        """Create a new project group with the specified parameters."""
        connection = info.context['connection']
        layer = ProjectGroupsLayer(connection)
        group = await layer.create(user_id, group_id, group_name, description)
        return GraphQLProjectGroup.from_internal(group)
