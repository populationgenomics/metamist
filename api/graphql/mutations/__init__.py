import strawberry
from strawberry.types import Info


from api.graphql.loaders import GraphQLContext
from api.graphql.mutations.project import ProjectMutations
from models.models.project import FullWriteAccessRoles


@strawberry.type
class Mutation:
    """Mutation class"""

    # Project
    @strawberry.field
    def project(
        self, name: str, info: Info[GraphQLContext, 'Mutation']
    ) -> ProjectMutations:
        """Project mutations"""
        connection = info.context['connection']
        projects = connection.get_and_check_access_to_projects_for_names(
            project_names=[name], allowed_roles=FullWriteAccessRoles
        )
        project_id = next(p for p in projects).id
        return ProjectMutations(project_id=project_id)
