# pylint: disable=redefined-builtin, import-outside-toplevel, ungrouped-imports

from typing import TYPE_CHECKING, Annotated

from graphql import GraphQLError
import strawberry
from strawberry.types import Info
from strawberry.scalars import JSON

from api.graphql.loaders import GraphQLContext
from api.graphql.mutations.analysis import AnalysisMutations
from api.graphql.mutations.analysis_runner import AnalysisRunnerMutations
from api.graphql.mutations.assay import AssayMutations
from api.graphql.mutations.comment import CommentMutations
from api.graphql.mutations.family import FamilyMutations
from api.graphql.mutations.participant import ParticipantMutations
from api.graphql.mutations.sequencing_group import SequencingGroupMutations

from db.python.layers.comment import CommentLayer
from db.python.tables.project import ProjectPermissionsTable
from models.models.project import (
    ProjectMemberRole,
    ProjectMemberUpdate,
    project_member_role_names,
)
from models.models.comment import CommentEntityType
from api.graphql.mutations.sample import SampleMutations

if TYPE_CHECKING:
    from api.graphql.schema import GraphQLComment


@strawberry.input
class ProjectMemberUpdateInput:
    """Project member update input"""

    member: str
    roles: list[str]


@strawberry.type
class ProjectMutations:
    """Project mutations"""

    project_id: strawberry.Private[int]

    @strawberry.mutation
    async def add_comment(
        self,
        content: str,
        id: int,
        info: Info[GraphQLContext, 'ProjectMutations'],
    ) -> Annotated['GraphQLComment', strawberry.lazy('api.graphql.schema')]:
        """Add a comment to a project"""
        # Import needed here to avoid circular import
        from api.graphql.schema import GraphQLComment

        connection = info.context['connection']
        cl = CommentLayer(connection)
        result = await cl.add_comment_to_entity(
            entity=CommentEntityType.project, entity_id=id, content=content
        )
        return GraphQLComment.from_internal(result)

    @strawberry.mutation
    async def create_project(
        self,
        name: str,
        dataset: str,
        create_test_project: bool,
        info: Info,
    ) -> int:
        """
        Create a new project
        """
        connection = info.context['connection']
        ptable = ProjectPermissionsTable(connection)

        await ptable.check_project_creator_permissions(author=connection.author)

        pid = await ptable.create_project(
            project_name=name,
            dataset_name=dataset,
            author=connection.author,
        )

        if create_test_project:
            await ptable.create_project(
                project_name=name + '-test',
                dataset_name=dataset,
                author=connection.author,
            )

        return pid

    @strawberry.mutation
    async def update_project(
        self,
        project: str,
        project_update_model: JSON,
        info: Info,
    ) -> JSON:
        """Update a project by project name"""
        connection = info.context['connection']
        connection.check_access({ProjectMemberRole.project_admin})

        ptable = ProjectPermissionsTable(connection)
        await ptable.update_project(
            project_name=project,
            update=project_update_model,  # type: ignore [arg-type]
            author=connection.author,  # type: ignore [arg-type]
        )

        return JSON({'success': True})

    @strawberry.mutation
    async def delete_project_data(
        self,
        delete_project: bool,
        info: Info,
    ) -> JSON:
        """
        Delete all data in a project by project name.
        """
        connection = info.context['connection']
        connection.check_access({ProjectMemberRole.project_admin})

        ptable = ProjectPermissionsTable(connection)
        assert connection.project

        success = await ptable.delete_project_data(
            project_id=connection.project.id,
            delete_project=delete_project,
        )

        return JSON({'success': success})

    @strawberry.mutation
    async def update_project_members(
        self,
        members: list[ProjectMemberUpdateInput],
        info: Info,
    ) -> JSON:
        """
        Update project members for specific read / write group.
        Not that this is protected by access to a specific access group
        """
        connection = info.context['connection']
        connection.check_access({ProjectMemberRole.project_member_admin})

        ptable = ProjectPermissionsTable(connection)

        for member in members:
            for role in member.roles:
                if role not in project_member_role_names:
                    raise GraphQLError(
                        f'Role {role} is not valid for member {member.member}'
                    )
        member_objs = [
            ProjectMemberUpdate.from_dict(strawberry.asdict(member))
            for member in members
        ]
        assert connection.project
        await ptable.set_project_members(
            project=connection.project, members=member_objs
        )

        return JSON({'success': True})

    # Assays
    @strawberry.field
    def assay(self) -> AssayMutations:
        """Assay mutations"""
        return AssayMutations()

    # Analysis Runner
    @strawberry.field
    def analysis_runner(self, root) -> AnalysisRunnerMutations:
        """Analysis Runner mutations"""
        return AnalysisRunnerMutations(project_id=root.project_id)

    # Analysis
    @strawberry.field
    def analysis(self, root) -> AnalysisMutations:
        """Analysis mutations"""
        return AnalysisMutations(project_id=root.project_id)

    # Comments
    @strawberry.field
    def comment(self) -> CommentMutations:
        """Comment mutations"""
        return CommentMutations()

    # Family
    @strawberry.field
    def family(self) -> FamilyMutations:
        """Family mutations"""
        return FamilyMutations()

    # Participant
    @strawberry.field
    def participant(self) -> ParticipantMutations:
        """Participant mutations"""
        return ParticipantMutations()

    # Samples
    @strawberry.field
    def sample(self, root) -> SampleMutations:
        """Sample mutations"""
        return SampleMutations(project_id=root.project_id)

    # SequencingGroup
    @strawberry.field
    def sequencing_group(self, root) -> SequencingGroupMutations:
        """Sequencing Group mutations"""
        return SequencingGroupMutations(project_id=root.project_id)
