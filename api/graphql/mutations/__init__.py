import strawberry
from strawberry.types import Info


from api.graphql.loaders import GraphQLContext
from api.graphql.mutations.cohort import CohortMutations
from api.graphql.mutations.comment import CommentMutations
from api.graphql.mutations.project import ProjectMutations
from api.graphql.mutations.analysis import AnalysisMutations
from api.graphql.mutations.analysis_runner import AnalysisRunnerMutations
from api.graphql.mutations.assay import AssayMutations
from api.graphql.mutations.family import FamilyMutations
from api.graphql.mutations.participant import ParticipantMutations
from api.graphql.mutations.sequencing_group import SequencingGroupMutations
from api.graphql.mutations.sample import SampleMutations

from models.models.project import FullWriteAccessRoles, ReadAccessRoles


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

    # Comments
    @strawberry.field
    def comment(self) -> CommentMutations:
        """Comment mutations"""
        return CommentMutations()

    # Assays
    @strawberry.field
    def assay(self) -> AssayMutations:
        """Assay mutations"""
        return AssayMutations()

    # Analysis Runner
    @strawberry.field
    def analysis_runner(
        self, project_name: str, info: Info[GraphQLContext, 'Mutation']
    ) -> AnalysisRunnerMutations:
        """Analysis Runner mutations"""
        connection = info.context['connection']
        projects = connection.get_and_check_access_to_projects_for_names(
            project_names=[project_name], allowed_roles=FullWriteAccessRoles
        )
        project_id = next(p for p in projects).id
        return AnalysisRunnerMutations(project_id=project_id)

    # Analysis
    @strawberry.field
    def analysis(
        self, project_name: str, info: Info[GraphQLContext, 'Mutation']
    ) -> AnalysisMutations:
        """Analysis mutations"""
        connection = info.context['connection']
        projects = connection.get_and_check_access_to_projects_for_names(
            project_names=[project_name], allowed_roles=FullWriteAccessRoles
        )
        project_id = next(p for p in projects).id
        return AnalysisMutations(project_id=project_id)

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
    def sample(
        self, project_name: str, info: Info[GraphQLContext, 'Mutation']
    ) -> SampleMutations:
        """Sample mutations"""
        connection = info.context['connection']
        projects = connection.get_and_check_access_to_projects_for_names(
            project_names=[project_name], allowed_roles=FullWriteAccessRoles
        )
        project_id = next(p for p in projects).id
        return SampleMutations(project_id=project_id)

    # SequencingGroup
    @strawberry.field
    def sequencing_group(
        self, project_name: str, info: Info[GraphQLContext, 'Mutation']
    ) -> SequencingGroupMutations:
        """Sequencing Group mutations"""
        connection = info.context['connection']
        projects = connection.get_and_check_access_to_projects_for_names(
            project_names=[project_name], allowed_roles=FullWriteAccessRoles
        )
        project_id = next(p for p in projects).id
        return SequencingGroupMutations(project_id=project_id)

    # Cohort
    @strawberry.field()
    def cohort(self) -> CohortMutations:
        """Cohort mutations"""
        return CohortMutations()
