import strawberry
from strawberry.types import Info

from api.graphql.mutations.analysis import AnalysisMutations
from api.graphql.mutations.analysis_runner import AnalysisRunnerMutations
from api.graphql.mutations.assay import AssayMutations
from api.graphql.mutations.cohort import CohortMutations
from api.graphql.mutations.comment import CommentMutations
from api.graphql.mutations.family import FamilyMutations
from api.graphql.mutations.participant import ParticipantMutations
from api.graphql.mutations.project import ProjectMutations
from api.graphql.mutations.project_groups import ProjectGroupsMutations
from api.graphql.mutations.sample import SampleMutations
from api.graphql.mutations.sequencing_group import SequencingGroupMutations
from api.graphql.mutations.user import UserMutations


@strawberry.type
class Mutation:
    """Mutation class"""

    # Project
    @strawberry.field
    def project(self) -> ProjectMutations:
        """Project mutations"""
        return ProjectMutations()

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
    def analysis_runner(self) -> AnalysisRunnerMutations:
        """Analysis Runner mutations"""
        return AnalysisRunnerMutations()

    # Analysis
    @strawberry.field
    def analysis(self) -> AnalysisMutations:
        """Analysis mutations"""
        return AnalysisMutations()

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
    def sample(self) -> SampleMutations:
        """Sample mutations"""
        return SampleMutations()

    # SequencingGroup
    @strawberry.field
    def sequencing_group(self) -> SequencingGroupMutations:
        """Sequencing Group mutations"""
        return SequencingGroupMutations()

    # Cohort
    @strawberry.field()
    def cohort(self) -> CohortMutations:
        """Cohort mutations"""
        return CohortMutations()

    # Users
    @strawberry.field()
    def user(self) -> UserMutations:
        """User mutations"""
        return UserMutations()

    # Project Groups
    @strawberry.field
    def project_group(self) -> ProjectGroupsMutations:
        """Project group mutations"""
        return ProjectGroupsMutations()
