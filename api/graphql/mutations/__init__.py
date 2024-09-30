import strawberry

from api.graphql.mutations.analysis import AnalysisMutations
from api.graphql.mutations.analysis_runner import AnalysisRunnerMutations
from api.graphql.mutations.assay import AssayMutations
from api.graphql.mutations.comment import CommentMutations
from api.graphql.mutations.family import FamilyMutations
from api.graphql.mutations.participant import ParticipantMutations
from api.graphql.mutations.project import ProjectMutations
from api.graphql.mutations.sample import SampleMutations
from api.graphql.mutations.sequencing_group import SequencingGroupMutations


@strawberry.type
class Mutation:
    """Mutation class"""

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

    # Comments
    @strawberry.field
    def comment(self) -> CommentMutations:
        """Comment mutations"""
        return CommentMutations()

    # Samples
    @strawberry.field
    def sample(self) -> SampleMutations:
        """Sample mutations"""
        return SampleMutations()

    # Assays
    @strawberry.field
    def assay(self) -> AssayMutations:
        """Assay mutations"""
        return AssayMutations()

    # Participant
    @strawberry.field
    def participant(self) -> ParticipantMutations:
        """Participant mutations"""
        return ParticipantMutations()

    # Family
    @strawberry.field
    def family(self) -> FamilyMutations:
        """Family mutations"""
        return FamilyMutations()

    # Project
    @strawberry.field
    def project(self) -> ProjectMutations:
        """Project mutations"""
        return ProjectMutations()

    # SequencingGroup
    @strawberry.field
    def sequencing_group(self) -> SequencingGroupMutations:
        """Sequencing Group mutations"""
        return SequencingGroupMutations()
