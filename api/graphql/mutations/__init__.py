import strawberry

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
        """Sample mutations"""
        return AssayMutations()

    # Participant
    @strawberry.field
    def participant(self) -> ParticipantMutations:
        """Sample mutations"""
        return ParticipantMutations()

    # Family
    @strawberry.field
    def family(self) -> FamilyMutations:
        """Sample mutations"""
        return FamilyMutations()

    # Project
    @strawberry.field
    def project(self) -> ProjectMutations:
        """Sample mutations"""
        return ProjectMutations()

    # SequencingGroup
    @strawberry.field
    def sequencing_group(self) -> SequencingGroupMutations:
        """Sample mutations"""
        return SequencingGroupMutations()
