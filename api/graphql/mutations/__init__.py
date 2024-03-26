import strawberry

from api.graphql.mutations.sample import SampleMutations
from api.graphql.mutations.sequencing_groups import SequencingGroupsMutations


@strawberry.type
class Mutation:
    """Mutation class"""

    # Sample
    @strawberry.field
    def sample(self) -> SampleMutations:
        """Sample mutations"""
        return SampleMutations()

    # Sequencing Groups
    @strawberry.field
    def sequencing_groups(self) -> SequencingGroupsMutations:
        """Sequencing group mutations"""
        return SequencingGroupsMutations()
