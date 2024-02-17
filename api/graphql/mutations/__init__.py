import strawberry

from api.graphql.mutations.sequencing_groups import SequencingGroupsMutations
from api.graphql.mutations.sample import SampleMutations


@strawberry.type
class Mutation:
    """Mutation class"""

    # Sample
    @strawberry.field
    def sample(self) -> SampleMutations:
        return SampleMutations()

    # Sequencing Groups
    @strawberry.field
    def sequencing_groups(self) -> SequencingGroupsMutations:
        return SequencingGroupsMutations()
