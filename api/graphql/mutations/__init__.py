import strawberry

from api.graphql.mutations.comment import CommentMutations
from api.graphql.mutations.sample import SampleMutations


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
