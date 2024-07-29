import strawberry

from api.graphql.mutations.comment import CommentMutations


@strawberry.type
class Mutation:
    """Mutation class"""

    # Comments
    @strawberry.field
    def comment(self) -> CommentMutations:
        """Comment mutations"""
        return CommentMutations()
