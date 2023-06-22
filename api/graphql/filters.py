from typing import TypeVar, Generic, Callable, Any

import strawberry

from db.python.utils import GenericFilter

T = TypeVar('T')


@strawberry.input(description='Filter for GraphQL queries')
class GraphQLFilter(Generic[T]):
    """EXTERNAL Filter for GraphQL queries"""

    eq: T | None = None
    in_: list[T] | None = None
    nin: list[T] | None = None

    def all_values(self):
        """
        Get all values used anywhere in a filter, useful for getting values to map later
        """
        v = []
        if self.eq:
            v.append(self.eq)
        if self.in_:
            v.extend(self.in_)
        if self.nin:
            v.extend(self.nin)

        return v

    def to_internal_filter(self, f: Callable[[T], Any] = None):
        """Convert from GraphQL to internal filter model"""

        if f:
            return GenericFilter(
                eq=f(self.eq) if self.eq else None,
                in_=list(map(f, self.in_)) if self.in_ else None,
                nin=list(map(f, self.nin)) if self.nin else None,
            )

        return GenericFilter(eq=self.eq, in_=self.in_, nin=self.nin)


GraphQLMetaFilter = strawberry.scalars.JSON
