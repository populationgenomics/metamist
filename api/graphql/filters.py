from typing import Any, Callable, Generic, TypeVar

import strawberry

from db.python.utils import GenericFilter

T = TypeVar('T')


@strawberry.input(description='Filter for GraphQL queries')
class GraphQLFilter(Generic[T]):
    """EXTERNAL Filter for GraphQL queries"""

    eq: T | None = None
    in_: list[T] | None = None
    nin: list[T] | None = None
    gt: T | None = None
    gte: T | None = None
    lt: T | None = None
    lte: T | None = None

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
        if self.gt:
            v.append(self.gt)
        if self.gte:
            v.append(self.gte)
        if self.lt:
            v.append(self.lt)
        if self.lte:
            v.append(self.lte)

        return v

    def to_internal_filter(self, f: Callable[[T], Any] = None):
        """Convert from GraphQL to internal filter model"""

        if f:
            return GenericFilter(
                eq=f(self.eq) if self.eq else None,
                in_=list(map(f, self.in_)) if self.in_ else None,
                nin=list(map(f, self.nin)) if self.nin else None,
                gt=f(self.gt) if self.gt else None,
                gte=f(self.gte) if self.gte else None,
                lt=f(self.lt) if self.lt else None,
                lte=f(self.lte) if self.lte else None,
            )

        return GenericFilter(
            eq=self.eq,
            in_=self.in_,
            nin=self.nin,
            gt=self.gt,
            gte=self.gte,
            lt=self.lt,
            lte=self.lte,
        )


GraphQLMetaFilter = strawberry.scalars.JSON
