from typing import Callable, Generic, TypeVar

import strawberry

from db.python.utils import GenericFilter, GenericMetaFilter

T = TypeVar('T')
Y = TypeVar('Y')


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
    contains: T | None = None
    icontains: T | None = None

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
        if self.contains:
            v.append(self.contains)
        if self.icontains:
            v.append(self.icontains)

        return v

    def to_internal_filter(self) -> GenericFilter[T]:
        """Convert from GraphQL to internal filter model"""
        return GenericFilter(
            eq=self.eq,
            in_=self.in_,
            nin=self.nin,
            gt=self.gt,
            gte=self.gte,
            lt=self.lt,
            lte=self.lte,
            contains=self.contains,
            icontains=self.icontains,
        )

    def to_internal_filter_mapped(self, f: Callable[[T], Y]) -> GenericFilter[Y]:
        """
        To internal filter, but apply a function to all values.
        Separate this into a separate function to please linters and type checkers
        """
        return GenericFilter(
            eq=f(self.eq) if self.eq else None,
            in_=list(map(f, self.in_)) if self.in_ else None,
            nin=list(map(f, self.nin)) if self.nin else None,
            gt=f(self.gt) if self.gt else None,
            gte=f(self.gte) if self.gte else None,
            lt=f(self.lt) if self.lt else None,
            lte=f(self.lte) if self.lte else None,
            contains=f(self.contains) if self.contains else None,
            icontains=f(self.icontains) if self.icontains else None,
        )


GraphQLMetaFilter = strawberry.scalars.JSON


def graphql_meta_filter_to_internal_filter(
    f: GraphQLMetaFilter | None,
) -> GenericMetaFilter | None:
    """Convert from GraphQL to internal filter model

    Args:
        f (GraphQLMetaFilter | None): GraphQL filter

    Returns:
        GenericMetaFilter | None: internal filter
    """
    if not f:
        return None

    d: GenericMetaFilter = {}
    f_to_d: dict[str, GraphQLMetaFilter] = dict(f)  # type: ignore
    for k, v in f_to_d.items():
        d[k] = GenericFilter(**v) if isinstance(v, dict) else GenericFilter(eq=v)
    return d
