import datetime
from typing import Callable, Generic, TypeVar

import strawberry

from db.python.utils import GenericFilter, GenericMetaFilter
from models.enums.analysis import AnalysisStatus

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
        v: list[T] = []
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


# The below concrete types are specified individually because there is a performance
# issue in strawberry graphql where the usage of generics in input types causes major
# slowdowns.
# @see https://github.com/strawberry-graphql/strawberry/issues/3544
@strawberry.input(description='String filter for GraphQL queries')
class GraphQLFilterStr(GraphQLFilter[str]):
    """String filter for GraphQL queries"""

    eq: str | None = None
    in_: list[str] | None = None
    nin: list[str] | None = None
    gt: str | None = None
    gte: str | None = None
    lt: str | None = None
    lte: str | None = None
    contains: str | None = None
    icontains: str | None = None


@strawberry.input(description='Int filter for GraphQL queries')
class GraphQLFilterInt(GraphQLFilter[int]):
    """Int filter for GraphQL queries"""

    eq: int | None = None
    in_: list[int] | None = None
    nin: list[int] | None = None
    gt: int | None = None
    gte: int | None = None
    lt: int | None = None
    lte: int | None = None
    contains: int | None = None
    icontains: int | None = None


@strawberry.input(description='Bool filter for GraphQL queries')
class GraphQLFilterBool(GraphQLFilter[bool]):
    """Bool filter for GraphQL queries"""

    eq: bool | None = None
    in_: list[bool] | None = None
    nin: list[bool] | None = None
    gt: bool | None = None
    gte: bool | None = None
    lt: bool | None = None
    lte: bool | None = None
    contains: bool | None = None
    icontains: bool | None = None


GraphQLAnalysisStatus = strawberry.enum(AnalysisStatus)


@strawberry.input(description='Analysis status filter for GraphQL queries')
class GraphQLFilterAnalysisStatus(GraphQLFilter[GraphQLAnalysisStatus]):
    """Analysis status filter for GraphQL queries"""

    eq: AnalysisStatus | None = None
    in_: list[AnalysisStatus] | None = None
    nin: list[AnalysisStatus] | None = None
    gt: AnalysisStatus | None = None
    gte: AnalysisStatus | None = None
    lt: AnalysisStatus | None = None
    lte: AnalysisStatus | None = None
    contains: AnalysisStatus | None = None
    icontains: AnalysisStatus | None = None


@strawberry.input(description='Datetime filter for GraphQL queries')
class GraphQLFilterDatetime(GraphQLFilter[datetime.datetime]):
    """Datetime filter for GraphQL queries"""

    eq: datetime.datetime | None = None
    in_: list[datetime.datetime] | None = None
    nin: list[datetime.datetime] | None = None
    gt: datetime.datetime | None = None
    gte: datetime.datetime | None = None
    lt: datetime.datetime | None = None
    lte: datetime.datetime | None = None
    contains: datetime.datetime | None = None
    icontains: datetime.datetime | None = None


@strawberry.input(description='Date filter for GraphQL queries')
class GraphQLFilterDate(GraphQLFilter[datetime.date]):
    """Date filter for GraphQL queries"""

    eq: datetime.date | None = None
    in_: list[datetime.date] | None = None
    nin: list[datetime.date] | None = None
    gt: datetime.date | None = None
    gte: datetime.date | None = None
    lt: datetime.date | None = None
    lte: datetime.date | None = None
    contains: datetime.date | None = None
    icontains: datetime.date | None = None


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
