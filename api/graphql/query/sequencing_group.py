from typing import TYPE_CHECKING, Annotated, Self

import strawberry

from api.graphql.context import GraphQLContext
from api.graphql.filters import (
    GraphQLFilter,
    GraphQLMetaFilter,
    graphql_meta_filter_to_internal_filter,
)
from api.graphql.query.analyses_loaders import AnalysisLoaderKeys
from api.graphql.query.analysis import GraphQLAnalysis, GraphQLAnalysisStatus
from api.graphql.query.assay_loaders import AssayLoaderKeys
from api.graphql.query.comment_loaders import CommentLoaderKeys
from api.graphql.query.sample_loaders import SampleLoaderKeys
from db.python.filters.generic import GenericFilter
from db.python.tables.analysis import AnalysisFilter
from models.models.project import (
    ProjectId,
    ReadAccessRoles,
)
from models.models.sequencing_group import SequencingGroupInternal
from models.utils import sequencing_group_id_format

if TYPE_CHECKING:
    from .assay import GraphQLAssay
    from .comment import GraphQLDiscussion
    from .sample import GraphQLSample


@strawberry.type
class GraphQLSequencingGroup:
    """SequencingGroup GraphQL model"""

    id: str
    type: str
    technology: str
    platform: str
    meta: strawberry.scalars.JSON
    external_ids: strawberry.scalars.JSON
    archived: bool | None

    internal_id: strawberry.Private[int]
    sample_id: strawberry.Private[int]

    @staticmethod
    def from_internal(
        internal: SequencingGroupInternal,
    ) -> 'GraphQLSequencingGroup':
        if not internal.id:
            raise ValueError('SequencingGroup must have an id')

        return GraphQLSequencingGroup(
            id=sequencing_group_id_format(internal.id),
            type=internal.type,
            technology=internal.technology,
            platform=internal.platform,
            meta=internal.meta,
            external_ids=internal.external_ids or {},
            archived=internal.archived,
            # internal
            internal_id=internal.id,
            sample_id=internal.sample_id,
        )

    @strawberry.field
    async def sample(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> Annotated['GraphQLSample', strawberry.lazy('.sample')]:
        loader = info.context.loaders[SampleLoaderKeys.SAMPLES_FOR_IDS]
        sample = await loader.load(root.sample_id)
        return GraphQLSample.from_internal(sample)

    @strawberry.field
    async def analyses(
        self,
        info: strawberry.Info[GraphQLContext, None],
        root: Self,
        status: GraphQLFilter[GraphQLAnalysisStatus] | None = None,
        type: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
        active: GraphQLFilter[bool] | None = None,
        project: GraphQLFilter[str] | None = None,
    ) -> list[Annotated['GraphQLAnalysis', strawberry.lazy('.analysis')]]:
        from .analysis import GraphQLAnalysis

        connection = info.context.connection
        loader = info.context.loaders[AnalysisLoaderKeys.ANALYSES_FOR_SEQUENCING_GROUPS]

        _project_filter: GenericFilter[ProjectId] | None = None
        if project:
            project_names = project.all_values()
            projects = connection.get_and_check_access_to_projects_for_names(
                project_names=project_names,
                allowed_roles=ReadAccessRoles,
            )
            project_id_map: dict[str, int] = {
                p.name: p.id for p in projects if p.name and p.id
            }
            _project_filter = project.to_internal_filter_mapped(
                lambda p: project_id_map[p]
            )

        analyses = await loader.load(
            {
                'id': root.internal_id,
                'filter_': AnalysisFilter(
                    status=status.to_internal_filter() if status else None,
                    type=type.to_internal_filter() if type else None,
                    meta=graphql_meta_filter_to_internal_filter(meta),
                    active=(
                        active.to_internal_filter()
                        if active
                        else GenericFilter(eq=True)
                    ),
                    project=_project_filter,
                ),
            }
        )
        return [GraphQLAnalysis.from_internal(a) for a in analyses]

    @strawberry.field
    async def assays(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> list[Annotated['GraphQLAssay', strawberry.lazy('.assay')]]:
        from .assay import GraphQLAssay

        loader = info.context.loaders[AssayLoaderKeys.ASSAYS_FOR_SEQUENCING_GROUPS]
        assays = await loader.load(root.internal_id)
        return [GraphQLAssay.from_internal(assay) for assay in assays]

    @strawberry.field()
    async def discussion(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> Annotated['GraphQLDiscussion', strawberry.lazy('.comment')]:
        from .comment import GraphQLDiscussion

        loader = info.context.loaders[
            CommentLoaderKeys.COMMENTS_FOR_SEQUENCING_GROUP_IDS
        ]
        discussion = await loader.load(root.internal_id)
        return GraphQLDiscussion.from_internal(discussion)
