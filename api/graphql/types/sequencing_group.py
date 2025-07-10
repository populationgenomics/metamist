# pylint: disable=reimported,import-outside-toplevel,wrong-import-position
from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.filters import (
    GraphQLFilter,
    GraphQLMetaFilter,
    graphql_meta_filter_to_internal_filter,
)
from api.graphql.loaders import GraphQLContext, LoaderKeys
from api.graphql.types.enums import GraphQLAnalysisStatus
from db.python.filters import GenericFilter
from db.python.tables.analysis import AnalysisFilter
from models.models import (
    SequencingGroupInternal,
)
from models.models.project import (
    ProjectId,
    ReadAccessRoles,
)
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_format,
)

if TYPE_CHECKING:
    from api.graphql.schema import Query
    from api.graphql.types.analysis import GraphQLAnalysis
    from api.graphql.types.assay import GraphQLAssay
    from api.graphql.types.comments import GraphQLDiscussion
    from api.graphql.types.sample import GraphQLSample


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
        """Convert a SequencingGroupInternal to GraphQLSequencingGroup"""
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
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLSequencingGroup'
    ) -> Annotated['GraphQLSample', strawberry.lazy('api.graphql.types.sample')]:
        """Get the sample associated with this sequencing group"""
        from api.graphql.types.sample import GraphQLSample

        loader = info.context['loaders'][LoaderKeys.SAMPLES_FOR_IDS]
        sample = await loader.load(root.sample_id)
        return GraphQLSample.from_internal(sample)

    @strawberry.field
    async def analyses(
        self,
        info: Info[GraphQLContext, 'Query'],
        root: 'GraphQLSequencingGroup',
        status: GraphQLFilter[GraphQLAnalysisStatus] | None = None,
        type_: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
        active: GraphQLFilter[bool] | None = None,
        project: GraphQLFilter[str] | None = None,
    ) -> Annotated[
        list['GraphQLAnalysis'], strawberry.lazy('api.graphql.types.analysis')
    ]:
        """Get analyses associated with this sequencing group"""
        from api.graphql.types.analysis import GraphQLAnalysis

        connection = info.context['connection']
        loader = info.context['loaders'][LoaderKeys.ANALYSES_FOR_SEQUENCING_GROUPS]

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
                    type=type_.to_internal_filter() if type_ else None,
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
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLSequencingGroup'
    ) -> Annotated[list['GraphQLAssay'], strawberry.lazy('api.graphql.types.assay')]:
        """Get assays associated with this sequencing group"""
        from api.graphql.types.assay import GraphQLAssay

        loader = info.context['loaders'][LoaderKeys.ASSAYS_FOR_SEQUENCING_GROUPS]
        assays = await loader.load(root.internal_id)
        return [GraphQLAssay.from_internal(assay) for assay in assays]

    @strawberry.field()
    async def discussion(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLSequencingGroup'
    ) -> Annotated['GraphQLDiscussion', strawberry.lazy('api.graphql.types.comments')]:
        """Get discussion associated with this sequencing group"""
        from api.graphql.types.comments import GraphQLDiscussion

        loader = info.context['loaders'][LoaderKeys.COMMENTS_FOR_SEQUENCING_GROUP_IDS]
        discussion = await loader.load(root.internal_id)
        return GraphQLDiscussion.from_internal(discussion)


from api.graphql.types.analysis import GraphQLAnalysis
from api.graphql.types.assay import GraphQLAssay
from api.graphql.types.comments import GraphQLDiscussion
from api.graphql.types.sample import GraphQLSample
