# pylint: disable=reimported,import-outside-toplevel,wrong-import-position
from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.filters import (
    GraphQLFilter,
)
from api.graphql.loaders import GraphQLContext, LoaderKeys
from db.python.filters import GenericFilter
from db.python.layers import (
    AnalysisLayer,
    CohortLayer,
    SequencingGroupLayer,
)
from db.python.tables.analysis import AnalysisFilter
from db.python.tables.sequencing_group import SequencingGroupFilter
from models.models import (
    CohortInternal,
)
from models.models.project import (
    ReadAccessRoles,
)
from models.utils.cohort_id_format import cohort_id_format, cohort_id_transform_to_raw

if TYPE_CHECKING:
    from api.graphql.schema import Query
    from api.graphql.types.analysis import GraphQLAnalysis
    from api.graphql.types.cohort_template import GraphQLCohortTemplate
    from api.graphql.types.project import GraphQLProject
    from api.graphql.types.sequencing_group import GraphQLSequencingGroup


# Create cohort GraphQL model
@strawberry.type
class GraphQLCohort:
    """Cohort GraphQL model"""

    id: str
    name: str
    description: str
    author: str

    project_id: strawberry.Private[int]

    @staticmethod
    def from_internal(internal: CohortInternal) -> 'GraphQLCohort':
        """Convert an internal CohortInternal model to a GraphQLCohort instance."""
        return GraphQLCohort(
            id=cohort_id_format(internal.id),
            name=internal.name,
            description=internal.description,
            author=internal.author,
            project_id=internal.project,
        )

    @strawberry.field()
    async def template(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLCohort'
    ) -> Annotated[
        'GraphQLCohortTemplate', strawberry.lazy('api.graphql.types.cohort_template')
    ]:
        """Get the cohort template associated with this cohort."""
        from api.graphql.types.cohort_template import GraphQLCohortTemplate

        connection = info.context['connection']
        template = await CohortLayer(connection).get_template_by_cohort_id(
            cohort_id_transform_to_raw(root.id)
        )

        projects = connection.get_and_check_access_to_projects_for_ids(
            project_ids=(
                template.criteria.projects if template.criteria.projects else []
            ),
            allowed_roles=ReadAccessRoles,
        )
        project_names = [p.name for p in projects if p.name]

        return GraphQLCohortTemplate.from_internal(
            template, project_names=project_names
        )

    @strawberry.field()
    async def sequencing_groups(
        self,
        info: Info[GraphQLContext, 'Query'],
        root: 'GraphQLCohort',
        active_only: GraphQLFilter[bool] | None = None,
    ) -> Annotated[
        list['GraphQLSequencingGroup'],
        strawberry.lazy('api.graphql.types.sequencing_group'),
    ]:
        """Get the sequencing groups associated with this cohort."""
        from api.graphql.types.sequencing_group import GraphQLSequencingGroup

        connection = info.context['connection']
        cohort_layer = CohortLayer(connection)
        sg_ids = await cohort_layer.get_cohort_sequencing_group_ids(
            cohort_id_transform_to_raw(root.id)
        )

        sg_layer = SequencingGroupLayer(connection)
        filter_ = SequencingGroupFilter(
            id=GenericFilter(in_=sg_ids),
            active_only=active_only.to_internal_filter() if active_only else None,
        )
        sequencing_groups = await sg_layer.query(filter_=filter_)

        return [GraphQLSequencingGroup.from_internal(sg) for sg in sequencing_groups]

    @strawberry.field()
    async def analyses(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLCohort'
    ) -> Annotated[
        list['GraphQLAnalysis'], strawberry.lazy('api.graphql.types.analysis')
    ]:
        """Get the analyses associated with this cohort."""
        from api.graphql.types.analysis import GraphQLAnalysis

        connection = info.context['connection']
        internal_analysis = await AnalysisLayer(connection).query(
            AnalysisFilter(
                cohort_id=GenericFilter(in_=[cohort_id_transform_to_raw(root.id)]),
            )
        )
        return [GraphQLAnalysis.from_internal(a) for a in internal_analysis]

    @strawberry.field()
    async def project(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLCohort'
    ) -> Annotated['GraphQLProject', strawberry.lazy('api.graphql.types.project')]:
        """Get the project associated with this cohort."""
        from api.graphql.types.project import GraphQLProject

        loader = info.context['loaders'][LoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project_id)
        return GraphQLProject.from_internal(project)


from api.graphql.types.analysis import GraphQLAnalysis
from api.graphql.types.cohort_template import GraphQLCohortTemplate
from api.graphql.types.project import GraphQLProject
from api.graphql.types.sequencing_group import GraphQLSequencingGroup
