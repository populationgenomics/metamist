# Create cohort GraphQL model
from typing import TYPE_CHECKING, Annotated, Self

import strawberry
from strawberry.types import Info

from api.graphql.context import GraphQLContext
from api.graphql.filters import GraphQLFilter
from api.graphql.query.project_loaders import ProjectLoaderKeys
from db.python.filters.generic import GenericFilter
from db.python.filters.sequencing_group import SequencingGroupFilter
from db.python.layers.analysis import AnalysisLayer
from db.python.layers.cohort import CohortLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.tables.analysis import AnalysisFilter
from models.models.cohort import CohortInternal, CohortTemplateInternal
from models.models.project import ReadAccessRoles
from models.utils.cohort_id_format import cohort_id_format, cohort_id_transform_to_raw
from models.utils.cohort_template_id_format import cohort_template_id_format

if TYPE_CHECKING:
    from .analysis import GraphQLAnalysis
    from .project import GraphQLProject
    from .sequencing_group import GraphQLSequencingGroup


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
        """From internal model"""
        return GraphQLCohort(
            id=cohort_id_format(internal.id),
            name=internal.name,
            description=internal.description,
            author=internal.author,
            project_id=internal.project,
        )

    @strawberry.field()
    async def template(
        self, info: Info[GraphQLContext, None], root: Self
    ) -> 'GraphQLCohortTemplate':
        """Get cohort template"""
        connection = info.context.connection
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
        info: Info[GraphQLContext, 'GraphQLCohort'],
        root: Self,
        active_only: GraphQLFilter[bool] | None = None,
    ) -> list[
        Annotated['GraphQLSequencingGroup', strawberry.lazy('.sequencing_group')]
    ]:
        """Get sequencing groups for cohort"""
        from api.graphql.query.sequencing_group import GraphQLSequencingGroup

        connection = info.context.connection
        cohort_layer = CohortLayer(connection)
        sg_ids = await cohort_layer.get_cohort_sequencing_group_ids(
            cohort_id_transform_to_raw(root.id)
        )

        sg_layer = SequencingGroupLayer(connection)
        filter = SequencingGroupFilter(
            id=GenericFilter(in_=sg_ids),
            active_only=active_only.to_internal_filter() if active_only else None,
        )
        sequencing_groups = await sg_layer.query(filter_=filter)

        return [GraphQLSequencingGroup.from_internal(sg) for sg in sequencing_groups]

    @strawberry.field()
    async def analyses(
        self, info: Info[GraphQLContext, None], root: Self
    ) -> list[Annotated['GraphQLAnalysis', strawberry.lazy('.analysis')]]:
        from api.graphql.query.analysis import GraphQLAnalysis

        connection = info.context.connection
        internal_analysis = await AnalysisLayer(connection).query(
            AnalysisFilter(
                cohort_id=GenericFilter(in_=[cohort_id_transform_to_raw(root.id)]),
            )
        )
        return [GraphQLAnalysis.from_internal(a) for a in internal_analysis]

    @strawberry.field()
    async def project(
        self, info: Info[GraphQLContext, None], root: Self
    ) -> Annotated['GraphQLProject', strawberry.lazy('.project')]:
        """Get project for cohort"""
        from .project import GraphQLProject

        loader = info.context.loaders[ProjectLoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project_id)
        return GraphQLProject.from_internal(project)


# Create cohort template GraphQL model
@strawberry.type
class GraphQLCohortTemplate:
    """CohortTemplate GraphQL model"""

    id: str
    name: str
    description: str
    criteria: strawberry.scalars.JSON

    project_id: strawberry.Private[int]

    @staticmethod
    def from_internal(
        internal: CohortTemplateInternal, project_names: list[str]
    ) -> 'GraphQLCohortTemplate':
        # At this point, the object that comes in doesn't have an ID field.
        return GraphQLCohortTemplate(
            id=cohort_template_id_format(internal.id),
            name=internal.name,
            description=internal.description,
            criteria=internal.criteria.to_external(
                project_names=project_names
            ).model_dump(),
            project_id=internal.project,
        )

    @strawberry.field()
    async def project(
        self, info: Info[GraphQLContext, None], root: Self
    ) -> Annotated['GraphQLProject', strawberry.lazy('.project')]:
        from .project import GraphQLProject

        loader = info.context.loaders[ProjectLoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project_id)
        return GraphQLProject.from_internal(project)
