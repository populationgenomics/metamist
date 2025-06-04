from typing import TYPE_CHECKING

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext, LoaderKeys
from api.graphql.types.project import GraphQLProject
from models.models import (
    CohortTemplateInternal,
)
from models.utils.cohort_template_id_format import (
    cohort_template_id_format,
)

if TYPE_CHECKING:
    from api.graphql.schema import Query


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
        """Create a GraphQLCohortTemplate instance from an internal CohortTemplateInternal model."""
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
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLCohortTemplate'
    ) -> GraphQLProject:
        """Get the project associated with this cohort template."""
        loader = info.context['loaders'][LoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project_id)
        return GraphQLProject.from_internal(project)
