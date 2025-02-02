import datetime
from typing import Self

import strawberry

from api.graphql.context import GraphQLContext
from api.graphql.query.project import GraphQLProject
from api.graphql.query.project_loaders import ProjectLoaderKeys
from models.models.analysis_runner import AnalysisRunnerInternal


@strawberry.type
class GraphQLAnalysisRunner:
    """AnalysisRunner GraphQL model"""

    ar_guid: str
    output_path: str

    timestamp: datetime.datetime
    access_level: str
    repository: str
    commit: str
    script: str
    description: str
    driver_image: str
    config_path: str | None
    cwd: str | None
    environment: str
    hail_version: str | None
    batch_url: str
    submitting_user: str
    meta: strawberry.scalars.JSON

    internal_project: strawberry.Private[int]

    @staticmethod
    def from_internal(internal: AnalysisRunnerInternal) -> 'GraphQLAnalysisRunner':
        """From internal model"""
        return GraphQLAnalysisRunner(
            ar_guid=internal.ar_guid,
            timestamp=internal.timestamp,
            access_level=internal.access_level,
            repository=internal.repository,
            commit=internal.commit,
            script=internal.script,
            description=internal.description,
            driver_image=internal.driver_image,
            config_path=internal.config_path,
            cwd=internal.cwd,
            environment=internal.environment,
            hail_version=internal.hail_version,
            batch_url=internal.batch_url,
            submitting_user=internal.submitting_user,
            meta=internal.meta,
            output_path=internal.output_path,
            # internal
            internal_project=internal.project,
        )

    @strawberry.field
    async def project(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> GraphQLProject:
        loader = info.context.loaders[ProjectLoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.internal_project)
        return GraphQLProject.from_internal(project)
