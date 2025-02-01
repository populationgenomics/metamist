from api.graphql.utils.loaders import connected_data_loader
from db.python.connect import Connection
from models.models.project import Project


class ProjectLoaderKeys:
    PROJECTS_FOR_IDS = 'projects_for_id'


@connected_data_loader(ProjectLoaderKeys.PROJECTS_FOR_IDS)
async def load_projects_for_ids(
    project_ids: list[int], connection: Connection
) -> list[Project]:
    """
    Get projects by IDs
    """
    projects = [connection.project_id_map.get(p) for p in project_ids]

    return [p for p in projects if p is not None]
