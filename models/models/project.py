from enum import StrEnum
from typing import Any, Optional

from models.base import SMBase

ProjectId = int

ProjectMemberRole = StrEnum(
    'ProjectMemberRole',
    [
        'reader',
        'contributor',
        'writer',
        'project_admin',
        'project_member_admin',
    ],
)


# These roles have read access to a project
ReadAccessRoles = {
    ProjectMemberRole.reader,
    ProjectMemberRole.contributor,
    ProjectMemberRole.writer,
}

# Only write has full write access
FullWriteAccessRoles = {ProjectMemberRole.writer}
project_member_role_names = [r.name for r in ProjectMemberRole]


class Project(SMBase):
    """Row for project in 'project' table"""

    id: ProjectId
    name: str
    dataset: str
    meta: Optional[dict[str, Any]] = None
    roles: set[ProjectMemberRole]
    """The roles that the current user has within the project"""

    @property
    def is_test_project(self):
        """Returns whether this is a main or a test project"""
        return self.name.endswith('-test')


class ProjectMemberUpdate(SMBase):
    """Item included in list of project member updates"""

    member: str
    roles: list[str]
