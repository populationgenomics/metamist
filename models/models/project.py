from enum import StrEnum
from typing import Any, Optional

from models.base import SMBase, parse_sql_dict

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

    @staticmethod
    def from_db(kwargs):
        """From DB row, with db keys"""
        kwargs = dict(kwargs)
        kwargs['meta'] = parse_sql_dict(kwargs.get('meta')) or {}

        # Sanitise role names and convert to enum members
        role_list: list[str] = kwargs['roles'].split(',') if kwargs.get('roles') else []
        kwargs['roles'] = {
            ProjectMemberRole[r] for r in role_list if r in project_member_role_names
        }
        return Project(**kwargs)


class ProjectMemberUpdate(SMBase):
    """Item included in list of project member updates"""

    member: str
    roles: list[str]
