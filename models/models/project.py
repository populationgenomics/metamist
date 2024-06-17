import json
from enum import Enum
from typing import Any, Optional

from pydantic import field_serializer

from models.base import SMBase

ProjectId = int

ProjectMemberRole = Enum(
    'ProjectMemberRole', ['reader', 'contributor', 'writer', 'data_manager']
)

project_member_role_names = [r.name for r in ProjectMemberRole]

# These roles have read access to a project
ReadAccessRoles = {
    ProjectMemberRole.reader,
    ProjectMemberRole.contributor,
    ProjectMemberRole.writer,
    ProjectMemberRole.data_manager,
}

# Only write has full write access
FullWriteAccessRoles = {ProjectMemberRole.writer}


class Project(SMBase):
    """Row for project in 'project' table"""

    id: ProjectId
    name: str
    dataset: str
    meta: Optional[dict[str, Any]] = None
    roles: set[ProjectMemberRole]
    """The roles that the current user has within the project"""

    @property
    def is_test(self):
        """
        Checks whether this is a test project. Comparing to the dataset is safer than
        just checking whether the name ends with -test, just in case we have a non-test
        project that happens to end with -test
        """
        return self.name == f'{self.dataset}-test'

    @field_serializer('roles')
    def serialize_roles(self, roles: set[ProjectMemberRole], _info):
        return [r.name for r in roles]

    @staticmethod
    def from_db(kwargs):
        """From DB row, with db keys"""
        kwargs = dict(kwargs)
        kwargs['meta'] = json.loads(kwargs['meta']) if kwargs.get('meta') else {}

        # Sanitise role names and convert to enum members
        role_list: list[str] = kwargs['roles'].split(',') if kwargs.get('roles') else []
        kwargs['roles'] = {
            ProjectMemberRole[r] for r in role_list if r in project_member_role_names
        }
        return Project(**kwargs)
