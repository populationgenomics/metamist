# pylint: disable=too-many-instance-attributes

import datetime
from dataclasses import dataclass
from typing import Optional

import strawberry


@dataclass
class ProjectGroup:
    """ProjectGroup model representing a group of projects."""

    id: int
    user_id: int
    group_id: int
    group_name: str
    description: Optional[str]
    project_ids: list[int]  # List of project IDs in this group

    created_at: strawberry.Private[datetime.datetime]
    updated_at: strawberry.Private[datetime.datetime]
