from typing import Any, Optional

from pydantic import BaseModel


class ProjectGroupFilter(BaseModel):
    """Filter for project groups."""

    id: Optional[int] = None
    user_id: Optional[int] = None
    group_id: Optional[int] = None
    group_name: Optional[str] = None
    description: Optional[str] = None

    def to_sql_where(self) -> tuple[str, dict]:
        """Convert the filter to an SQL WHERE clause and parameters."""
        clauses = []

        params: dict[str, Any] = {}

        if self.id is not None:
            clauses.append('g.id = :id')
            params['id'] = self.id
        if self.user_id is not None:
            clauses.append('g.user_id = :user_id')
            params['user_id'] = self.user_id
        if self.group_id is not None:
            clauses.append('g.group_id = :group_id')
            params['group_id'] = self.group_id
        if self.group_name is not None:
            clauses.append('g.group_name LIKE :group_name')
            params['group_name'] = f'%{self.group_name}%'
        if self.description is not None:
            clauses.append('g.description LIKE :description')
            params['description'] = f'%{self.description}%'
        where = ' AND '.join(clauses) if clauses else '1=1'
        return where, params
