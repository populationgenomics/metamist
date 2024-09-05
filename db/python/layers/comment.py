from db.python.connect import Connection
from db.python.filters.generic import GenericFilter
from db.python.layers.base import BaseLayer
from db.python.tables.assay import AssayTable
from db.python.tables.comment import CommentTable
from db.python.tables.family import FamilyFilter, FamilyTable
from db.python.tables.participant import ParticipantTable
from db.python.tables.sample import SampleTable
from db.python.tables.sequencing_group import SequencingGroupTable
from db.python.utils import Forbidden, NotFoundError
from models.models.comment import (
    CommentEntityType,
    CommentInternal,
    CommentStatus,
    DiscussionInternal,
)
from models.models.project import ProjectMemberRole

COMMENT_WRITE_ROLES = {ProjectMemberRole.writer, ProjectMemberRole.contributor}


class CommentLayer(BaseLayer):
    """Layer for comment logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)

        self.ct = CommentTable(connection)
        self.at = AssayTable(connection)
        self.st = SampleTable(connection)
        self.pt = ParticipantTable(connection)
        self.ft = FamilyTable(connection)
        self.sgt = SequencingGroupTable(connection)

    async def get_discussion_for_entity_ids(
        self, entity: CommentEntityType, entity_ids: list[int]
    ) -> list[DiscussionInternal | None]:
        """
        Query discussion for the provided entity IDs
        _Note_: this doesn't check if the user has permission to access the
        comments. It is expected that these checks are carried out on the entities
        that the comments are attached to.
        """
        return await self.ct.get_discussion_for_entity_ids(entity, entity_ids)

    async def check_project_access_for_entity(
        self,
        entity: CommentEntityType,
        entity_id: int,
        allowed_roles: set[ProjectMemberRole],
    ):
        """
        Check if the current user has access to the project related to the comment entity
        """

        project_id: int | None = None

        match entity:
            case CommentEntityType.assay:
                pid, _ = await self.at.get_assay_by_id(entity_id)
                project_id = pid
            case CommentEntityType.sample:
                pid, _ = await self.st.get_sample_by_id(entity_id)
                project_id = pid
            case CommentEntityType.participant:
                pids, _ = await self.pt.get_participants_by_ids([entity_id])
                project_id = list(pids)[0] if len(pids) > 0 else None
            case CommentEntityType.family:
                pids, _ = await self.ft.query(
                    FamilyFilter(id=GenericFilter(eq=entity_id))
                )
                project_id = list(pids)[0] if len(pids) > 0 else None
            case CommentEntityType.project:
                project_id = entity_id

            case CommentEntityType.sequencing_group:
                pids, _ = await self.sgt.get_sequencing_groups_by_ids([entity_id])
                project_id = list(pids)[0] if len(pids) > 0 else None

        if project_id is None:
            raise NotFoundError(f'Project not found for {entity} with id {entity_id}')

        self.connection.check_access_to_projects_for_ids([project_id], allowed_roles)

    async def check_project_access_for_comment(
        self, comment: CommentInternal, allowed_roles: set[ProjectMemberRole]
    ):
        """
        Check if the current user has access to the project that the comment was made in
        """
        await self.check_project_access_for_entity(
            entity=comment.comment_entity_type,
            entity_id=comment.comment_entity_id,
            allowed_roles=allowed_roles,
        )

    async def check_author_access_for_comment(self, comment: CommentInternal):
        """
        Check if the current user is the author of the comment. This is done to ensure
        that people can only update/delete their own comments
        """
        current_user = self.connection.author
        if current_user != comment.author:
            raise Forbidden(
                f'User {current_user} cannot update comment authored by {comment.author}'
            )

    async def add_comment_to_entity(
        self, entity: CommentEntityType, entity_id: int, content: str
    ):
        """
        Add a comment to the specified entity, this will check if the user has the
        necessary permissions in the project for the specified entity
        """
        await self.check_project_access_for_entity(
            entity, entity_id, COMMENT_WRITE_ROLES
        )
        return await self.ct.add_comment_to_entity(entity, entity_id, content)

    async def add_comment_to_thread(self, parent_id: int, content: str):
        """Adds a child comment to a parent comment's thread"""
        comment = await self.ct.get_comment_by_id(parent_id)
        await self.check_project_access_for_comment(comment, COMMENT_WRITE_ROLES)
        return await self.ct.add_comment_to_thread(content, parent_id)

    async def update_comment(self, comment_id: int, content: str):
        """Updates an existing comment's content"""
        comment = await self.ct.get_comment_by_id(comment_id)
        await self.check_project_access_for_comment(comment, COMMENT_WRITE_ROLES)
        await self.check_author_access_for_comment(comment)
        return await self.ct.update_comment(content=content, comment_id=comment_id)

    async def delete_comment(self, comment_id: int):
        """Soft deletes an existing comment"""
        comment = await self.ct.get_comment_by_id(comment_id)
        await self.check_project_access_for_comment(comment, COMMENT_WRITE_ROLES)
        await self.check_author_access_for_comment(comment)
        return await self.ct.update_comment(
            status=CommentStatus.deleted, comment_id=comment_id
        )

    async def restore_comment(self, comment_id: int):
        """Restores a previously deleted comment"""
        comment = await self.ct.get_comment_by_id(comment_id)
        await self.check_project_access_for_comment(comment, COMMENT_WRITE_ROLES)
        await self.check_author_access_for_comment(comment)
        return await self.ct.update_comment(
            status=CommentStatus.active, comment_id=comment_id
        )
