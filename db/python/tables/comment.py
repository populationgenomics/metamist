from itertools import groupby

from db.python.tables.base import DbBase
from db.python.utils import InternalError, NotFoundError
from models.models.comment import (
    CommentEntityType,
    CommentInternal,
    CommentStatus,
    DiscussionInternal,
)

# These comment queries look a bit scary but aren't actually too bad
#
# As a bit of background – because metamist has a very relational structure with
# lots of entities referencing other ones, discoverability of comments could be a
# problem. ie. you might make a comment on a family, and then when you are looking at a
# participant who is a member of that family you would want to see the comments made
# on the family as it may provide important context when looking at the participant.
#
# This big object describes how to traverse the relational structure from a related
# entity type back to the requested entity type so that comments on the related
# entites can be found as part of the same query.
#
# It makes a bit more sense when you consider that each of the queries in this object
# is prefixed with a query that can be derived from the comment entity type name, this
# part of the query is not included as it would be way more verbose and very repetitive
# to keep it in there.
#
# SELECT
#    entity_ids.id as requested_entity_id,
#    {comment_entity}_comment.comment_id,
#    '{comment_entity}' AS comment_entity_type,
#    {comment_entity}_comment.{comment_entity}_id AS comment_entity_id
# FROM {comment_entity}_comment
#
# `comment_entity` is replaced with the _inner_ comment entity from the below dict
# and the query string below is added onto the end. for example, for getting related
# assay comments to a sample the full query would be:
#
# SELECT
#    entity_ids.id as requested_entity_id,
#    assay_comment.comment_id,
#    'assay' AS comment_entity_type,
#    assay_comment.assay_id AS comment_entity_id
# FROM assay_comment
# JOIN assay
# ON assay.id = assay_comment.assay_id
# JOIN entity_ids ON assay.sample_id = entity_ids.id
#
# the `entity_ids` table is a CTE that in this case would contain the ids of the
# requested samples.
#
# So the query strings below basically need to describe the query part necessary to
# traverse the relational database model _from_ the entity comments table, back to the
# entity_ids table which contains the ids for the requested entity, which in our example
# case is samples.
#
# All these queries are then combined into a bit UNION query that will return all the
# related comments as well as the ones made directly on the requested entity.
# The first item in each of the top level dicts describes how to get the direct comments
# it should be pretty much the exact same for each entity, except for samples which have
# a special case where samples can have subsamples, we want to include those comments
# in the related comments

comment_queries: dict[CommentEntityType, dict[CommentEntityType, str]] = {
    CommentEntityType.project: {
        CommentEntityType.project: """
            JOIN entity_ids ON project_comment.project_id = entity_ids.id
        """,
        CommentEntityType.sample: """
            JOIN sample
            ON sample.id = sample_comment.sample_id
            JOIN entity_ids ON sample.project = entity_ids.id
        """,
        CommentEntityType.assay: """
            JOIN assay
            ON assay.id = assay_comment.assay_id
            JOIN sample
            ON sample.id = assay.sample_id
            JOIN entity_ids ON sample.project = entity_ids.id
        """,
        CommentEntityType.participant: """
            JOIN participant
            ON participant.id = participant_comment.participant_id
            JOIN entity_ids ON participant.project = entity_ids.id
        """,
        CommentEntityType.family: """
            JOIN family
            ON family.id = family_comment.family_id
            JOIN entity_ids ON family.project = entity_ids.id
        """,
        CommentEntityType.sequencing_group: """
            JOIN sequencing_group
            ON sequencing_group.id = sequencing_group_comment.sequencing_group_id
            JOIN sample
            ON sample.id = sequencing_group.sample_id
            JOIN entity_ids ON sample.project = entity_ids.id
        """,
    },
    CommentEntityType.sample: {
        CommentEntityType.sample: """
            JOIN sample s
            ON s.id = sample_comment.sample_id

            -- collect the parent ids of the comments on the requested entity
            -- this is used to fetch related comments on sample parents
            JOIN (
                select entity_ids.id, ss.sample_parent_id, ss.sample_root_id
                from entity_ids
                join sample ss
                on ss.id = entity_ids.id
            ) as entity_ids

            ON entity_ids.id = s.id
            OR entity_ids.id = s.sample_parent_id
            OR entity_ids.id = s.sample_root_id

            OR entity_ids.sample_parent_id = s.id
            OR entity_ids.sample_root_id = s.id

        """,
        CommentEntityType.assay: """
            JOIN assay
            ON assay.id = assay_comment.assay_id
            JOIN entity_ids ON assay.sample_id = entity_ids.id
        """,
        CommentEntityType.participant: """
            JOIN sample
            ON sample.participant_id = participant_comment.participant_id
            JOIN entity_ids ON sample.id = entity_ids.id
        """,
        CommentEntityType.family: """
            JOIN family_participant
            ON family_participant.family_id = family_comment.family_id
            JOIN sample
            ON sample.participant_id = family_participant.participant_id
            JOIN entity_ids ON sample.id = entity_ids.id
        """,
        CommentEntityType.sequencing_group: """
            JOIN sequencing_group
            ON sequencing_group.id = sequencing_group_comment.sequencing_group_id
            JOIN entity_ids ON sequencing_group.sample_id = entity_ids.id
        """,
    },
    CommentEntityType.assay: {
        CommentEntityType.assay: """
            JOIN entity_ids ON assay_comment.assay_id = entity_ids.id
        """,
        CommentEntityType.sample: """
            JOIN assay
            ON assay.sample_id = sample_comment.sample_id
            JOIN entity_ids ON assay.id = entity_ids.id
        """,
        CommentEntityType.family: """
            JOIN family_participant
            ON family_participant.family_id = family_comment.family_id
            JOIN sample
            ON sample.participant_id = family_participant.participant_id
            JOIN assay
            ON assay.sample_id = sample.id
            JOIN entity_ids ON assay.id = entity_ids.id
        """,
        CommentEntityType.participant: """
            JOIN sample
            ON sample.participant_id = participant_comment.participant_id
            JOIN assay
            ON assay.sample_id = sample.id
            JOIN entity_ids ON assay.id = entity_ids.id
        """,
        CommentEntityType.sequencing_group: """
            JOIN sequencing_group_assay
            ON sequencing_group_assay.sequencing_group_id = sequencing_group_comment.sequencing_group_id
            JOIN entity_ids ON sequencing_group_assay.assay_id = entity_ids.id
        """,
    },
    CommentEntityType.participant: {
        CommentEntityType.participant: """
            JOIN entity_ids ON participant_comment.participant_id = entity_ids.id
        """,
        CommentEntityType.assay: """
            JOIN assay
            ON assay.id = assay_comment.assay_id
            JOIN sample
            ON sample.id = assay.sample_id
            JOIN entity_ids ON sample.participant_id = entity_ids.id
        """,
        CommentEntityType.sample: """
            JOIN sample
            ON sample.id = sample_comment.sample_id
            JOIN entity_ids ON sample.participant_id = entity_ids.id
        """,
        CommentEntityType.family: """
            JOIN family_participant
            ON family_participant.family_id = family_comment.family_id
            JOIN entity_ids ON family_participant.participant_id = entity_ids.id
        """,
        CommentEntityType.sequencing_group: """
            JOIN sequencing_group
            ON sequencing_group.id = sequencing_group_comment.sequencing_group_id
            JOIN sample
            ON sample.id = sequencing_group.sample_id
            JOIN entity_ids ON sample.participant_id = entity_ids.id
        """,
    },
    CommentEntityType.family: {
        CommentEntityType.family: """
            JOIN entity_ids ON family_comment.family_id = entity_ids.id
        """,
        CommentEntityType.assay: """
            JOIN assay
            ON assay.id = assay_comment.assay_id
            JOIN sample
            ON sample.id = assay.sample_id
            JOIN family_participant
            ON family_participant.participant_id = sample.participant_id
            JOIN entity_ids ON family_participant.family_id = entity_ids.id
        """,
        CommentEntityType.sample: """
            JOIN sample
            ON sample.id = sample_comment.sample_id
            JOIN family_participant
            ON family_participant.participant_id = sample.participant_id
            JOIN entity_ids ON family_participant.family_id = entity_ids.id
        """,
        CommentEntityType.participant: """
            JOIN family_participant
            ON family_participant.participant_id = participant_comment.participant_id
            JOIN entity_ids ON family_participant.family_id = entity_ids.id
        """,
        CommentEntityType.sequencing_group: """
            JOIN sequencing_group
            ON sequencing_group.id = sequencing_group_comment.sequencing_group_id
            JOIN sample
            ON sample.id = sequencing_group.sample_id
            JOIN family_participant
            ON family_participant.participant_id = sample.participant_id
            JOIN entity_ids ON family_participant.family_id = entity_ids.id
        """,
    },
    CommentEntityType.sequencing_group: {
        CommentEntityType.sequencing_group: """
            JOIN entity_ids ON sequencing_group_comment.sequencing_group_id = entity_ids.id
        """,
        CommentEntityType.participant: """
            JOIN sample
            ON sample.participant_id = participant_comment.participant_id
            JOIN sequencing_group
            ON sequencing_group.sample_id = sample.id
            JOIN entity_ids ON sequencing_group.id = entity_ids.id
        """,
        CommentEntityType.family: """
            JOIN family_participant
            ON family_participant.family_id = family_comment.family_id
            JOIN sample
            ON sample.participant_id = family_participant.participant_id
            JOIN sequencing_group
            ON sequencing_group.sample_id = sample.id
            JOIN entity_ids ON sequencing_group.id = entity_ids.id
        """,
        CommentEntityType.assay: """
            JOIN sequencing_group_assay
            ON sequencing_group_assay.assay_id = assay_comment.assay_id
            JOIN entity_ids ON sequencing_group_assay.sequencing_group_id = entity_ids.id
        """,
        CommentEntityType.sample: """
            JOIN sequencing_group
            ON sequencing_group.sample_id = sample_comment.sample_id
            JOIN entity_ids ON sequencing_group.id = entity_ids.id
        """,
    },
}


class CommentTable(DbBase):
    """
    Comment table operations and queries
    """

    async def get_comments_for_entity_ids(
        self,
        entity: CommentEntityType,
        entity_ids: list[int],
        include_related_comments: bool = True,
        comment_id: int | None = None,
    ):
        """
        Get all the comments for a list of entities, will return flat list of comments
        """

        queries_for_entity = comment_queries.get(entity, None)
        if queries_for_entity is None:
            raise InternalError(f'Unknown comment entity {entity}')

        # In the below there are two entity ids queried, requested_entity_id and
        # comment_entity_id. requested_entity_id is the ID of the entity requested
        # whereas comment_entity_id is the id of the entity that the comment
        # is attached to, they can be different because comments related to the
        # requested entity can be returned as well as those attached directly
        combined_comment_query = '\nUNION\n'.join(
            [
                f"""(
                    SELECT
                        entity_ids.id as requested_entity_id,
                        {comment_entity}_comment.comment_id,
                        '{comment_entity}' AS comment_entity_type,
                        {comment_entity}_comment.{comment_entity}_id AS comment_entity_id
                    FROM {comment_entity}_comment
                    {comment_query}
                )"""
                for comment_entity, comment_query in queries_for_entity.items()
                if comment_entity == entity or include_related_comments
            ]
        )

        query = f"""
            WITH entity_ids as (
                SELECT id from {entity}
                WHERE id in :entity_ids
            ),
            top_level_comment_list AS (
                {combined_comment_query}
            ) SELECT
                c.id as comment_id,
                c.parent_id,
                c.content,
                c.status,
                tc.requested_entity_id,
                tc.comment_entity_type,
                tc.comment_entity_id,
                al.timestamp,
                al.author
			FROM comment FOR SYSTEM_TIME ALL AS c
            JOIN top_level_comment_list tc
            ON c.id = tc.comment_id OR c.parent_id = tc.comment_id
            LEFT JOIN audit_log al
            ON al.id = c.audit_log_id
            {'WHERE c.id = :comment_id or c.parent_id = :comment_id'  if comment_id else ''}
            ORDER BY c.id, al.timestamp
        """
        values: dict['str', int | list[int]] = {'entity_ids': entity_ids}

        if comment_id:
            values['comment_id'] = comment_id

        comment_versions = await self.connection.fetch_all(query, values)

        # Group comments by their ids so that versions get included within a comment
        comment_map: dict[int, CommentInternal] = {
            id: CommentInternal.from_db_versions(list(dict(v) for v in g))
            for id, g in groupby(comment_versions, key=lambda k: k['comment_id'])
        }

        # Organize threaded comments under their parents
        for _, comment in comment_map.items():
            if comment.parent_id is not None:
                parent = comment_map.get(comment.parent_id, None)
                if parent is not None:
                    parent.add_comment_to_thread(comment)

        return comment_map

    async def get_discussion_for_entity_ids(
        self, entity: CommentEntityType, entity_ids: list[int]
    ) -> list[DiscussionInternal | None]:
        """
        Get comments organized into a discussion, separated into direct and related
        comments for the specified entity
        """
        comments: list[CommentInternal] = []

        comment_map = await self.get_comments_for_entity_ids(
            entity=entity, entity_ids=entity_ids, include_related_comments=True
        )

        # Only add parent comments to list
        for _, comment in comment_map.items():
            if comment.parent_id is None:
                comments.append(comment)

        # Group comments by the entity id so that they can be returned in the same order
        # They were requested in. And wrap them in the Discussion model to separate
        # direct from related comments
        comments_by_entity_id_map = {
            id: DiscussionInternal.from_flat_comments(
                list(g), requested_entity_id=id, requested_entity_type=entity
            )
            for id, g in groupby(comments, key=lambda k: k.requested_entity_id)
        }

        return [comments_by_entity_id_map.get(eid, None) for eid in entity_ids]

    async def get_comment_by_id(self, comment_id: int):
        """
        Get's a comment and its threaded comments by the comment id
        """
        # To get a comment by id and be able to return the necessary entity info
        # we need to determine which entity the requested comment is attached to
        # so we build a query to union together results from all the comment join
        # tables.

        join_table_query = '\nUNION\n'.join(
            [
                f"""(
                SELECT
                    {entity_type}_id as entity_id,
                    '{entity_type}' as entity_type
                FROM {entity_type}_comment ec
                JOIN root_comment rc
                ON rc.comment_id = ec.comment_id
            )"""
                for entity_type in CommentEntityType
            ]
        )

        # Only root comments are attached to entities, so if the comment has a parent
        # ID we need to use that to find the entity type rather than the comment id
        query = f"""
            WITH root_comment as (
                SELECT COALESCE(parent_id, id) as comment_id
                FROM comment
                WHERE id = :comment_id
            ) {join_table_query}
        """

        rows = await self.connection.fetch_all(query, {'comment_id': comment_id})

        if len(rows) == 0:
            raise NotFoundError(f'Comment with id {comment_id} was not found')

        comments = await self.get_comments_for_entity_ids(
            entity_ids=[rows[0]['entity_id']],
            entity=rows[0]['entity_type'],
            include_related_comments=False,
            comment_id=comment_id,
        )

        if comment_id not in comments:
            raise NotFoundError(f'Comment with id {comment_id} was not found')

        return comments[comment_id]

    async def add_comment_to_entity(
        self, entity: CommentEntityType, entity_id: int, content: str
    ):
        """Adds a comment as a top level comment on the provided entity"""
        join_table = f'{entity}_comment'
        join_column = f'{entity}_id'

        comment_insert = """
            INSERT INTO comment (content, status, audit_log_id)
            VALUES (:content, 'active', :audit_log_id) RETURNING id;
        """

        join_insert = f"""
            INSERT INTO {join_table} (comment_id, {join_column}, audit_log_id)
            VALUES (:comment_id, :entity_id, :audit_log_id);
        """

        async with self.connection.transaction():
            audit_log_id = await self._connection.audit_log_id()

            comment_id = await self.connection.fetch_val(
                comment_insert,
                {
                    'content': content,
                    'audit_log_id': audit_log_id,
                },
            )

            await self.connection.execute(
                join_insert,
                {
                    'comment_id': comment_id,
                    'entity_id': entity_id,
                    'audit_log_id': audit_log_id,
                },
            )

            return await self.get_comment_by_id(comment_id)

    async def add_comment_to_thread(self, content: str, parent_id: int):
        """Add a comment a child comment to a parent comment's thread"""
        audit_log_id = await self._connection.audit_log_id()

        comment_insert = """
            INSERT INTO comment (parent_id, content, status, audit_log_id)
            VALUES (:parent_id, :content, 'active', :audit_log_id) RETURNING id;
        """

        comment_id = await self.connection.fetch_val(
            comment_insert,
            {
                'content': content,
                'parent_id': parent_id,
                'audit_log_id': audit_log_id,
            },
        )

        return await self.get_comment_by_id(comment_id)

    async def update_comment(
        self,
        comment_id: int,
        content: str | None = None,
        status: CommentStatus | None = None,
    ):
        """Update an existing comment"""
        current_comment = await self.get_comment_by_id(comment_id)

        content_changed = content is not None and current_comment.content != content
        status_changed = status is not None and current_comment.status != status
        no_update = content is None and status is None
        comment_changed = content_changed or status_changed

        # If changes are not passed, or nothing has changed then no need to do anything
        if no_update or not comment_changed:
            return current_comment

        audit_log_id = await self._connection.audit_log_id()

        # Construct the query string and values, excluding any updates that are
        # unchanged or don't have a value set. The query string would be invalid
        # if both content and status were not set or unchanged, but the checks
        # above avoid getting this far if that is the case.
        updates = [
            ('content', content, content_changed),
            ('status', status, status_changed),
        ]

        update_q = ', '.join(
            [f'{k} = :{k}' for k, v, changed in updates if v is not None and changed]
        )
        update_v = {k: v for k, v, changed in updates if v is not None and changed}

        comment_update = f"""
            UPDATE comment
            SET {update_q},
                audit_log_id = :audit_log_id
            WHERE id = :comment_id
        """

        await self.connection.execute(
            comment_update,
            {
                'comment_id': comment_id,
                'audit_log_id': audit_log_id,
            }
            | update_v,
        )

        return await self.get_comment_by_id(comment_id)
