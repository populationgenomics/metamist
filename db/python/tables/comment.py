from itertools import groupby

from db.python.tables.base import DbBase
from db.python.utils import InternalError
from models.models.comment import CommentEntityType, CommentInternal

comment_queries: dict[CommentEntityType, dict[CommentEntityType, str]] = {
    CommentEntityType.project: {
        CommentEntityType.project: """
            WHERE project_comment.project_id = :entity_id
        """,
        CommentEntityType.sample: """
            JOIN sample
            ON sample.id = sample_comment.sample_id
            WHERE sample.project = :entity_id
        """,
        CommentEntityType.project.assay: """
            JOIN assay
            ON assay.id = assay_comment.assay_id
            JOIN sample
            ON sample.id = assay.sample_id
            WHERE sample.project = :entity_id
        """,
        CommentEntityType.participant: """
            JOIN participant
            ON participant.id = participant_comment.participant_id
            WHERE participant.project = :entity_id
        """,
        CommentEntityType.sequencing_group: """
            JOIN sequencing_group
            ON sequencing_group.id = sequencing_group_comment.sequencing_group_id
            JOIN sample
            ON sample.id = sequencing_group.sample_id
            WHERE sample.project = :entity_id
        """,
    },
    CommentEntityType.sample: {
        CommentEntityType.sample: """
            JOIN sample
            ON sample.id = sample_comment.sample_id
            
            -- Include comments on subsamples too
            WHERE sample.id = :entity_id
            OR sample.sample_root_id = :entity_id
            OR sample.sample_parent_id = :entity_id
        """,
        CommentEntityType.project.assay: """
            JOIN assay
            ON assay.id = assay_comment.assay_id
            WHERE assay.sample_id = :entity_id
        """,
        CommentEntityType.participant: """
            JOIN sample
            ON sample.participant_id = participant_comment.participant_id
            WHERE sample.id = :entity_id
        """,
        CommentEntityType.sequencing_group: """
            JOIN sequencing_group
            ON sequencing_group.id = sequencing_group_comment.sequencing_group_id
            WHERE sequencing_group.sample_id = :entity_id
        """,
    },
    CommentEntityType.assay: {
        CommentEntityType.project.assay: """
            WHERE assay_comment.assay_id = :entity_id
        """,
        CommentEntityType.sample: """
            JOIN assay
            ON assay.sample_id = sample_comment.sample_id
            WHERE assay.id = :entity_id
        """,
        CommentEntityType.participant: """
            JOIN sample
            ON sample.participant_id = participant_comment.participant_id
            JOIN assay
            ON assay.sample_id = sample.id
            WHERE assay.id = :entity_id
        """,
        CommentEntityType.sequencing_group: """
            JOIN sequencing_group_assay
            ON sequencing_group_assay.sequencing_group_id = sequencing_group_comment.sequencing_group_id
            WHERE sequencing_group_assay.assay_id = :entity_id
        """,
    },
    CommentEntityType.participant: {
        CommentEntityType.participant: """
            WHERE participant_comment.participant_id = :entity_id
        """,
        CommentEntityType.project.assay: """
            JOIN assay
            ON assay.id = assay_comment.assay_id
            JOIN sample
            ON sample.id = assay.sample_id
            WHERE sample.participant_id = :entity_id
        """,
        CommentEntityType.sample: """
            JOIN sample
            ON sample.id = sample_comment.sample_id
            WHERE sample.participant_id = :entity_id
        """,
        CommentEntityType.sequencing_group: """
            JOIN sequencing_group
            ON sequencing_group.id = sequencing_group_comment.sequencing_group_id
            JOIN sample
            ON sample.id = sequencing_group.sample_id
            WHERE sample.participant_id = :entity_id
        """,
    },
    CommentEntityType.sequencing_group: {
        CommentEntityType.sequencing_group: """
            WHERE sequencing_group_comment.sequencing_group_id = :entity_id
        """,
        CommentEntityType.participant: """
            JOIN sample
            ON sample.participant_id = participant_comment.participant_id
            JOIN sequencing_group
            ON sequencing_group.sample_id = sample.id
            WHERE sequencing_group.id = :entity_id
        """,
        CommentEntityType.project.assay: """
            JOIN sequencing_group_assay
            ON sequencing_group_assay.assay_id = assay_comment.assay_id
            WHERE sequencing_group_assay.sequencing_group_id = :entity_id
        """,
        CommentEntityType.sample: """
            JOIN sequencing_group
            ON sequencing_group.sample_id = sample_comment.sample_id
            WHERE sequencing_group.id = :entity_id
        """,
    },
}


class CommentTable(DbBase):
    """
    Capture Comment table operations and queries
    """

    async def query(
        self, entity: CommentEntityType, entity_id: int
    ) -> list[CommentInternal]:
        """Query comments"""

        queries_for_entity = comment_queries.get(entity, None)
        if queries_for_entity is None:
            raise InternalError(f"Unknown comment entity {entity}")

        combined_comment_query = "\nUNION\n".join(
            [
                f"""(
                    SELECT
                        {comment_entity}_comment.comment_id,
                        '{comment_entity}' AS entity_type,
                        {comment_entity}_comment.{comment_entity}_id AS entity_id
                    FROM {comment_entity}_comment
                    {comment_query}
                )"""
                for comment_entity, comment_query in queries_for_entity.items()
            ]
        )

        _query = f"""
            WITH top_level_comment_list AS (
                {combined_comment_query}
            ) SELECT
                c.id as comment_id,
                c.parent_id,
                c.content,
                c.status,
                tc.entity_type,
                tc.entity_id,
                al.timestamp,
                al.author
			FROM comment FOR SYSTEM_TIME ALL AS c
            JOIN top_level_comment_list tc
            ON c.id = tc.comment_id OR c.parent_id = tc.comment_id
            LEFT JOIN audit_log al
            ON al.id = c.audit_log_id
            ORDER BY c.id, al.timestamp
        """

        print(_query)

        comment_versions = await self.connection.fetch_all(
            _query, {'entity_id': entity_id}
        )

        comments: list[CommentInternal] = []

        # Group comments by their ids so that versions get included within a comment
        comment_map = {
            id: CommentInternal.from_db_versions(list(dict(v) for v in g))
            for id, g in groupby(comment_versions, key=lambda k: k['comment_id'])
        }

        # Organize threaded comments under their parents
        for _, comment in comment_map.items():
            if comment.parent_id is None:
                comments.append(comment)
            else:
                parent = comment_map.get(comment.parent_id, None)
                if parent is not None:
                    parent.add_comment_to_thread(comment)

        return comments

    async def add_comment(
        self, entity: CommentEntityType, entity_id: int, content: str
    ):

        join_table = f"{entity}_comment"
        join_column = f"{entity}_id"

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

            return 1
