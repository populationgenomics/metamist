from itertools import groupby

from db.python.tables.base import DbBase
from models.models.comment import CommentInternal

entity_table_map = {'sample': ('sample_comment', 'sample_id')}


class CommentTable(DbBase):
    """
    Capture Comment table operations and queries
    """

    async def query(self) -> list[CommentInternal]:
        """Query comments"""

        _query = """
            with top_level_comment_list as (
                select
                    comment_id,
                    'sample' as entity_type,
                    sample_id as entity_id
                from sample_comment sc
                join sample s
                on s.id = sc.sample_id
                where s.project = 1
            ) select
                c.id as comment_id,
                c.parent_id,
                c.content,
                tc.entity_type,
                tc.entity_id,
                al.timestamp,
                al.author,
                cc.id is null as is_deleted
			from comment FOR SYSTEM_TIME ALL as c
            -- Join with the comments table with just the current state to see
            -- if this row is a deleted comment
            left join comment cc
            on cc.id = c.id
            join top_level_comment_list tc
            on c.id = tc.comment_id or c.parent_id = tc.comment_id
            left join audit_log al
            on al.id = c.audit_log_id
            order by c.id, al.timestamp
        """

        comment_versions = await self.connection.fetch_all(_query)

        return [
            CommentInternal.from_db_versions(list(dict(v) for v in g))
            for _, g in groupby(comment_versions, key=lambda k: k['comment_id'])
        ]

    async def add_comment(self, entity: str, entity_id: int, content: str):

        if entity not in entity_table_map:
            raise ValueError(f'Unknown entity {entity}')

        join_table, join_column = entity_table_map[entity]

        comment_insert = """
            INSERT INTO comment (content, audit_log_id)
            VALUES (:content, :audit_log_id) RETURNING id;
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
