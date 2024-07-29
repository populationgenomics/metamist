from itertools import groupby

from db.python.tables.base import DbBase
from models.models.comment import CommentInternal


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
