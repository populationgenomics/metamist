from db.python.tables.base import DbBase
from models.models.comment import CommentInternal


class CommentTable(DbBase):
    """
    Capture Comment table operations and queries
    """

    table_name = 'comment'

    async def query(self) -> list[CommentInternal]:
        """Query comments"""

        _query = f"""
            
            with top_level_comment_list as (
                select comment_id, 'sample' as entity, sample_id as entity_id
                from sample_comment sc
                join sample s
                on s.id = sc.sample_id
                where s.project = 1 
            ) select
                c.id as comment_id,
                c.parent_id,
                c.root_id,
                c.content,
                tc.entity,
                tc.entity_id,
                al.timestamp,
                al.author

            from top_level_comment_list tc
            left join {self.table_name} c
            on c.id = tc.comment_id or c.root_id = tc.comment_id
            left join audit_log al
            on al.id = c.audit_log_id
            order by comment_id


        """

        comments = await self.connection.fetch_all(_query)
        return [CommentInternal.from_db(dict(comment)) for comment in comments]
