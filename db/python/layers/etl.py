import datetime
import json
import logging
from typing import Any

from db.python.gcp_connect import BqDbBase, PubSubConnection
from db.python.layers.bq_base import BqBaseLayer
from models.models import SearchItem, EtlSummary


def reformat_record(record):
    """Reformat record to be more readable"""
    
    record['details'] = json.loads(record['details'])
    record['sample_record'] = json.loads(record['sample_record'])
    
    return EtlSummary(
        request_id=record['request_id'],
        last_run_at=record['last_run_at'], 
        status=record['status'], 
        source_type=record['details']['source_type'],
        submitting_user=record['details']['submitting_user'],
        parser_result=record['details']['result'],
    )
    

class EtlLayer(BqBaseLayer):
    """Web layer"""

    async def get_etl_summary(
        self,
        grid_filter: list[SearchItem],
        token: int = 0,
        limit: int = 20,
    ) -> Any:
        """
        TODO
        """
        etlDb = EtlDb(self.connection)
        return await etlDb.get_etl_summary()


class EtlDb(BqDbBase):
    """Db layer for web related routes,"""

    async def get_etl_summary(
        self,
    ) -> list[EtlSummary]:
        """
        TODO
        """
        return await self.get_report()

    async def get_report(
        self, source_type: str = None, etl_status: str = None, start_dt: str = None
    ):
        """Get ETL report from BQ"""
        
        # build query filter
        query_filters = []

        if source_type:
            query_filters.append(
                f'JSON_VALUE(details.source_type) LIKE "\/{source_type}\/%"'
            )

        if start_dt:
            query_filters.append(f'timestamp > "{start_dt}"')

        # construct query filter
        query_filter = ' AND'.join(query_filters)
        if query_filter:
            query_filter = 'WHERE ' + query_filter

        # Status filter applied after grouping by request_id
        # One rquest can have multiple runs, we are interested only in the last run
        if etl_status:
            status_filter = f'WHERE status = "{etl_status.upper()}"'
        else:
            status_filter = ''

        # query BQ table
        _query = f"""
            WITH l AS (
            SELECT request_id, max(timestamp) as last_time
            FROM `{self._connection.gcp_project}.metamist.etl-logs` 
            {query_filter}
            group by request_id
            )
            select logs.request_id, logs.timestamp as last_run_at,
            logs.status, logs.details, d.body as sample_record
            from l 
            inner join `{self._connection.gcp_project}.metamist.etl-logs` logs on
            l.request_id = logs.request_id 
            and logs.timestamp = l.last_time
            INNER JOIN `{self._connection.gcp_project}.metamist.etl-data` d on
            d.request_id = logs.request_id
            {status_filter}
        """
        
        query_job_result = self._connection.connection.query(_query).result()
        records = [reformat_record(dict(row)) for row in query_job_result]
        return records


class EtlPubSub:
    """Etl Pub Sub wrapper"""
    
    def __init__(
        self,
        connection: PubSubConnection,
    ):
        self.connection = connection

    async def publish(
        self,
        msg: dict,
    ) -> str:
        """
        publish to pubsub, append user and timestampe to the message  
        """
        msg['timestamp'] = datetime.datetime.utcnow().isoformat()
        msg['submitting_user'] = self.connection.author
        
        try:
            self.connection.client.publish(self.connection.topic, json.dumps(msg).encode())
            return 'submitted'
        except Exception as e:  # pylint: disable=broad-exception-caught
            logging.error(f'Failed to publish to pubsub: {e}')
    
        return 'failed'
        