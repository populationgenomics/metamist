import datetime
import json
import logging
from typing import Any

from fastapi import HTTPException

from db.python.gcp_connect import BqDbBase, PubSubConnection
from db.python.layers.bq_base import BqBaseLayer
from models.models import EtlRecord
from models.enums import EtlStatus


class EtlLayer(BqBaseLayer):
    """ETL layer"""

    async def get_etl_summary(
        self,
        source_type: str | None = None,
        status: EtlStatus | None = None,
        from_date: str | None = None,
    ) -> list[EtlRecord]:
        """
        Get ETL Summary for the given source_type
        """
        etlDb = EtlDb(self.connection)
        return await etlDb.get_etl_summary(source_type, status, from_date)
    
    async def get_etl_record(
        self,
        request_id: str,
    ) -> Any:
        """
        Get ETL record for the given request_id
        """
        etlDb = EtlDb(self.connection)
        return await etlDb.get_etl_record(request_id)


class EtlDb(BqDbBase):
    """Db layer for web related routes,"""

    async def get_etl_record(
        self,
        request_id: str,
    ) -> EtlRecord | None:
        
        _query = f"""
            WITH l AS (
            SELECT request_id, max(timestamp) as last_time
            FROM `{self._connection.gcp_project}.metamist.etl-logs` 
            WHERE request_id = "{request_id}"
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
        """
        
        query_job_result = list(self._connection.connection.query(_query).result())
        
        if query_job_result:
            return EtlRecord.from_json(dict(query_job_result[0]))
        
        raise ValueError('No record found')
    

    async def get_etl_summary(
        self,
        source_type: str | None = None,
        status: EtlStatus | None = None,
        from_date: str | None = None,
    ) -> list[EtlRecord]:
        """
        TODO - add more information into summary
        """
        return await self.get_report(source_type, status, from_date)

    async def get_report(
        self,
        source_type: str | None = None,
        status: EtlStatus | None = None,
        from_date: str | None = None,
    ) -> list[EtlRecord]:
        """Get ETL report from BQ"""
        
        # build query filter
        query_filters = []

        # filter by source_type, ignore the version
        if source_type:
            query_filters.append(
                f'JSON_VALUE(details.source_type) LIKE "/{source_type}/%"'
            )

        if from_date:
            query_filters.append(f'timestamp > "{from_date}"')

        # combined into one query filter
        query_filter = ' AND'.join(query_filters)
        if query_filter:
            query_filter = 'WHERE ' + query_filter

        # Status filter applied after grouping by request_id
        # One request can have multiple runs, we are interested only in the last run
        if status:
            status_filter = f'WHERE status = "{status.value}"'
        else:
            status_filter = ''

        # query BQ table
        # group by request_id to get the last run
        # join with etl-data to get the sample record
        # and apply status filter if any
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
        
        print("\n=====================", _query, "\n=====================\n")
        
        query_job_result = self._connection.connection.query(_query).result()
        records = [EtlRecord.from_json(dict(row)) for row in query_job_result]
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
    ) -> bool:
        """
        publish to pubsub, append user and timestampe to the message  
        """
        msg['timestamp'] = datetime.datetime.utcnow().isoformat()
        msg['submitting_user'] = self.connection.author
        
        print(msg)
        
        try:
            self.connection.client.publish(self.connection.topic, json.dumps(msg).encode())
            logging.info(f'Published message to {self.connection.topic}.')
            return True
        except Exception as e:  # pylint: disable=broad-exception-caught
            logging.error(f'Failed to publish to pubsub: {e}')
            
        return False