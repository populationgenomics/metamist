import json
import base64
import unittest
from unittest.mock import patch, MagicMock
import etl.load.main

ETL_SAMPLE_RECORD = """
{"identifier": "AB0002", "name": "j smith", "age": 50, "measurement": "98.7", "observation": "B++", "receipt_date": "1/02/2023"}
"""


class TestEtlLoad(unittest.TestCase):
    """Test etl cloud functions"""

    @patch('etl.load.main.bq.Client', autospec=True)
    def test_etl_load_not_found_record(
        self, bq_client
    ):
        """Test etl load not found"""
        request = MagicMock(args={}, spec=['__len__', 'toJSON', 'authorization', 'get_json'])
        request.get_json.return_value = json.loads('{"request_id": "1234567890"}')

        query_job_result = MagicMock(items=[], args={}, spec=[])
        query_job_result.total_rows = 0

        query_result = MagicMock(args={}, spec=['result'])
        query_result.result.return_value = query_job_result

        bq_client_instance = bq_client.return_value
        bq_client_instance.query.return_value = query_result

        response = etl.load.main.etl_load(request)
        assert response == ({'success': False, 'message': 'Record with id: 1234567890 not found'}, 404)

    @patch('etl.load.main.bq.Client', autospec=True)
    def test_etl_load_found_record_simple_payload(
        self, bq_client
    ):
        """Test etl load simple payload"""
        request = MagicMock(args={}, spec=['__len__', 'toJSON', 'authorization', 'get_json'])
        request.get_json.return_value = json.loads('{"request_id": "1234567890"}')

        query_row = MagicMock(args={}, spec=['body'])
        query_row.body = ETL_SAMPLE_RECORD

        query_job_result = MagicMock(args={}, spec=['__iter__', '__next__'])
        query_job_result.total_rows = 1
        query_job_result.__iter__.return_value = [query_row]

        query_result = MagicMock(args={}, spec=['result'])
        query_result.result.return_value = query_job_result

        bq_client_instance = bq_client.return_value
        bq_client_instance.query.return_value = query_result

        response = etl.load.main.etl_load(request)

        assert response == {
            'id': '1234567890',
            'record': json.loads(ETL_SAMPLE_RECORD),
            'success': True
        }

    @patch('etl.load.main.bq.Client', autospec=True)
    def test_etl_load_found_record_pubsub_payload(
        self, bq_client
    ):
        """Test etl load pubsub payload"""
        request = MagicMock(args={}, spec=['__len__', 'toJSON', 'authorization', 'get_json'])

        data = {
            'request_id': '6dc4b9ae-74ee-42ee-9298-b0a51d5c6836',
            'timestamp': '2023-08-22T00:59:43.485926',
            'type': '/',
            'submitting_user': 'user@mail.com'
        }
        data_base64 = base64.b64encode(json.dumps(data).encode())
        pubsub_payload_example = {
            'deliveryAttempt': 1,
            'message': {
                'data': data_base64,
                'messageId': '8130738175803139',
                'message_id': '8130738175803139',
                'publishTime': '2023-08-22T00:59:44.062Z',
                'publish_time': '2023-08-22T00:59:44.062Z'
            },
            'subscription': 'projects/project/subscriptions/metamist-etl-subscription-ab63723'
        }

        request.get_json.return_value = pubsub_payload_example

        query_row = MagicMock(args={}, spec=['body'])
        query_row.body = ETL_SAMPLE_RECORD

        query_job_result = MagicMock(args={}, spec=['__iter__', '__next__'])
        query_job_result.total_rows = 1
        query_job_result.__iter__.return_value = [query_row]

        query_result = MagicMock(args={}, spec=['result'])
        query_result.result.return_value = query_job_result

        bq_client_instance = bq_client.return_value
        bq_client_instance.query.return_value = query_result

        response = etl.load.main.etl_load(request)

        assert response == {
            'id': '6dc4b9ae-74ee-42ee-9298-b0a51d5c6836',
            'record': json.loads(ETL_SAMPLE_RECORD),
            'success': True
        }
