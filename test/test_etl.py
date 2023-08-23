import json
import unittest
from unittest.mock import patch, MagicMock

import etl.extract.main
import etl.load.main

ETL_SAMPLE_RECORD = """
{"identifier": "AB0002", "name": "j smith", "age": 50, "measurement": "98.7", "observation": "B++", "receipt_date": "1/02/2023"}
"""


class TestEtl(unittest.TestCase):
    """Test etl cloud functions"""

    @patch('etl.extract.main.email_from_id_token')
    @patch('etl.extract.main.uuid.uuid4')
    @patch('etl.extract.main.bq.Client', autospec=True)
    @patch('etl.extract.main.pubsub_v1.PublisherClient', autospec=True)
    def test_etl_extract_valid_payload(
        self, pubsub_client, bq_client, uuid4_fun, email_from_id_token_fun
    ):
        """Test etl extract valid payload"""
        request = MagicMock(args={}, spec=['__len__', 'toJSON', 'authorization'])
        request.json = json.loads(ETL_SAMPLE_RECORD)
        request.path = ''

        bq_client_instance = bq_client.return_value
        bq_client_instance.insert_rows_json.return_value = None

        pubsub_client_instance = pubsub_client.return_value
        pubsub_client_instance.publish.return_value = None

        email_from_id_token_fun.return_value = 'test@email.com'
        uuid4_fun.return_value = '1234567890'

        response = etl.extract.main.etl_extract(request)

        assert response == {'id': uuid4_fun.return_value, 'success': True}

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

        pubsub_payload_example = {
            'deliveryAttempt': 1,
            'message': {
                'data': 'eyJyZXF1ZXN0X2lkIjogIjZkYzRiOWFlLTc0ZWUtNDJlZS05Mjk4LWIwYTUxZDVjNjgzNiIsICJ0aW1lc3RhbXAiOiAiMjAyMy0wOC0yMlQwMDo1OTo0My40ODU5MjYiLCAidHlwZSI6ICIvIiwgInN1Ym1pdHRpbmdfdXNlciI6ICJtaWxvc2xhdi5oeWJlbkBwb3B1bGF0aW9uZ2Vub21pY3Mub3JnLmF1In0=',
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
