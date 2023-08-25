import json
import unittest
from unittest.mock import patch, MagicMock
import etl.extract.main

ETL_SAMPLE_RECORD = """
{"identifier": "AB0002", "name": "j smith", "age": 50, "measurement": "98.7", "observation": "B++", "receipt_date": "1/02/2023"}
"""


class TestEtlExtract(unittest.TestCase):
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
