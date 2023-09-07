import base64
import json
from test.testbase import DbIsolatedTest, run_as_sync
from unittest.mock import MagicMock, patch

import etl.load.main
import metamist.parser as mp

ETL_SAMPLE_RECORD_1 = """
{"identifier": "AB0002", "name": "j smith", "age": 50, "measurement": "98.7", "observation": "B++", "receipt_date": "1/02/2023"}
"""

ETL_SAMPLE_RECORD_2 = """
{"sample_id": "123456", "external_id": "GRK100311", "individual_id": "608", "sequencing_type": "exome", "collection_centre": "KCCG", "collection_date": "2023-08-05T01:39:28.611476", "collection_specimen": "blood"}
"""


class TestEtlLoad(DbIsolatedTest):
    """Test etl cloud functions"""

    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()

    @run_as_sync
    @patch('etl.load.main.bq.Client', autospec=True)
    async def test_etl_load_not_found_record(self, bq_client):
        """Test etl load not found"""
        request = MagicMock(
            args={}, spec=['__len__', 'toJSON', 'authorization', 'get_json']
        )
        request.get_json.return_value = json.loads('{"request_id": "1234567890"}')

        query_job_result = MagicMock(items=[], args={}, spec=[])
        query_job_result.total_rows = 0

        query_result = MagicMock(args={}, spec=['result'])
        query_result.result.return_value = query_job_result

        bq_client_instance = bq_client.return_value
        bq_client_instance.query.return_value = query_result

        response = etl.load.main.etl_load(request)
        self.assertEqual(
            response,
            (
                {
                    'success': False,
                    'message': 'Record with id: 1234567890 not found',
                },
                412,
            ),
        )

    @run_as_sync
    @patch('etl.load.main.bq.Client', autospec=True)
    @patch('etl.load.main.call_parser')
    async def test_etl_load_found_record_simple_payload(self, call_parser, bq_client):
        """Test etl load simple payload"""
        request = MagicMock(
            args={}, spec=['__len__', 'toJSON', 'authorization', 'get_json']
        )
        request.get_json.return_value = json.loads('{"request_id": "1234567890"}')

        query_row = MagicMock(args={}, spec=['body', 'type'])
        query_row.body = ETL_SAMPLE_RECORD_2
        query_row.type = '/gmp/v1'

        query_job_result = MagicMock(args={}, spec=['__iter__', '__next__'])
        query_job_result.total_rows = 1
        query_job_result.__iter__.return_value = [query_row]

        query_result = MagicMock(args={}, spec=['result'])
        query_result.result.return_value = query_job_result

        bq_client_instance = bq_client.return_value
        bq_client_instance.query.return_value = query_result

        call_parser.return_value = ('SUCCESS', '')

        response = etl.load.main.etl_load(request)
        self.assertDictEqual(
            response,
            {
                'id': '1234567890',
                'record': json.loads(ETL_SAMPLE_RECORD_2),
                'result': "''",
                'success': True,
            },
        )

    @run_as_sync
    @patch('etl.load.main.bq.Client', autospec=True)
    @patch('etl.load.main.call_parser')
    async def test_etl_load_found_record_pubsub_payload(self, call_parser, bq_client):
        """Test etl load pubsub payload"""
        request = MagicMock(
            args={}, spec=['__len__', 'toJSON', 'authorization', 'get_json']
        )

        data = {
            'request_id': '6dc4b9ae-74ee-42ee-9298-b0a51d5c6836',
            'timestamp': '2023-08-22T00:59:43.485926',
            'type': '/',
            'submitting_user': 'user@mail.com',
        }
        data_base64 = base64.b64encode(json.dumps(data).encode())
        pubsub_payload_example = {
            'deliveryAttempt': 1,
            'message': {
                'data': data_base64,
                'messageId': '8130738175803139',
                'message_id': '8130738175803139',
                'publishTime': '2023-08-22T00:59:44.062Z',
                'publish_time': '2023-08-22T00:59:44.062Z',
            },
            'subscription': 'projects/project/subscriptions/metamist-etl-subscription-ab63723',
        }

        request.get_json.return_value = pubsub_payload_example

        query_row = MagicMock(args={}, spec=['body', 'type'])
        query_row.body = ETL_SAMPLE_RECORD_2
        query_row.type = '/gmp/v1'

        query_job_result = MagicMock(args={}, spec=['__iter__', '__next__'])
        query_job_result.total_rows = 1
        query_job_result.__iter__.return_value = [query_row]

        query_result = MagicMock(args={}, spec=['result'])
        query_result.result.return_value = query_job_result

        bq_client_instance = bq_client.return_value
        bq_client_instance.query.return_value = query_result

        call_parser.return_value = ('SUCCESS', '')

        response = etl.load.main.etl_load(request)
        self.assertDictEqual(
            response,
            {
                'id': '6dc4b9ae-74ee-42ee-9298-b0a51d5c6836',
                'record': json.loads(ETL_SAMPLE_RECORD_2),
                'result': "''",
                'success': True,
            },
        )

    @run_as_sync
    async def test_etl_load_parser(
        self,
    ):
        """Test simple parsing of json data
        Comment out if you want to test using LOCAL environment,
        """

        PARTICIPANT_COL_NAME = 'individual_id'
        SAMPLE_ID_COL_NAME = 'sample_id'
        SEQ_TYPE_COL_NAME = 'sequencing_type'

        sample_meta_map = {
            'collection_centre': 'centre',
            'collection_date': 'collection_date',
            'collection_specimen': 'specimen',
        }

        default_sequencing_type = 'genome'
        default_sequencing_technology = 'short-read'

        # parser =
        mp.GenericMetadataParser(
            search_locations=[],
            project=self.project_name,
            participant_column=PARTICIPANT_COL_NAME,
            sample_name_column=SAMPLE_ID_COL_NAME,
            reads_column=None,
            checksum_column=None,
            seq_type_column=SEQ_TYPE_COL_NAME,
            default_sequencing_type=default_sequencing_type,
            default_sample_type='blood',
            default_sequencing_technology=default_sequencing_technology,
            default_reference_assembly_location=None,
            participant_meta_map={},
            sample_meta_map=sample_meta_map,
            assay_meta_map={},
            qc_meta_map={},
            allow_extra_files_in_search_path=None,
            key_map=None,
        )

        # json_data = [
        #     {
        #         'sample_id': '123456',
        #         'external_id': 'GRK100311',
        #         'individual_id': '608',
        #         'sequencing_type': 'exome',
        #         'collection_centre': 'KCCG',
        #         'collection_date': '2023-08-05T01:39:28.611476',
        #         'collection_specimen': 'blood',
        #     }
        # ]
        # res = await parser.from_json(json_data, confirm=False, dry_run=True)
        # print(res)

        # assert False
