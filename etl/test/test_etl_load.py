import base64
import json
from test.testbase import run_as_sync
from unittest import TestCase
from unittest.mock import MagicMock, patch

import etl.load.main
from metamist.parser.generic_metadata_parser import GenericMetadataParser

ETL_SAMPLE_RECORD_3 ={
    'config': {
        'search_locations': [],
        'project': 'milo-dev',
        'participant_column': 'individual_id',
        'sample_name_column': 'sample_id',
        'seq_type_column': 'sequencing_type',
        'default_sequencing_type': 'sequencing_type',
        'default_sample_type': 'blood',
        'default_sequencing_technology': 'short-read',
        'sample_meta_map': {
            'collection_centre': 'centre',
            'collection_date': 'collection_date',
            'collection_specimen': 'specimen',
        },
        'participant_meta_map': {},
        'assay_meta_map': {},
        'qc_meta_map': {},
    },
    'data': {
        'sample_id': '123456',
        'external_id': 'AAA000000',
        'individual_id': '678',
        'sequencing_type': 'exome',
        'collection_centre': 'ABCDEF',
        'collection_date': '2023-08-05T01:39:28.611476',
        'collection_specimen': 'blood',
    },
}


class TestGetParserInstance(GenericMetadataParser):
    def __init__(
        self,
        project: str,
    ):
        super().__init__(
            project=project,
            sample_name_column='sample',
            search_locations=[],
            participant_meta_map={},
            sample_meta_map={},
            assay_meta_map={},
            qc_meta_map={},
            allow_extra_files_in_search_path=False,
        )

    def get_sample_id(self, row) -> str:
        """Get external sample ID from row"""
        return row['sample']

    @staticmethod
    def get_info() -> tuple[str, str]:
        """
        Information about parser, including short name and version
        """
        return ('test', 'v1')


class TestEtlLoad(TestCase):
    """Test etl cloud functions"""

    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()

    @staticmethod
    def get_query_job_result(
        mock_bq,
        request_type: str | None = None,
        body: str | None = None,
        submitting_user: str | None = None,
    ):

        query_job_result = MagicMock(args={}, spec=['__iter__', '__next__'])

        if request_type or body or submitting_user:
            query_row = MagicMock(args={}, spec=['body', 'type', 'submitting_user'])
            query_row.body = body
            query_row.type = request_type
            query_row.submitting_user = submitting_user

            query_job_result.total_rows = 1
            query_job_result.__iter__.return_value = [query_row]
        else:
            query_job_result.total_rows = 0
            query_job_result.__iter__.return_value = []

        mock_bq.return_value.query.return_value.result.return_value = query_job_result

    @run_as_sync
    @patch('etl.load.main._get_bq_client', autospec=True)
    async def test_etl_load_not_found_record(self, mock_bq):
        """
        Test etl load, where the bigquery record is not found
        """

        # mock should return no rows
        self.get_query_job_result(mock_bq)

        request = MagicMock(
            args={}, spec=['__len__', 'toJSON', 'authorization', 'get_json']
        )
        request.get_json.return_value = {'request_id': '1234567890'}

        response, status = etl.load.main.etl_load(request)
        self.assertEqual(status, 412)
        self.assertEqual(
            response,
            {
                'success': False,
                'message': 'Record with id: 1234567890 not found',
            },
        )

    @patch('etl.load.main._get_bq_client', autospec=True)
    @patch('etl.load.main.get_parser_instance')
    @patch('etl.load.main.call_parser')
    def test_etl_load_found_record_simple_payload(
        self, mock_call_parser, mock_get_parser_instance, mock_bq
    ):
        """Test etl load simple payload"""

        ETL_SAMPLE_RECORD_2 = json.dumps(
            {
                'sample_id': '123456',
                'external_id': 'AAA000000',
                'individual_id': '678',
                'sequencing_type': 'exome',
                'collection_centre': 'ABCDEF',
                'collection_date': '2023-08-05T01:39:28.611476',
                'collection_specimen': 'blood',
            }
        )

        # mock the bigquery response
        self.get_query_job_result(
            mock_bq,
            request_type='/bbv/v1',
            submitting_user='user@mail.com',
            body=ETL_SAMPLE_RECORD_2,
        )

        # we test the parsing functionality below
        mock_get_parser_instance.return_value = (None, 'Parser for /bbv/v1 not found')

        # the "web" request
        request = MagicMock(
            args={}, spec=['__len__', 'toJSON', 'authorization', 'get_json']
        )
        request.get_json.return_value = {'request_id': '1234567890'}

        response, status = etl.load.main.etl_load(request)

        # etl_load will fail as bbv/v1 is invalid parser
        self.assertEqual(status, 400)
        self.assertDictEqual(
            response,
            {
                'id': '1234567890',
                'record': json.loads(ETL_SAMPLE_RECORD_2),
                'result': 'Error: Parser for /bbv/v1 not found when parsing record with id: 1234567890',
                'success': False,
            },
        )

    @patch('etl.load.main._get_bq_client', autospec=True)
    @patch('etl.load.main.get_parser_instance')
    @patch('etl.load.main.call_parser')
    def test_etl_load_found_record_pubsub_payload(
        self, mock_call_parser, mock_get_parser_instance, mock_bq
    ):
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

        # mock the bigquery response
        self.get_query_job_result(
            mock_bq=mock_bq,
            request_type='/gmp/v1',
            submitting_user='user@mail.com',
            body=json.dumps(ETL_SAMPLE_RECORD_3),
        )

        mock_get_parser_instance.return_value = (TestGetParserInstance, None)
        mock_call_parser.return_value = (etl.load.main.ParsingStatus.SUCCESS, '')

        response, status = etl.load.main.etl_load(request)

        self.assertEqual(status, 200)
        self.assertDictEqual(
            response,
            {
                'id': '6dc4b9ae-74ee-42ee-9298-b0a51d5c6836',
                'record': ETL_SAMPLE_RECORD_3,
                'result': '',
                'success': True,
            },
        )

    @patch('etl.load.main.get_accessor_config')
    @patch('etl.load.main.prepare_parser_map')
    def test_get_parser_instance_success(
        self, mock_prepare_parser_map, mock_get_accessor_config
    ):
        """Test get_parser_instance success"""

        mock_get_accessor_config.return_value = {
            'user@test.com': [
                {
                    'name': 'test/v1',
                    'default_parameters': {'project': 'test'},
                    # 'parser_name': 'test',
                }
            ]
        }

        mock_prepare_parser_map.return_value = {
            'test/v1': TestGetParserInstance,
        }

        parser, _ = etl.load.main.get_parser_instance(
            submitting_user='user@test.com',
            # note the leading slash should be stripped
            request_type='/test/v1',
            init_params={'project': 'to_override'},
        )
        self.assertIsNotNone(parser)
        self.assertIsInstance(parser, TestGetParserInstance)

        # do this assert for static analysis
        assert isinstance(parser, TestGetParserInstance)
        self.assertEqual(parser.project, 'test')

    @patch('etl.load.main.get_accessor_config')
    @patch('etl.load.main.prepare_parser_map')
    def test_get_parser_instance_different_parser_name(
        self, mock_prepare_parser_map, mock_get_accessor_config
    ):
        """Test get_parser_instance success"""

        mock_get_accessor_config.return_value = {
            'user@test.com': [
                {
                    'name': 'test/v1',
                    'default_parameters': {'project': 'test'},
                    'parser_name': 'different_parser/name',
                }
            ]
        }

        mock_prepare_parser_map.return_value = {
            'different_parser/name': TestGetParserInstance,
        }

        parser, _ = etl.load.main.get_parser_instance(
            submitting_user='user@test.com',
            request_type='/test/v1',
            init_params={'project': 'to_override'},
        )
        self.assertIsNotNone(parser)
        self.assertIsInstance(parser, TestGetParserInstance)

        # do this assert for static analysis
        assert isinstance(parser, TestGetParserInstance)
        self.assertEqual(parser.project, 'test')

    @patch('etl.load.main.get_accessor_config')
    def test_get_parser_instance_fail_no_user(self, mock_get_accessor_config):
        """Test get_parser_instance success"""

        mock_get_accessor_config.return_value = {'user@test.com': []}

        parser, error = etl.load.main.get_parser_instance(
            submitting_user='non-existent-user@different.com',
            request_type='/test/v1',
            init_params={'project': 'to_override'},
        )
        self.assertIsNone(parser)
        self.assertEqual(
            'Submitting user non-existent-user@different.com is not allowed to access any parsers',
            error,
        )

    @patch('etl.load.main.get_accessor_config')
    @patch('etl.load.main.prepare_parser_map')
    def test_get_parser_no_matching_config(
        self, mock_prepare_parser_map, mock_get_accessor_config
    ):
        """Test get_parser_instance success"""

        mock_get_accessor_config.return_value = {
            'user@test.com': [
                {
                    'name': 'test/v1',
                    'default_parameters': {'project': 'test'},
                }
            ]
        }

        # this doesn't need to be mocked as it fails before here
        mock_prepare_parser_map.return_value = {
            'test/v1': TestGetParserInstance,
        }

        parser, error = etl.load.main.get_parser_instance(
            submitting_user='user@test.com',
            request_type='test/v2',
            init_params={'project': 'to_override'},
        )
        self.assertIsNone(parser)
        self.assertEqual(
            'Submitting user user@test.com is not allowed to access test/v2',
            error,
        )

    @patch('etl.load.main.get_accessor_config')
    @patch('etl.load.main.prepare_parser_map')
    def test_get_parser_no_matching_parser(
        self, mock_prepare_parser_map, mock_get_accessor_config
    ):
        """Test get_parser_instance success"""

        mock_get_accessor_config.return_value = {
            'user@test.com': [
                {
                    'name': 'a/b',
                    'default_parameters': {'project': 'test'},
                }
            ]
        }

        mock_prepare_parser_map.return_value = {
            'c/d': TestGetParserInstance,
        }

        parser, error = etl.load.main.get_parser_instance(
            submitting_user='user@test.com',
            request_type='a/b',
            init_params={'project': 'to_override'},
        )
        self.assertIsNone(parser)
        self.assertEqual(
            'Submitting user user@test.com could not find parser for a/b',
            error,
        )
