import unittest
from io import StringIO
from unittest.mock import patch

from test.testbase import run_as_sync

from sample_metadata.parser.sample_file_map_parser import SampleFileMapParser


class TestSampleMapParser(unittest.TestCase):
    """Test the TestSampleMapParser"""

    @run_as_sync
    @patch('sample_metadata.apis.ParticipantApi.get_participant_id_map_by_external_ids')
    @patch('sample_metadata.apis.SampleApi.get_sample_id_map_by_external')
    @patch('sample_metadata.apis.SequenceApi.get_sequence_ids_for_sample_ids_by_type')
    async def test_single_row_fastq(
        self, mock_get_sequence_ids, mock_get_sample_id, mock_participant_ids
    ):
        """
        Test importing a single row, forms objects and checks response
        - MOCKS: get_sample_id_map_by_external, get_sequence_ids_for_sample_ids_by_type
        """
        mock_participant_ids.return_value = {}
        mock_get_sample_id.return_value = {}
        mock_get_sequence_ids.return_value = {}

        rows = [
            'Individual ID\tFilenames',
            '<sample-id>\t<sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz',
        ]
        parser = SampleFileMapParser(
            search_locations=[],
            # doesn't matter, we're going to mock the call anyway
            project='dev',
        )
        fs = ['<sample-id>.filename-R1.fastq.gz', '<sample-id>.filename-R2.fastq.gz']
        parser.filename_map = {k: 'gs://BUCKET/FAKE/' + k for k in fs}
        parser.skip_checking_gcs_objects = True

        file_contents = '\n'.join(rows)
        resp = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )

        participants_to_add = resp['participants']['insert']
        participants_to_update = resp['participants']['update']
        samples_to_add = resp['samples']['insert']
        samples_to_update = resp['samples']['update']
        sequencing_to_add = resp['sequences']['insert']
        sequencing_to_update = resp['sequences']['update']

        self.assertEqual(1, len(participants_to_add))
        self.assertEqual(0, len(participants_to_update))
        self.assertEqual(1, len(samples_to_add))
        self.assertEqual(0, len(samples_to_update))
        self.assertEqual(1, len(sequencing_to_add))
        self.assertEqual(0, len(sequencing_to_update))

        self.assertDictEqual({}, samples_to_add[0].meta)
        expected_sequence_dict = {
            'reads': [
                [
                    {
                        'location': 'gs://BUCKET/FAKE/<sample-id>.filename-R1.fastq.gz',
                        'basename': '<sample-id>.filename-R1.fastq.gz',
                        'class': 'File',
                        'checksum': None,
                        'size': None,
                    },
                    {
                        'location': 'gs://BUCKET/FAKE/<sample-id>.filename-R2.fastq.gz',
                        'basename': '<sample-id>.filename-R2.fastq.gz',
                        'class': 'File',
                        'checksum': None,
                        'size': None,
                    },
                ]
            ],
            'reads_type': 'fastq',
        }
        self.assertDictEqual(expected_sequence_dict, sequencing_to_add[0].meta)

    @run_as_sync
    @patch('sample_metadata.apis.ParticipantApi.get_participant_id_map_by_external_ids')
    @patch('sample_metadata.apis.SampleApi.get_sample_id_map_by_external')
    @patch('sample_metadata.apis.SequenceApi.get_sequence_ids_for_sample_ids_by_type')
    async def test_two_rows_with_provided_checksums(
        self, mock_get_sequence_ids, mock_get_sample_id, mock_participant_ids
    ):
        """
        Test importing a single row, forms objects and checks response
        - MOCKS: get_sample_id_map_by_external, get_sequence_ids_for_sample_ids_by_type
        """
        mock_participant_ids.return_value = {}
        mock_get_sample_id.return_value = {}
        mock_get_sequence_ids.return_value = {}

        rows = [
            'Individual ID\tFilenames\tChecksum',
            '<sample-id>\t<sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz\t<checksum>,<checksum2>',
            '<sample-id2>\t<sample-id2>.filename-R1.fastq.gz\t<checksum3>',
            '<sample-id2>\t<sample-id2>.filename-R2.fastq.gz\t<checksum4>',
        ]
        parser = SampleFileMapParser(
            search_locations=[],
            # doesn't matter, we're going to mock the call anyway
            project='dev',
        )
        fs = [
            '<sample-id>.filename-R1.fastq.gz',
            '<sample-id>.filename-R2.fastq.gz',
            '<sample-id2>.filename-R1.fastq.gz',
            '<sample-id2>.filename-R2.fastq.gz',
        ]
        parser.filename_map = {k: 'gs://BUCKET/FAKE/' + k for k in fs}
        parser.skip_checking_gcs_objects = True

        file_contents = '\n'.join(rows)
        resp = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )

        participants_to_add = resp['participants']['insert']
        participants_to_update = resp['participants']['update']
        samples_to_add = resp['samples']['insert']
        samples_to_update = resp['samples']['update']
        sequencing_to_add = resp['sequences']['insert']
        sequencing_to_update = resp['sequences']['update']

        self.assertEqual(2, len(participants_to_add))
        self.assertEqual(0, len(participants_to_update))
        self.assertEqual(2, len(samples_to_add))
        self.assertEqual(0, len(samples_to_update))
        self.assertEqual(2, len(sequencing_to_add))
        self.assertEqual(0, len(sequencing_to_update))

        self.assertDictEqual({}, samples_to_add[0].meta)
        expected_sequence1_reads = [
            {
                'location': 'gs://BUCKET/FAKE/<sample-id>.filename-R1.fastq.gz',
                'basename': '<sample-id>.filename-R1.fastq.gz',
                'class': 'File',
                'checksum': '<checksum>',
                'size': None,
            },
            {
                'location': 'gs://BUCKET/FAKE/<sample-id>.filename-R2.fastq.gz',
                'basename': '<sample-id>.filename-R2.fastq.gz',
                'class': 'File',
                'checksum': '<checksum2>',
                'size': None,
            },
        ]

        self.assertListEqual(
            expected_sequence1_reads, sequencing_to_add[0].meta['reads'][0]
        )

        expected_sequence2_reads = [
            {
                'location': 'gs://BUCKET/FAKE/<sample-id2>.filename-R1.fastq.gz',
                'basename': '<sample-id2>.filename-R1.fastq.gz',
                'class': 'File',
                'checksum': '<checksum3>',
                'size': None,
            },
            {
                'location': 'gs://BUCKET/FAKE/<sample-id2>.filename-R2.fastq.gz',
                'basename': '<sample-id2>.filename-R2.fastq.gz',
                'class': 'File',
                'checksum': '<checksum4>',
                'size': None,
            },
        ]
        self.assertListEqual(
            expected_sequence2_reads, sequencing_to_add[1].meta['reads'][0]
        )
