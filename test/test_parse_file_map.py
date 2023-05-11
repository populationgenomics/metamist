from io import StringIO
from unittest.mock import patch
from test.testbase import DbIsolatedTest, run_as_sync

from metamist.parser.generic_parser import ParsedParticipant
from metamist.parser.sample_file_map_parser import SampleFileMapParser


class TestSampleMapParser(DbIsolatedTest):
    """Test the TestSampleMapParser"""

    @run_as_sync
    @patch('metamist.parser.generic_parser.query_async')
    async def test_single_row_fastq(self, mock_graphql_query):
        """
        Test importing a single row, forms objects and checks response
        - MOCKS: query_async
        """
        mock_graphql_query.side_effect = self.run_graphql_query_async

        rows = [
            'Individual ID\tFilenames',
            '<sample-id>\t<sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz',
        ]
        parser = SampleFileMapParser(
            search_locations=[],
            project=self.project_name,
            default_sequencing_technology='short-read',
        )
        fs = ['<sample-id>.filename-R1.fastq.gz', '<sample-id>.filename-R2.fastq.gz']
        parser.filename_map = {k: 'gs://BUCKET/FAKE/' + k for k in fs}
        parser.skip_checking_gcs_objects = True

        file_contents = '\n'.join(rows)
        summary, participants = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )

        self.assertEqual(1, summary['participants']['insert'])
        self.assertEqual(0, summary['participants']['update'])
        self.assertEqual(1, summary['samples']['insert'])
        self.assertEqual(0, summary['samples']['update'])
        self.assertEqual(1, summary['sequencing_groups']['insert'])
        self.assertEqual(0, summary['sequencing_groups']['update'])
        self.assertEqual(1, summary['assays']['insert'])
        self.assertEqual(0, summary['assays']['update'])

        assay = participants[0].samples[0].sequencing_groups[0].assays[0]

        self.assertDictEqual({}, participants[0].samples[0].meta)
        expected_sequence_dict = {
            'reads': [
                {
                    'location': 'gs://BUCKET/FAKE/<sample-id>.filename-R1.fastq.gz',
                    'basename': '<sample-id>.filename-R1.fastq.gz',
                    'class': 'File',
                    'checksum': None,
                    'size': None,
                    'datetime_added': None,
                },
                {
                    'location': 'gs://BUCKET/FAKE/<sample-id>.filename-R2.fastq.gz',
                    'basename': '<sample-id>.filename-R2.fastq.gz',
                    'class': 'File',
                    'checksum': None,
                    'size': None,
                    'datetime_added': None,
                },
            ],
            'reads_type': 'fastq',
            'sequencing_type': 'genome',
            'sequencing_technology': 'short-read',
            'sequencing_platform': 'illumina',
        }
        self.maxDiff = None
        self.assertDictEqual(expected_sequence_dict, assay.meta)

    @run_as_sync
    @patch('metamist.parser.generic_parser.query_async')
    async def test_to_external(self, mock_graphql_query):
        """
        Test importing a single row, forms objects and checks response
        - MOCKS: query_async
        """
        mock_graphql_query.side_effect = self.run_graphql_query_async

        rows = [
            'Individual ID\tFilenames',
            '<sample-id>\t<sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz',
        ]
        parser = SampleFileMapParser(
            search_locations=[],
            project=self.project_name,
            default_sequencing_technology='short-read',
        )
        fs = ['<sample-id>.filename-R1.fastq.gz', '<sample-id>.filename-R2.fastq.gz']
        parser.filename_map = {k: 'gs://BUCKET/FAKE/' + k for k in fs}
        parser.skip_checking_gcs_objects = True

        file_contents = '\n'.join(rows)
        participants: list[ParsedParticipant]
        _, participants = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )
        for p in participants:
            p.to_sm()

    @run_as_sync
    @patch('metamist.parser.generic_parser.query_async')
    async def test_two_rows_with_provided_checksums(self, mock_graphql_query):
        """
        Test importing a single row, forms objects and checks response
        - MOCKS: get_sample_id_map_by_external, get_sequence_ids_for_sample_ids_by_type
        """
        mock_graphql_query.side_effect = self.run_graphql_query_async

        rows = [
            'Individual ID\tFilenames\tChecksum',
            '<sample-id>\t<sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz\t<checksum>,<checksum2>',
            '<sample-id2>\t<sample-id2>.filename-R1.fastq.gz\t<checksum3>',
            '<sample-id2>\t<sample-id2>.filename-R2.fastq.gz\t<checksum4>',
        ]
        parser = SampleFileMapParser(
            search_locations=[],
            # doesn't matter, we're going to mock the call anyway
            project=self.project_name,
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
        summary, participants = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )

        self.assertEqual(2, summary['participants']['insert'])
        self.assertEqual(0, summary['participants']['update'])
        self.assertEqual(2, summary['samples']['insert'])
        self.assertEqual(0, summary['samples']['update'])
        self.assertEqual(2, summary['assays']['insert'])
        self.assertEqual(0, summary['assays']['update'])
        self.maxDiff = None

        self.assertDictEqual({}, participants[0].samples[0].meta)
        expected_sequence1_reads = [
            {
                'location': 'gs://BUCKET/FAKE/<sample-id>.filename-R1.fastq.gz',
                'basename': '<sample-id>.filename-R1.fastq.gz',
                'class': 'File',
                'checksum': '<checksum>',
                'size': None,
                'datetime_added': None,
            },
            {
                'location': 'gs://BUCKET/FAKE/<sample-id>.filename-R2.fastq.gz',
                'basename': '<sample-id>.filename-R2.fastq.gz',
                'class': 'File',
                'checksum': '<checksum2>',
                'size': None,
                'datetime_added': None,
            },
        ]

        self.assertListEqual(
            expected_sequence1_reads,
            participants[0].samples[0].sequencing_groups[0].assays[0].meta['reads'],
        )

        expected_sequence2_reads = [
            {
                'location': 'gs://BUCKET/FAKE/<sample-id2>.filename-R1.fastq.gz',
                'basename': '<sample-id2>.filename-R1.fastq.gz',
                'class': 'File',
                'checksum': '<checksum3>',
                'size': None,
                'datetime_added': None,
            },
            {
                'location': 'gs://BUCKET/FAKE/<sample-id2>.filename-R2.fastq.gz',
                'basename': '<sample-id2>.filename-R2.fastq.gz',
                'class': 'File',
                'checksum': '<checksum4>',
                'size': None,
                'datetime_added': None,
            },
        ]
        self.assertListEqual(
            expected_sequence2_reads,
            participants[1].samples[0].sequencing_groups[0].assays[0].meta['reads'],
        )
