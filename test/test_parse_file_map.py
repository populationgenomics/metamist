from io import StringIO
from test.testbase import DbIsolatedTest, run_as_sync
from unittest.mock import patch

from metamist.parser.generic_parser import DefaultSequencing, ParsedParticipant
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
            default_sequencing=DefaultSequencing(),
        )
        fs = ['<sample-id>.filename-R1.fastq.gz', '<sample-id>.filename-R2.fastq.gz']
        parser.filename_map = {k: 'gs://BUCKET/FAKE/' + k for k in fs}
        parser.skip_checking_gcs_objects = True

        file_contents = '\n'.join(rows)
        summary, participants = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )

        self.assertEqual(1, summary.participants.insert)
        self.assertEqual(0, summary.participants.update)
        self.assertEqual(1, summary.samples.insert)
        self.assertEqual(0, summary.samples.update)
        self.assertEqual(1, summary.sequencing_groups.insert)
        self.assertEqual(0, summary.sequencing_groups.update)
        self.assertEqual(1, summary.assays.insert)
        self.assertEqual(0, summary.assays.update)

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
            default_sequencing=DefaultSequencing(),
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

        self.assertEqual(2, summary.participants.insert)
        self.assertEqual(0, summary.participants.update)
        self.assertEqual(2, summary.samples.insert)
        self.assertEqual(0, summary.samples.update)
        self.assertEqual(2, summary.assays.insert)
        self.assertEqual(0, summary.assays.update)
        self.maxDiff = None

        self.assertDictEqual({}, participants[0].samples[0].meta)
        expected_assay1_reads = [
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
            expected_assay1_reads,
            participants[0].samples[0].sequencing_groups[0].assays[0].meta['reads'],
        )

        expected_assay2_reads = [
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
            expected_assay2_reads,
            participants[1].samples[0].sequencing_groups[0].assays[0].meta['reads'],
        )

    @run_as_sync
    @patch('metamist.parser.generic_parser.query_async')
    async def test_valid_rna_rows(self, mock_graphql_query):
        """
        Test importing a single row of rna data
        """

        mock_graphql_query.side_effect = self.run_graphql_query_async

        rows = [
            'Sample ID\tFilenames\tType\tfacility\tlibrary\tend_type\tread_length',
            '<sample-id>\t<sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz\tpolyarna\tVCGS\tTSStrmRNA\tpaired\t151',
            '<sample-id2>\t<sample-id2>.filename-R1.fastq.gz\ttotalrna\tVCGS\tTSStrtRNA\tpaired\t151',
            '<sample-id2>\t<sample-id2>.filename-R2.fastq.gz\ttotalrna\tVCGS\tTSStrtRNA\tpaired\t151',
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
        summary, samples = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )

        self.assertEqual(0, summary.participants.insert)
        self.assertEqual(0, summary.participants.update)
        self.assertEqual(2, summary.samples.insert)
        self.assertEqual(0, summary.samples.update)
        self.assertEqual(2, summary.assays.insert)
        self.assertEqual(0, summary.assays.update)
        self.maxDiff = None

        self.assertEqual('polyarna', samples[0].sequencing_groups[0].sequencing_type)
        expected_sg1_meta = {
            'sequencing_facility': 'VCGS',
            'sequencing_library': 'TSStrmRNA',
            'read_end_type': 'paired',
            'read_length': 151,
        }
        self.assertDictEqual(
            expected_sg1_meta,
            samples[0].sequencing_groups[0].meta,
        )

        self.assertEqual('totalrna', samples[1].sequencing_groups[0].sequencing_type)
        expected_sg2_meta = {
            'sequencing_facility': 'VCGS',
            'sequencing_library': 'TSStrtRNA',
            'read_end_type': 'paired',
            'read_length': 151,
        }
        self.assertDictEqual(
            expected_sg2_meta,
            samples[1].sequencing_groups[0].meta,
        )

    @run_as_sync
    @patch('metamist.parser.generic_parser.query_async')
    async def test_invalid_rna_row(self, mock_graphql_query):
        """
        Test importing a single row of rna data
        """

        mock_graphql_query.side_effect = self.run_graphql_query_async

        rows = [
            'Sample ID\tFilenames\tType',
            '<sample-id>\t<sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz\tpolyarna',
        ]

        parser = SampleFileMapParser(
            search_locations=[],
            # doesn't matter, we're going to mock the call anyway
            project=self.project_name,
        )
        fs = [
            '<sample-id>.filename-R1.fastq.gz',
            '<sample-id>.filename-R2.fastq.gz',
        ]
        parser.filename_map = {k: 'gs://BUCKET/FAKE/' + k for k in fs}
        parser.skip_checking_gcs_objects = True

        file_contents = '\n'.join(rows)
        with self.assertRaises(ValueError):
            _, _ = await parser.parse_manifest(
                StringIO(file_contents), delimiter='\t', dry_run=True
            )

    @run_as_sync
    @patch('metamist.parser.generic_parser.query_async')
    async def test_rna_row_with_default_field_values(self, mock_graphql_query):
        """
        Test importing a single row of rna data
        """

        mock_graphql_query.side_effect = self.run_graphql_query_async

        rows = [
            'Sample ID\tFilenames\tType',
            '<sample-id>\t<sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz\tpolyarna',
        ]

        parser = SampleFileMapParser(
            search_locations=[],
            # doesn't matter, we're going to mock the call anyway
            project=self.project_name,
            default_sequencing=DefaultSequencing(facility='VCGS', library='TSStrmRNA'),
            default_read_end_type='paired',
            default_read_length=151,
        )
        fs = [
            '<sample-id>.filename-R1.fastq.gz',
            '<sample-id>.filename-R2.fastq.gz',
        ]
        parser.filename_map = {k: 'gs://BUCKET/FAKE/' + k for k in fs}
        parser.skip_checking_gcs_objects = True

        file_contents = '\n'.join(rows)
        summary, samples = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )

        self.assertEqual(0, summary.participants.insert)
        self.assertEqual(0, summary.participants.update)
        self.assertEqual(1, summary.samples.insert)
        self.assertEqual(0, summary.samples.update)
        self.assertEqual(1, summary.assays.insert)
        self.assertEqual(0, summary.assays.update)
        self.maxDiff = None

        self.assertEqual('polyarna', samples[0].sequencing_groups[0].sequencing_type)
        expected_sg1_meta = {
            'sequencing_facility': 'VCGS',
            'sequencing_library': 'TSStrmRNA',
            'read_end_type': 'paired',
            'read_length': 151,
        }
        self.assertDictEqual(
            expected_sg1_meta,
            samples[0].sequencing_groups[0].meta,
        )
