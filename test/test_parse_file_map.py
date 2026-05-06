from io import StringIO
from unittest.mock import patch

import pytest

from metamist.parser.generic_parser import DefaultSequencing, ParsedParticipant
from metamist.parser.sample_file_map_parser import SampleFileMapParser

from db.python.connect import Connection
from test.conftest import GraphQLQueryFunction, make_graphql_query_mock


class TestSampleMapParser:
    """Test the TestSampleMapParser"""

    @pytest.fixture(autouse=True)
    async def set_up(self, connection_with_project: Connection):
        assert connection_with_project.project is not None
        self.project = connection_with_project.project

    @pytest.mark.asyncio
    @patch('metamist.parser.generic_parser.query_async')
    async def test_single_row_fastq(
        self,
        mock_graphql_query,
        graphql_query: GraphQLQueryFunction,
    ):
        """
        Test importing a single row, forms objects and checks response
        - MOCKS: query_async
        """
        mock_graphql_query.side_effect = make_graphql_query_mock(graphql_query)

        rows = [
            'Individual ID\tFilenames',
            '<sample-id>\t<sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz',
        ]
        parser = SampleFileMapParser(
            search_locations=[],
            project=self.project.name,
            default_sequencing=DefaultSequencing(),
        )
        fs = ['<sample-id>.filename-R1.fastq.gz', '<sample-id>.filename-R2.fastq.gz']
        parser.filename_map = {k: 'gs://BUCKET/FAKE/' + k for k in fs}
        parser.skip_checking_gcs_objects = True

        file_contents = '\n'.join(rows)
        summary, participants = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )

        assert summary.participants.insert == 1
        assert summary.participants.update == 0
        assert summary.samples.insert == 1
        assert summary.samples.update == 0
        assert summary.sequencing_groups.insert == 1
        assert summary.sequencing_groups.update == 0
        assert summary.assays.insert == 1
        assert summary.assays.update == 0

        assay = participants[0].samples[0].sequencing_groups[0].assays[0]

        assert participants[0].samples[0].meta == {}
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
        assert assay.meta == expected_sequence_dict

    @pytest.mark.asyncio
    @patch('metamist.parser.generic_parser.query_async')
    async def test_to_external(
        self,
        mock_graphql_query,
        graphql_query: GraphQLQueryFunction,
    ):
        """
        Test importing a single row, forms objects and checks response
        - MOCKS: query_async
        """
        mock_graphql_query.side_effect = make_graphql_query_mock(graphql_query)

        rows = [
            'Individual ID\tFilenames',
            '<sample-id>\t<sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz',
        ]
        parser = SampleFileMapParser(
            search_locations=[],
            project=self.project.name,
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

    @pytest.mark.asyncio
    @patch('metamist.parser.generic_parser.query_async')
    async def test_two_rows_with_provided_checksums(
        self,
        mock_graphql_query,
        graphql_query: GraphQLQueryFunction,
    ):
        """
        Test importing a single row, forms objects and checks response
        - MOCKS: get_sample_id_map_by_external, get_sequence_ids_for_sample_ids_by_type
        """
        mock_graphql_query.side_effect = make_graphql_query_mock(graphql_query)

        rows = [
            'Individual ID\tFilenames\tChecksum',
            '<sample-id>\t<sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz\t<checksum>,<checksum2>',
            '<sample-id2>\t<sample-id2>.filename-R1.fastq.gz\t<checksum3>',
            '<sample-id2>\t<sample-id2>.filename-R2.fastq.gz\t<checksum4>',
        ]
        parser = SampleFileMapParser(
            search_locations=[],
            # doesn't matter, we're going to mock the call anyway
            project=self.project.name,
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

        assert summary.participants.insert == 2
        assert summary.participants.update == 0
        assert summary.samples.insert == 2
        assert summary.samples.update == 0
        assert summary.assays.insert == 2
        assert summary.assays.update == 0

        assert participants[0].samples[0].meta == {}
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

        assert (
            participants[0].samples[0].sequencing_groups[0].assays[0].meta['reads']
            == expected_assay1_reads
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
        assert (
            participants[1].samples[0].sequencing_groups[0].assays[0].meta['reads']
            == expected_assay2_reads
        )

    @pytest.mark.asyncio
    @patch('metamist.parser.generic_parser.query_async')
    async def test_valid_rna_rows(
        self,
        mock_graphql_query,
        graphql_query: GraphQLQueryFunction,
    ):
        """
        Test importing a single row of rna data
        """
        mock_graphql_query.side_effect = make_graphql_query_mock(graphql_query)

        rows = [
            'Sample ID\tFilenames\tType\tfacility\tlibrary\tend_type\tread_length',
            '<sample-id>\t<sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz\tpolyarna\tVCGS\tTSStrmRNA\tpaired\t151',
            '<sample-id2>\t<sample-id2>.filename-R1.fastq.gz\ttotalrna\tVCGS\tTSStrtRNA\tpaired\t151',
            '<sample-id2>\t<sample-id2>.filename-R2.fastq.gz\ttotalrna\tVCGS\tTSStrtRNA\tpaired\t151',
        ]

        parser = SampleFileMapParser(
            search_locations=[],
            # doesn't matter, we're going to mock the call anyway
            project=self.project.name,
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

        assert summary.participants.insert == 0
        assert summary.participants.update == 0
        assert summary.samples.insert == 2
        assert summary.samples.update == 0
        assert summary.assays.insert == 2
        assert summary.assays.update == 0

        assert samples[0].sequencing_groups[0].sequencing_type == 'polyarna'
        expected_sg1_meta = {
            'sequencing_facility': 'VCGS',
            'sequencing_library': 'TSStrmRNA',
            'read_end_type': 'paired',
            'read_length': 151,
        }
        assert samples[0].sequencing_groups[0].meta == expected_sg1_meta

        assert samples[1].sequencing_groups[0].sequencing_type == 'totalrna'
        expected_sg2_meta = {
            'sequencing_facility': 'VCGS',
            'sequencing_library': 'TSStrtRNA',
            'read_end_type': 'paired',
            'read_length': 151,
        }
        assert samples[1].sequencing_groups[0].meta == expected_sg2_meta

    @pytest.mark.asyncio
    @patch('metamist.parser.generic_parser.query_async')
    async def test_invalid_rna_row(
        self,
        mock_graphql_query,
        graphql_query: GraphQLQueryFunction,
    ):
        """
        Test importing a single row of rna data
        """
        mock_graphql_query.side_effect = make_graphql_query_mock(graphql_query)

        rows = [
            'Sample ID\tFilenames\tType',
            '<sample-id>\t<sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz\tpolyarna',
        ]

        parser = SampleFileMapParser(
            search_locations=[],
            # doesn't matter, we're going to mock the call anyway
            project=self.project.name,
        )
        fs = [
            '<sample-id>.filename-R1.fastq.gz',
            '<sample-id>.filename-R2.fastq.gz',
        ]
        parser.filename_map = {k: 'gs://BUCKET/FAKE/' + k for k in fs}
        parser.skip_checking_gcs_objects = True

        file_contents = '\n'.join(rows)
        with pytest.raises(ValueError):
            _, _ = await parser.parse_manifest(
                StringIO(file_contents), delimiter='\t', dry_run=True
            )

    @pytest.mark.asyncio
    @patch('metamist.parser.generic_parser.query_async')
    async def test_rna_row_with_default_field_values(
        self,
        mock_graphql_query,
        graphql_query: GraphQLQueryFunction,
    ):
        """
        Test importing a single row of rna data
        """
        mock_graphql_query.side_effect = make_graphql_query_mock(graphql_query)

        rows = [
            'Sample ID\tFilenames\tType',
            '<sample-id>\t<sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz\tpolyarna',
        ]

        parser = SampleFileMapParser(
            search_locations=[],
            # doesn't matter, we're going to mock the call anyway
            project=self.project.name,
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

        assert summary.participants.insert == 0
        assert summary.participants.update == 0
        assert summary.samples.insert == 1
        assert summary.samples.update == 0
        assert summary.assays.insert == 1
        assert summary.assays.update == 0

        assert samples[0].sequencing_groups[0].sequencing_type == 'polyarna'
        expected_sg1_meta = {
            'sequencing_facility': 'VCGS',
            'sequencing_library': 'TSStrmRNA',
            'read_end_type': 'paired',
            'read_length': 151,
        }
        assert samples[0].sequencing_groups[0].meta == expected_sg1_meta
