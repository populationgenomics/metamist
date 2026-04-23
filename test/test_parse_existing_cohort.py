import logging
from datetime import datetime
from io import StringIO
from unittest.mock import patch

import pytest

from metamist.parser.generic_parser import ParsedParticipant

from db.python.connect import Connection
from db.python.layers import ParticipantLayer
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    ParticipantUpsertInternal,
    SampleUpsertInternal,
)
from scripts.parse_existing_cohort import Columns, ExistingCohortParser
from test.conftest import GraphQLQueryFunction, make_graphql_query_mock


class TestExistingCohortParser:
    """Test the ExistingCohortParser"""

    @pytest.fixture(autouse=True)
    async def set_up(self, connection_with_project: Connection):
        assert connection_with_project.project is not None
        self.project = connection_with_project.project

    @pytest.mark.asyncio
    @patch('metamist.parser.generic_parser.query_async')
    @patch('metamist.parser.cloudhelper.CloudHelper.datetime_added')
    @patch('metamist.parser.cloudhelper.CloudHelper.file_exists')
    @patch('metamist.parser.cloudhelper.CloudHelper.file_size')
    async def test_single_row(
        self,
        mock_filesize,
        mock_fileexists,
        mock_datetime_added,
        mock_graphql_query,
        graphql_query: GraphQLQueryFunction,
    ):
        """
        Test importing a single row, forms objects and checks response
        """
        mock_graphql_query.side_effect = make_graphql_query_mock(graphql_query)

        mock_filesize.return_value = 111
        mock_fileexists.return_value = False
        mock_datetime_added.return_value = datetime.fromisoformat('2022-02-02T22:22:22')

        rows = [
            'HEADER',
            '""',
            'Application\tExternal ID\tSample Concentration (ng/ul)\tVolume (uL)\tSex\tSample/Name\tReference Genome\tParticipant ID\t',
            'App\tEXTID1234\t100\t100\tFemale\t220405_FLUIDX1234\thg38\tPID123',
        ]
        parser = ExistingCohortParser(
            include_participant_column=False,
            batch_number='M01',
            search_locations=[],
            project=self.project.name,
            allow_missing_files=False,
            sequencing_type='genome',
        )

        parser.filename_map = {
            'HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq': '/path/to/HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq',
            'HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq': '/path/to/HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq',
        }

        file_contents = '\n'.join(rows)
        participants: list[ParsedParticipant]
        summary, participants = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )

        assert summary.samples.insert == 1
        assert summary.assays.insert == 1
        assert summary.samples.update == 0
        assert summary.assays.update == 0

        sample_to_add = participants[0].samples[0]
        assert sample_to_add.primary_external_id == 'EXTID1234'
        expected_sequence_dict = {
            'reference_genome': 'hg38',
            'platform': 'App',
            'concentration': 100,
            'volume': 100,
            'fluid_x_tube_id': '220405_FLUIDX1234',
            'reads_type': 'fastq',
            'reads': [
                {
                    'location': '/path/to/HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq',
                    'basename': 'HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq',
                    'class': 'File',
                    'checksum': None,
                    'size': 111,
                    'datetime_added': '2022-02-02T22:22:22',
                },
                {
                    'location': '/path/to/HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq',
                    'basename': 'HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq',
                    'class': 'File',
                    'checksum': None,
                    'size': 111,
                    'datetime_added': '2022-02-02T22:22:22',
                },
            ],
            'sequencing_platform': 'illumina',
            'sequencing_technology': 'short-read',
            'sequencing_type': 'genome',
            'batch': 'M01',
        }
        assay = sample_to_add.sequencing_groups[0].assays[0]
        assert assay.meta == expected_sequence_dict

    @pytest.mark.asyncio
    async def test_no_header(self):
        """
        Test input without a header
        """

        rows = [
            'Application\tExternal ID\tSample Concentration (ng/ul)\tVolume (uL)\tSex\tSample/Name\tReference Genome\tParticipant ID\t',
            'App\tEXTID1234\t100\t100\tFemale\t220405_FLUIDX1234\thg38\tPID123',
        ]
        parser = ExistingCohortParser(
            include_participant_column=False,
            batch_number='M01',
            search_locations=[],
            project=self.project.name,
            allow_missing_files=False,
            sequencing_type='genome',
        )

        parser.filename_map = {
            'HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq': '/path/to/HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq',
            'HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq': '/path/to/HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq',
        }

        file_contents = '\n'.join(rows)

        with pytest.raises(ValueError):
            await parser.parse_manifest(
                StringIO(file_contents), delimiter='\t', dry_run=True
            )

    # TODO mfranklin / vivbak: this test is failing because of change in the parsers
    #   to exclude absolute paths (as absolute paths are NOT in the file map).
    #   I don't know what needs to change to fix this test, except maybe
    #   that the EC parser shouldn't return absolute paths
    # @run_as_sync
    # @patch('metamist.parser.generic_parser.query_async')
    # async def test_missing_fastqs(self, mock_graphql_query):
    #     """
    #     Tests case where the fastq's in the storage do not match the ingested samples.
    #     """
    #     mock_graphql_query.side_effect = self.run_graphql_query_async

    #     rows = [
    #         'HEADER',
    #         '""',
    #         'Application\tExternal ID\tSample Concentration (ng/ul)\tVolume (uL)\tSex\tSample/Name\tReference Genome\tParticipant ID\t',
    #         'App\tEXTID1234\t100\t100\tFemale\t220405_FLUIDX1234\thg38\tPID123',
    #     ]
    #     parser = ExistingCohortParser(
    #         include_participant_column=False,
    #         batch_number='M01',
    #         search_locations=[],
    #         project=self.project_name,
    #         allow_missing_files=False,
    #     )

    #     parser.filename_map = {
    #         'HG3F_2_220405_FLUIDXMISTMATCH1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq': '/path/to/HG3F_2_220405_FLUIDXMISMATCH1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq',
    #         'HG3F_2_220405_FLUIDXMISMATCH1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq': '/path/to/HG3F_2_220405_FLUIDXMISMATCH1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq',
    #     }

    #     file_contents = '\n'.join(rows)

    #     with pytest.raises(ValueError):
    #         await parser.parse_manifest(
    #             StringIO(file_contents), delimiter='\t', dry_run=True
    #         )
    #     return

    @pytest.mark.asyncio
    @patch('metamist.parser.generic_parser.query_async')
    @patch('metamist.parser.cloudhelper.CloudHelper.datetime_added')
    @patch('metamist.parser.cloudhelper.CloudHelper.file_exists')
    @patch('metamist.parser.cloudhelper.CloudHelper.file_size')
    async def test_existing_row(
        self,
        mock_filesize,
        mock_fileexists,
        mock_datetime_added,
        mock_graphql_query,
        connection_with_project: Connection,
        graphql_query: GraphQLQueryFunction,
    ):
        """
        Tests ingestion for an existing sample.
        """
        mock_graphql_query.side_effect = make_graphql_query_mock(graphql_query)

        player = ParticipantLayer(connection_with_project)
        await player.upsert_participants(
            [
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EXTID1234'},
                    samples=[
                        SampleUpsertInternal(
                            external_ids={PRIMARY_EXTERNAL_ORG: 'EXTID1234'},
                        )
                    ],
                )
            ]
        )

        mock_filesize.return_value = 111
        mock_fileexists.return_value = False
        mock_datetime_added.return_value = datetime.fromisoformat('2022-02-02T22:22:22')

        rows = [
            'HEADER',
            '""',
            'Application\tExternal ID\tSample Concentration (ng/ul)\tVolume (uL)\tSex\tSample/Name\tReference Genome\t',
            'App\tEXTID1234\t100\t100\tFemale\t220405_FLUIDX1234\thg38\t',
        ]
        parser = ExistingCohortParser(
            include_participant_column=False,
            batch_number='M01',
            search_locations=[],
            project=self.project.name,
            allow_missing_files=False,
            sequencing_type='genome',
        )

        parser.filename_map = {
            'HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq': '/path/to/HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq',
            'HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq': '/path/to/HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq',
        }

        file_contents = '\n'.join(rows)
        summary, _ = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )

        assert summary.samples.insert == 0
        assert summary.assays.insert == 1
        assert summary.samples.update == 1
        assert summary.assays.update == 0

    @pytest.mark.asyncio
    async def test_get_read_filenames_no_reads_fail(self):
        """Test ValueError is raised when allow_missing_files is False and sequencing groups have no reads"""

        single_row = {Columns.MANIFEST_FLUID_X: ''}

        parser = ExistingCohortParser(
            include_participant_column=False,
            batch_number='M01',
            search_locations=[],
            project=self.project.name,
            allow_missing_files=False,
            sequencing_type='genome',
        )
        parser.filename_map = {}

        with pytest.raises(ValueError):
            # this will raise a ValueError because the allow_missing_files=False,
            # and there are no matching reads in the filename map
            await parser.get_read_filenames(sample_id='', row=single_row)

    @pytest.mark.asyncio
    async def test_get_read_filenames_no_reads_pass(
        self, caplog: pytest.LogCaptureFixture
    ):
        """Test when allow_missing_files is True and records with missing fastqs, no ValueError is raised"""

        single_row = {Columns.MANIFEST_FLUID_X: ''}

        parser = ExistingCohortParser(
            include_participant_column=False,
            batch_number='M01',
            search_locations=[],
            project=self.project.name,
            allow_missing_files=True,
            sequencing_type='genome',
        )
        parser.filename_map = {}

        with caplog.at_level(logging.INFO):
            read_filenames = await parser.get_read_filenames(
                sample_id='', row=single_row
            )

        assert len(caplog.records) == 1
        assert 'No read files found for ' in caplog.records[0].message

        assert len(read_filenames) == 0

    @pytest.mark.asyncio
    async def test_genome_sequencing_type(self):
        """Test that the sequencing type is set correctly when the --sequencing-type flag is set to 'genome''"""

        # Test with 'genome'
        parser = ExistingCohortParser(
            include_participant_column=False,
            batch_number='M01',
            search_locations=[],
            project=self.project.name,
            allow_missing_files=True,
            sequencing_type='genome',
        )
        assert parser.default_sequencing.seq_type == 'genome'

    @pytest.mark.asyncio
    async def test_exome_sequencing_type(self):
        """Test that the sequencing type is set correctly when the --sequencing-type flag is set to 'exome'"""

        # Test with 'exome'
        parser = ExistingCohortParser(
            include_participant_column=False,
            batch_number='M01',
            search_locations=[],
            project=self.project.name,
            allow_missing_files=True,
            sequencing_type='exome',
        )
        assert parser.default_sequencing.seq_type == 'exome'

    @pytest.mark.asyncio
    @patch('metamist.parser.generic_parser.query_async')
    @patch('metamist.parser.cloudhelper.CloudHelper.datetime_added')
    @patch('metamist.parser.cloudhelper.CloudHelper.file_exists')
    @patch('metamist.parser.cloudhelper.CloudHelper.file_size')
    async def test_sequencing_type_in_assay_meta(
        self,
        mock_filesize,
        mock_fileexists,
        mock_datetime_added,
        mock_graphql_query,
        graphql_query: GraphQLQueryFunction,
    ):
        """Test that the sequencing type is set correctly when the --sequencing-type flag is set to 'genome' or 'exome'"""

        mock_graphql_query.side_effect = make_graphql_query_mock(graphql_query)

        mock_filesize.return_value = 111
        mock_fileexists.return_value = False
        mock_datetime_added.return_value = datetime.fromisoformat('2022-02-02T22:22:22')

        rows = [
            'HEADER',
            '""',
            'Application\tExternal ID\tSample Concentration (ng/ul)\tVolume (uL)\tSex\tSample/Name\tReference Genome\tParticipant ID\t',
            'App\tEXTID1234\t100\t100\tFemale\t220405_FLUIDX1234\thg38\tPID123',
        ]

        for sequencing_type in ['genome', 'exome']:
            parser = ExistingCohortParser(
                include_participant_column=False,
                batch_number='M01',
                search_locations=[],
                project=self.project.name,
                allow_missing_files=False,
                sequencing_type=sequencing_type,
            )
            parser.filename_map = {
                'HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq': '/path/to/HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq',
                'HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq': '/path/to/HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq',
            }

            file_contents = '\n'.join(rows)
            participants: list[ParsedParticipant]
            _, participants = await parser.parse_manifest(
                StringIO(file_contents), delimiter='\t', dry_run=True
            )

            sample_to_add = participants[0].samples[0]
            expected_sequence_dict = {
                'reference_genome': 'hg38',
                'platform': 'App',
                'concentration': 100,
                'volume': 100,
                'fluid_x_tube_id': '220405_FLUIDX1234',
                'reads_type': 'fastq',
                'reads': [
                    {
                        'location': '/path/to/HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq',
                        'basename': 'HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq',
                        'class': 'File',
                        'checksum': None,
                        'size': 111,
                        'datetime_added': '2022-02-02T22:22:22',
                    },
                    {
                        'location': '/path/to/HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq',
                        'basename': 'HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq',
                        'class': 'File',
                        'checksum': None,
                        'size': 111,
                        'datetime_added': '2022-02-02T22:22:22',
                    },
                ],
                'sequencing_platform': 'illumina',
                'sequencing_technology': 'short-read',
                'sequencing_type': f'{sequencing_type}',
                'batch': 'M01',
            }
            assay = sample_to_add.sequencing_groups[0].assays[0]
            assert assay.meta == expected_sequence_dict
