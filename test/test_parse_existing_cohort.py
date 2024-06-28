from datetime import datetime
from io import StringIO
from test.testbase import DbIsolatedTest, run_as_sync
from unittest.mock import patch

from db.python.layers import ParticipantLayer
from metamist.parser.generic_parser import ParsedParticipant
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    ParticipantUpsertInternal,
    SampleUpsertInternal,
)
from scripts.parse_existing_cohort import Columns, ExistingCohortParser


class TestExistingCohortParser(DbIsolatedTest):
    """Test the ExistingCohortParser"""

    @run_as_sync
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
    ):
        """
        Test importing a single row, forms objects and checks response
        """
        mock_graphql_query.side_effect = self.run_graphql_query_async

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
            project=self.project_name,
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

        self.assertEqual(1, summary.samples.insert)
        self.assertEqual(1, summary.assays.insert)
        self.assertEqual(0, summary.samples.update)
        self.assertEqual(0, summary.assays.update)

        sample_to_add = participants[0].samples[0]
        self.assertEqual('EXTID1234', sample_to_add.external_sid)
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
        self.maxDiff = None
        self.assertDictEqual(expected_sequence_dict, assay.meta)
        return

    @run_as_sync
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
            project=self.project_name,
            allow_missing_files=False,
            sequencing_type='genome',
        )

        parser.filename_map = {
            'HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq': '/path/to/HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq',
            'HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq': '/path/to/HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq',
        }

        file_contents = '\n'.join(rows)

        with self.assertRaises(ValueError):
            await parser.parse_manifest(
                StringIO(file_contents), delimiter='\t', dry_run=True
            )
        return

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

    #     with self.assertRaises(ValueError):
    #         await parser.parse_manifest(
    #             StringIO(file_contents), delimiter='\t', dry_run=True
    #         )
    #     return

    @run_as_sync
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
    ):
        """
        Tests ingestion for an existing sample.
        """
        mock_graphql_query.side_effect = self.run_graphql_query_async

        player = ParticipantLayer(self.connection)
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
            project=self.project_name,
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

        self.assertEqual(0, summary.samples.insert)
        self.assertEqual(1, summary.assays.insert)
        self.assertEqual(1, summary.samples.update)
        self.assertEqual(0, summary.assays.update)

        return

    @run_as_sync
    async def test_get_read_filenames_no_reads_fail(self):
        """Test ValueError is raised when allow_missing_files is False and sequencing groups have no reads"""

        single_row = {Columns.MANIFEST_FLUID_X: ''}

        parser = ExistingCohortParser(
            include_participant_column=False,
            batch_number='M01',
            search_locations=[],
            project=self.project_name,
            allow_missing_files=False,
            sequencing_type='genome',
        )
        parser.filename_map = {}

        with self.assertRaises(ValueError):
            # this will raise a ValueError because the allow_missing_files=False,
            # and there are no matching reads in the filename map
            await parser.get_read_filenames(sample_id='', row=single_row)

    @run_as_sync
    async def test_get_read_filenames_no_reads_pass(self):
        """Test when allow_missing_files is True and records with missing fastqs, no ValueError is raised"""

        single_row = {Columns.MANIFEST_FLUID_X: ''}

        parser = ExistingCohortParser(
            include_participant_column=False,
            batch_number='M01',
            search_locations=[],
            project=self.project_name,
            allow_missing_files=True,
            sequencing_type='genome',
        )
        parser.filename_map = {}

        with self.assertLogs(level='INFO') as cm:
            read_filenames = await parser.get_read_filenames(
                sample_id='', row=single_row
            )

        self.assertEqual(len(cm.output), 1)
        self.assertIn('No read files found for ', cm.output[0])

        self.assertEqual(len(read_filenames), 0)

    @run_as_sync
    async def test_genome_sequencing_type(self):
        """Test that the sequencing type is set correctly when the --sequencing-type flag is set to 'genome''"""

        # Test with 'genome'
        parser = ExistingCohortParser(
            include_participant_column=False,
            batch_number='M01',
            search_locations=[],
            project=self.project_name,
            allow_missing_files=True,
            sequencing_type='genome',
        )
        self.assertEqual(parser.default_sequencing.seq_type, 'genome')

    @run_as_sync
    async def test_exome_sequencing_type(self):
        """Test that the sequencing type is set correctly when the --sequencing-type flag is set to 'exome'"""

        # Test with 'exome'
        parser = ExistingCohortParser(
            include_participant_column=False,
            batch_number='M01',
            search_locations=[],
            project=self.project_name,
            allow_missing_files=True,
            sequencing_type='exome',
        )
        self.assertEqual(parser.default_sequencing.seq_type, 'exome')

    @run_as_sync
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
    ):
        """Test that the sequencing type is set correctly when the --sequencing-type flag is set to 'genome' or 'exome'"""

        mock_graphql_query.side_effect = self.run_graphql_query_async

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
            with self.subTest(sequencing_type=sequencing_type):
                parser = ExistingCohortParser(
                    include_participant_column=False,
                    batch_number='M01',
                    search_locations=[],
                    project=self.project_name,
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
                self.maxDiff = None
                self.assertDictEqual(expected_sequence_dict, assay.meta)
        return
