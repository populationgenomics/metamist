import unittest
from io import StringIO
from datetime import datetime
from unittest.mock import patch
from test.testbase import run_as_sync

from scripts.parse_existing_cohort import ExistingCohortParser


class TestExistingCohortParser(unittest.TestCase):
    """Test the ExistingCohortParser"""

    @run_as_sync
    @patch('sample_metadata.apis.SampleApi.get_sample_id_map_by_external')
    @patch('sample_metadata.apis.ParticipantApi.get_participant_id_map_by_external_ids')
    @patch('sample_metadata.apis.SequenceApi.get_sequence_ids_for_sample_ids_by_type')
    @patch('sample_metadata.parser.cloudhelper.CloudHelper.datetime_added')
    @patch('sample_metadata.parser.cloudhelper.CloudHelper.file_exists')
    @patch('sample_metadata.parser.cloudhelper.CloudHelper.file_size')
    async def test_single_row(
        self,
        mock_filesize,
        mock_fileexists,
        mock_datetime_added,
        mock_get_sequence_ids,
        mock_get_sample_id,
        mock_get_participant_id,
    ):
        """
        Test importing a single row, forms objects and checks response
        """
        mock_get_sample_id.return_value = {}
        mock_get_sequence_ids.return_value = {}
        mock_get_participant_id.return_value = {}

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
            project='to-be-mocked-dev',
        )

        parser.filename_map = {
            'HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq': '/path/to/HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq',
            'HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq': '/path/to/HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq',
        }

        file_contents = '\n'.join(rows)
        resp = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )

        self.assertEqual(1, len(resp['samples']['insert']))
        self.assertEqual(1, len(resp['sequences']['insert']))
        self.assertEqual(0, len(resp['samples']['update']))
        self.assertEqual(0, len(resp['sequences']['update']))

        samples_to_add = resp['samples']['insert']
        sequences_to_add = resp['sequences']['insert']

        self.assertEqual('EXTID1234', samples_to_add[0].external_id)
        expected_sequence_dict = {
            'reference_genome': 'hg38',
            'platform': 'App',
            'concentration': '100',
            'volume': '100',
            'fluid_x_tube_id': '220405_FLUIDX1234',
            'reads_type': 'fastq',
            'reads': [
                [
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
                ]
            ],
            'batch': 'M01',
        }
        self.assertDictEqual(expected_sequence_dict, sequences_to_add[0].meta)
        return

    @run_as_sync
    async def test_no_header(
        self,
    ):
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
            project='to-be-mocked-dev',
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

    @run_as_sync
    @patch('sample_metadata.apis.SampleApi.get_sample_id_map_by_external')
    @patch('sample_metadata.apis.ParticipantApi.get_participant_id_map_by_external_ids')
    @patch('sample_metadata.apis.SequenceApi.get_sequence_ids_for_sample_ids_by_type')
    async def test_missing_fastqs(
        self,
        mock_get_sequence_ids,
        mock_get_sample_id,
        mock_get_participant_id,
    ):
        """
        Tests case where the fastq's in the storage do not match the ingested samples.
        """
        mock_get_sample_id.return_value = {}
        mock_get_sequence_ids.return_value = {}
        mock_get_participant_id.return_value = {}

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
            project='to-be-mocked-dev',
        )

        parser.filename_map = {
            'HG3F_2_220405_FLUIDXMISTMATCH1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq': '/path/to/HG3F_2_220405_FLUIDXMISMATCH1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq',
            'HG3F_2_220405_FLUIDXMISMATCH1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq': '/path/to/HG3F_2_220405_FLUIDXMISMATCH1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq',
        }

        file_contents = '\n'.join(rows)

        with self.assertRaises(ValueError):
            await parser.parse_manifest(
                StringIO(file_contents), delimiter='\t', dry_run=True
            )
        return

    @run_as_sync
    @patch('sample_metadata.apis.SampleApi.get_sample_id_map_by_external')
    @patch('sample_metadata.apis.ParticipantApi.get_participant_id_map_by_external_ids')
    @patch('sample_metadata.apis.SequenceApi.get_sequences_by_sample_ids')
    @patch('sample_metadata.apis.SequenceApi.get_sequence_ids_for_sample_ids_by_type')
    @patch('sample_metadata.parser.cloudhelper.CloudHelper.datetime_added')
    @patch('sample_metadata.parser.cloudhelper.CloudHelper.file_exists')
    @patch('sample_metadata.parser.cloudhelper.CloudHelper.file_size')
    async def test_existing_row(
        self,
        mock_filesize,
        mock_fileexists,
        mock_datetime_added,
        mock_get_sequence_ids,
        mock_get_sequences_by_sample_ids,
        mock_get_participant_id,
        mock_get_sample_id,
    ):
        """
        Tests ingestion for an existing sample.
        """
        mock_get_sample_id.return_value = {'EXTID1234': 'CPG123'}
        mock_get_sequence_ids.return_value = {}
        mock_get_sequences_by_sample_ids.return_value = {}
        mock_get_participant_id.return_value = {'EXTID1234': 1234}

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
            project='to-be-mocked-dev',
        )

        parser.filename_map = {
            'HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq': '/path/to/HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R1.fastq',
            'HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq': '/path/to/HG3F_2_220405_FLUIDX1234_Homo-sapiens_AAC-TAT_R_220208_VB_BLAH_M002_R2.fastq',
        }

        file_contents = '\n'.join(rows)
        resp = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )

        self.assertEqual(0, len(resp['samples']['insert']))
        self.assertEqual(1, len(resp['sequences']['insert']))
        self.assertEqual(1, len(resp['samples']['update']))
        self.assertEqual(0, len(resp['sequences']['update']))

        return
