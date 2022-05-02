import unittest
from io import StringIO
from unittest.mock import patch
from test.testbase import run_test_as_sync

from sample_metadata.parser.generic_metadata_parser import GenericMetadataParser


class TestParseGenericMetadata(unittest.TestCase):
    """Test the GenericMetadataParser"""

    @run_test_as_sync
    @patch('sample_metadata.apis.SampleApi.get_sample_id_map_by_external')
    @patch('sample_metadata.apis.SequenceApi.get_sequence_ids_from_sample_ids')
    @patch('os.path.getsize')
    async def test_single_row(
        self, mock_stat_size, mock_get_sequence_ids, mock_get_sample_id
    ):
        """
        Test importing a single row, forms objects and checks response
        - MOCKS: get_sample_id_map_by_external, get_sequence_ids_from_sample_ids
        """
        mock_get_sample_id.return_value = {}
        mock_get_sequence_ids.return_value = {}
        mock_stat_size.return_value = 111

        rows = [
            'GVCF\tCRAM\tSampleId\tsample.flowcell_lane\tsample.platform\tsample.centre\tsample.reference_genome\traw_data.FREEMIX\traw_data.PCT_CHIMERAS\traw_data.MEDIAN_INSERT_SIZE\traw_data.MEDIAN_COVERAGE',
            '<sample-id>.g.vcf.gz\t<sample-id>.cram\t<sample-id>\tHK7NFCCXX.1\tILLUMINA\tKCCG\thg38\t0.01\t0.01\t400\t30',
        ]
        parser = GenericMetadataParser(
            search_locations=[],
            sample_name_column='SampleId',
            participant_meta_map={},
            sample_meta_map={'sample.centre': 'centre'},
            sequence_meta_map={
                'raw_data.FREEMIX': 'qc.freemix',
                'raw_data.PCT_CHIMERAS': 'qc.pct_chimeras',
                'raw_data.MEDIAN_INSERT_SIZE': 'qc.median_insert_size',
                'raw_data.MEDIAN_COVERAGE': 'qc.median_coverage',
            },
            qc_meta_map={
                'raw_data.FREEMIX': 'freemix',
                'raw_data.PCT_CHIMERAS': 'pct_chimeras',
                'raw_data.MEDIAN_INSERT_SIZE': 'median_insert_size',
                'raw_data.MEDIAN_COVERAGE': 'median_coverage',
            },
            # doesn't matter, we're going to mock the call anyway
            sample_metadata_project='devdev',
            reads_column='CRAM',
            gvcf_column='GVCF',
        )

        parser.filename_map = {
            '<sample-id>.g.vcf.gz': '/path/to/<sample-id>.g.vcf.gz',
            '<sample-id>.cram': '/path/to/<sample-id>.cram',
        }

        file_contents = '\n'.join(rows)
        resp = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )

        self.assertEqual(1, len(resp['samples']['insert']))
        self.assertEqual(1, len(resp['sequences']['insert']))
        self.assertEqual(0, len(resp['samples']['update']))
        self.assertEqual(0, len(resp['sequences']['update']))
        self.assertEqual(1, len(sum(resp['analyses'].values(), [])))

        samples_to_add = resp['samples']['insert']
        sequences_to_add = resp['sequences']['insert']
        analyses_to_add = resp['analyses']

        self.assertDictEqual({'centre': 'KCCG'}, samples_to_add[0].meta)
        expected_sequence_dict = {
            'qc': {
                'median_insert_size': '400',
                'median_coverage': '30',
                'freemix': '0.01',
                'pct_chimeras': '0.01',
            },
            'reads': [
                {
                    'location': '/path/to/<sample-id>.cram',
                    'basename': '<sample-id>.cram',
                    'class': 'File',
                    'checksum': None,
                    'size': 111,
                }
            ],
            'reads_type': 'cram',
            'gvcfs': [
                {
                    'location': '/path/to/<sample-id>.g.vcf.gz',
                    'basename': '<sample-id>.g.vcf.gz',
                    'class': 'File',
                    'checksum': None,
                    'size': 111,
                }
            ],
            'gvcf_types': 'gvcf',
        }
        self.assertDictEqual(expected_sequence_dict, sequences_to_add[0].meta)
        analysis = analyses_to_add['<sample-id>'][0]
        self.assertDictEqual(
            {
                'median_insert_size': '400',
                'median_coverage': '30',
                'freemix': '0.01',
                'pct_chimeras': '0.01',
            },
            analysis.meta,
        )

    @run_test_as_sync
    @patch('sample_metadata.apis.SampleApi.get_sample_id_map_by_external')
    @patch('sample_metadata.apis.SequenceApi.get_sequence_ids_from_sample_ids')
    @patch('sample_metadata.apis.ParticipantApi.get_participant_id_map_by_external_ids')
    @patch('os.path.getsize')
    async def test_rows_with_participants(
        self,
        mock_stat_size,
        mock_get_sequence_ids,
        mock_get_sample_id,
        mock_get_participant_id_map_by_external_ids,
    ):
        """
        Test importing a single row with a participant id, forms objects and checks response
        - MOCKS: get_sample_id_map_by_external, get_sequence_ids_from_sample_ids
        """
        mock_get_sample_id.return_value = {}
        mock_get_sequence_ids.return_value = {}
        mock_stat_size.return_value = 111
        mock_get_participant_id_map_by_external_ids.return_value = {}

        rows = [
            'Individual ID\tSample ID\tFilenames\tType',
            'Demeter\tsample_id001\tsample_id001.filename-R1.fastq.gz,sample_id001.filename-R2.fastq.gz\tWGS',
            'Demeter\tsample_id001\tsample_id001.exome.filename-R1.fastq.gz,sample_id001.exome.filename-R2.fastq.gz\tWES',
            'Apollo\tsample_id002\tsample_id002.filename-R1.fastq.gz\tWGS',
            'Apollo\tsample_id002\tsample_id002.filename-R2.fastq.gz\tWGS',
            'Athena\tsample_id003\tsample_id003.filename-R1.fastq.gz',
            'Athena\tsample_id003\tsample_id003.filename-R2.fastq.gz',
            'Apollo\tsample_id004\tsample_id004.filename-R1.fastq.gz',
            'Apollo\tsample_id004\tsample_id004.filename-R2.fastq.gz',
        ]

        parser = GenericMetadataParser(
            search_locations=[],
            participant_column='Individual ID',
            sample_name_column='Sample ID',
            reads_column='Filenames',
            seq_type_column='Type',
            participant_meta_map={},
            sample_meta_map={},
            sequence_meta_map={},
            qc_meta_map={},
            # doesn't matter, we're going to mock the call anyway
            sample_metadata_project='devdev',
        )

        parser.filename_map = {
            'sample_id001.filename-R1.fastq.gz': '/path/to/sample_id001.filename-R1.fastq.gz',
            'sample_id001.filename-R2.fastq.gz': '/path/to/sample_id001.filename-R2.fastq.gz',
            'sample_id001.exome.filename-R1.fastq.gz': '/path/to/sample_id001.exome.filename-R1.fastq.gz',
            'sample_id001.exome.filename-R2.fastq.gz': '/path/to/sample_id001.exome.filename-R2.fastq.gz',
            'sample_id002.filename-R1.fastq.gz': '/path/to/sample_id002.filename-R1.fastq.gz',
            'sample_id002.filename-R2.fastq.gz': '/path/to/sample_id002.filename-R2.fastq.gz',
            'sample_id003.filename-R1.fastq.gz': '/path/to/sample_id003.filename-R1.fastq.gz',
            'sample_id003.filename-R2.fastq.gz': '/path/to/sample_id003.filename-R2.fastq.gz',
            'sample_id004.filename-R1.fastq.gz': '/path/to/sample_id004.filename-R1.fastq.gz',
            'sample_id004.filename-R2.fastq.gz': '/path/to/sample_id004.filename-R2.fastq.gz',
        }

        # Call generic parser
        file_contents = '\n'.join(rows)
        resp = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )

        self.assertEqual(3, len(resp['participants']['insert']))
        self.assertEqual(0, len(resp['participants']['update']))
        self.assertEqual(4, len(resp['samples']['insert']))
        self.assertEqual(0, len(resp['samples']['update']))
        self.assertEqual(5, len(resp['sequences']['insert']))
        self.assertEqual(0, len(resp['sequences']['update']))
        self.assertEqual(0, len(sum(resp['analyses'].values(), [])))

        participants_to_add = resp['participants']['insert']
        sequences_to_add = resp['sequences']['insert']

        expected_sequence_dict = {
            'reads': [
                [
                    {
                        'basename': 'sample_id001.filename-R1.fastq.gz',
                        'checksum': None,
                        'class': 'File',
                        'location': '/path/to/sample_id001.filename-R1.fastq.gz',
                        'size': 111,
                    },
                    {
                        'basename': 'sample_id001.filename-R2.fastq.gz',
                        'checksum': None,
                        'class': 'File',
                        'location': '/path/to/sample_id001.filename-R2.fastq.gz',
                        'size': 111,
                    },
                ]
            ],
            'reads_type': 'fastq',
        }
        self.assertDictEqual(expected_sequence_dict, sequences_to_add[0].meta)

        # Check that both of Demeter's sequences are there
        self.assertEqual(participants_to_add[0].external_id, 'Demeter')
        self.assertEqual(len(participants_to_add[0].samples), 1)
        self.assertEqual(len(participants_to_add[0].samples[0].sequences), 2)

        return

    @run_test_as_sync
    @patch('sample_metadata.apis.SampleApi.get_sample_id_map_by_external')
    @patch('sample_metadata.apis.SequenceApi.get_sequence_ids_from_sample_ids')
    @patch('sample_metadata.apis.ParticipantApi.get_participant_id_map_by_external_ids')
    async def test_rows_with_valid_participant_meta(
        self,
        mock_get_sequence_ids,
        mock_get_sample_id,
        mock_get_participant_id_map_by_external_ids,
    ):
        """
        Test importing a several rows with a participant metadata (reported gender, sex and karyotype),
        forms objects and checks response
        - MOCKS: get_sample_id_map_by_external,  get_participant_id_map_by_external_ids,
        get_sequence_ids_from_sample_ids
        """

        mock_get_sample_id.return_value = {}
        mock_get_sequence_ids.return_value = {}
        mock_get_participant_id_map_by_external_ids.return_value = {}

        rows = [
            'Individual ID\tSample ID\tSex\tGender\tKaryotype',
            'Demeter\tsample_id001\tMale\tNon-binary\tXY',
            'Apollo\tsample_id002\tFemale\tFemale\tXX',
            'Athena\tsample_id003\tFEMalE',
            'Dionysus\tsample_id00x\t\tMale\tXX',
            'Pluto\tsample_id00y',
        ]

        parser = GenericMetadataParser(
            search_locations=[],
            participant_column='Individual ID',
            sample_name_column='Sample ID',
            participant_meta_map={},
            sample_meta_map={},
            sequence_meta_map={},
            qc_meta_map={},
            reported_sex_column='Sex',
            reported_gender_column='Gender',
            karyotype_column='Karyotype',
            # doesn't matter, we're going to mock the call anyway
            sample_metadata_project='devdev',
        )

        # Call generic parser
        file_contents = '\n'.join(rows)
        resp = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )
        participants_to_add = resp['participants']['insert']

        # Assert that the participant meta is there.
        self.assertEqual(participants_to_add[0].reported_gender, 'Non-binary')
        self.assertEqual(participants_to_add[0].reported_sex, 1)
        self.assertEqual(participants_to_add[0].karyotype, 'XY')
        self.assertEqual(participants_to_add[1].reported_gender, 'Female')
        self.assertEqual(participants_to_add[1].reported_sex, 2)
        self.assertEqual(participants_to_add[1].karyotype, 'XX')
        self.assertEqual(participants_to_add[2].reported_sex, 2)
        self.assertEqual(participants_to_add[2].get('reported_gender'), None)
        self.assertEqual(participants_to_add[2].get('karyotype'), None)
        self.assertEqual(participants_to_add[3].reported_gender, 'Male')
        self.assertEqual(participants_to_add[3].karyotype, 'XX')
        return

    @run_test_as_sync
    @patch('sample_metadata.apis.SampleApi.get_sample_id_map_by_external')
    @patch('sample_metadata.apis.ParticipantApi.get_participant_id_map_by_external_ids')
    async def test_rows_with_invalid_participant_meta(
        self,
        mock_get_sample_id,
        mock_get_participant_id_map_by_external_ids,
    ):
        """
        Test importing a single rows with invalid participant metadata,
        forms objects and checks response
        - MOCKS: get_sample_id_map_by_external, get_participant_id_map_by_external_ids
        """

        mock_get_sample_id.return_value = {}
        mock_get_participant_id_map_by_external_ids.return_value = {}

        rows = [
            'Individual ID\tSample ID\tSex\tKaryotype',
            'Athena\tsample_id003\tFemalee\tXX',
        ]

        parser = GenericMetadataParser(
            search_locations=[],
            participant_column='Individual ID',
            sample_name_column='Sample ID',
            participant_meta_map={},
            sample_meta_map={},
            sequence_meta_map={},
            qc_meta_map={},
            reported_sex_column='Sex',
            karyotype_column='Karyotype',
            # doesn't matter, we're going to mock the call anyway
            sample_metadata_project='devdev',
        )

        # Call generic parser
        file_contents = '\n'.join(rows)
        with self.assertRaises(ValueError):
            await parser.parse_manifest(
                StringIO(file_contents), delimiter='\t', dry_run=True
            )
        return
