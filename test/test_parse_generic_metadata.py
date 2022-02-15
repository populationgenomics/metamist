import unittest
from io import StringIO
from unittest.mock import patch

from sample_metadata.parser.generic_metadata_parser import GenericMetadataParser


class TestParseGenericMetadata(unittest.TestCase):
    """Test the GenericMetadataParser"""

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
            sample_metadata_project='dev',
            reads_column='CRAM',
            gvcf_column='GVCF',
        )
        file_contents = '\n'.join(rows)
        resp = await parser.parse_manifest(
            StringIO(file_contents), delimiter='\t', dry_run=True
        )

        (
            samples_to_add,
            sequencing_to_add,
            samples_to_update,
            sequencing_to_update,
            analyses_to_add,
        ) = resp

        self.assertEqual(1, len(samples_to_add))
        self.assertEqual(1, len(sequencing_to_add))
        self.assertEqual(0, len(samples_to_update))
        self.assertEqual(0, len(sequencing_to_update))
        self.assertEqual(1, len(analyses_to_add))

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
                    'location': '<sample-id>.cram',
                    'basename': '<sample-id>.cram',
                    'class': 'File',
                    'checksum': None,
                    'size': 111,
                }
            ],
            'reads_type': 'cram',
            'gvcfs': [
                {
                    'location': '<sample-id>.g.vcf.gz',
                    'basename': '<sample-id>.g.vcf.gz',
                    'class': 'File',
                    'checksum': None,
                    'size': 111,
                }
            ],
            'gvcf_types': 'gvcf',
        }
        self.assertDictEqual(
            expected_sequence_dict, sequencing_to_add['<sample-id>'].meta
        )
        self.assertDictEqual(
            {
                'median_insert_size': '400',
                'median_coverage': '30',
                'freemix': '0.01',
                'pct_chimeras': '0.01',
            },
            analyses_to_add['<sample-id>'][0].meta,
        )
