import unittest
from io import StringIO
from unittest.mock import patch

from sample_metadata.parser.sample_file_map_parser import SampleFileMapParser


class TestSampleMapParser(unittest.TestCase):
    """Test the TestSampleMapParser"""

    @patch('sample_metadata.apis.SampleApi.get_sample_id_map_by_external')
    @patch('sample_metadata.apis.SequenceApi.get_sequence_ids_from_sample_ids')
    @patch('os.path.getsize')
    async def test_single_row_fastq(
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
            'Individual ID\tFilenames',
            '<sample-id>\t<sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz',
        ]
        parser = SampleFileMapParser(
            search_locations=[],
            # doesn't matter, we're going to mock the call anyway
            sample_metadata_project='dev',
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
        self.assertEqual(0, len(analyses_to_add))

        self.assertDictEqual({}, samples_to_add[0].meta)
        expected_sequence_dict = {
            'reads': [
                [
                    {
                        'location': '<sample-id>.filename-R1.fastq.gz',
                        'basename': '<sample-id>.filename-R1.fastq.gz',
                        'class': 'File',
                        'checksum': None,
                        'size': 111,
                    },
                    {
                        'location': '<sample-id>.filename-R2.fastq.gz',
                        'basename': '<sample-id>.filename-R2.fastq.gz',
                        'class': 'File',
                        'checksum': None,
                        'size': 111,
                    },
                ]
            ],
            'reads_type': 'fastq',
        }
        self.assertDictEqual(
            expected_sequence_dict, sequencing_to_add['<sample-id>'].meta
        )
