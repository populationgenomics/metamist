import unittest
from io import StringIO
from unittest.mock import patch

from test.testbase import run_test_as_sync

from scripts.parse_ont_sheet import OntParser


class TestOntSampleSheetParser(unittest.TestCase):
    """Test the TestOntSampleSheetParser"""

    @run_test_as_sync
    @patch('sample_metadata.apis.SampleApi.get_sample_id_map_by_external')
    @patch('sample_metadata.apis.SequenceApi.get_sequence_ids_from_sample_ids')
    async def test_single_row_fastq(self, mock_get_sequence_ids, mock_get_sample_id):
        """
        Test importing a single row, forms objects and checks response
        - MOCKS: get_sample_id_map_by_external, get_sequence_ids_from_sample_ids
        """
        mock_get_sample_id.return_value = {'Sample01': 'CPG001'}
        mock_get_sequence_ids.return_value = {}

        rows = [
            'Sequencing_date,Experiment name,Sample ID,Protocol,Flow cell,Barcoding,Device,Flowcell ID,MUX total,Basecalling,Fail FASTQ filename,Pass FASTQ filename',
            '10/12/2034,PBXP_Awesome,Sample01,LSK1,PRO002,None,PromethION,XYZ1,7107,4.0.11+f1071ce,Sample01_fail.fastq.gz,Sample01_pass.fastq.gz',
            '21/10/2021,PBXP_Awesome,Sample02,LSK1,PRO002,None,PromethION,XYZ1,8056,4.0.11+f1071ce,Sample02_fail.fastq.gz,Sample02_pass.fastq.gz',
        ]
        parser = OntParser(
            search_locations=[],
            # doesn't matter, we're going to mock the call anyway
            sample_metadata_project='dev',
        )

        parser.skip_checking_gcs_objects = True
        fs = ['Sample01_pass.fastq.gz', 'Sample02_pass.fastq.gz']
        parser.filename_map = {k: 'gs://BUCKET/FAKE/' + k for k in fs}
        parser.skip_checking_gcs_objects = True

        file_contents = '\n'.join(rows)
        resp = await parser.parse_manifest(
            StringIO(file_contents), delimiter=',', dry_run=True
        )

        (
            samples_to_add,
            sequencing_to_add,
            samples_to_update,
            sequencing_to_update,
            analyses_to_add,
        ) = resp

        self.assertEqual(1, len(samples_to_add))
        self.assertEqual(2, len(sequencing_to_add))
        self.assertEqual(1, len(samples_to_update))
        self.assertEqual(0, len(sequencing_to_update))
        self.assertEqual(0, len(analyses_to_add))
