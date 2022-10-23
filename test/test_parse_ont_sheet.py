import unittest
from io import StringIO
from unittest.mock import patch

from test.testbase import run_as_sync

from scripts.parse_ont_sheet import OntParser


class TestOntSampleSheetParser(unittest.TestCase):
    """Test the TestOntSampleSheetParser"""

    @run_as_sync
    @patch('sample_metadata.apis.ParticipantApi.get_participant_id_map_by_external_ids')
    @patch('sample_metadata.apis.SampleApi.get_sample_id_map_by_external')
    @patch('sample_metadata.apis.SequenceApi.get_sequences_by_sample_ids')
    async def test_simple_sheet(
        self, mock_get_sequence_ids, mock_get_sample_id, mock_get_participant_id
    ):
        """
        Test importing a two rows, forms objects and checks response
        - MOCKS:
            - get_participant_id_map_by_external_ids
            - get_sample_id_map_by_external
            - get_sequence_ids_for_sample_ids_by_type
        """
        mock_get_participant_id.return_value = {'Sample01': 1}
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
            project='dev',
        )

        parser.skip_checking_gcs_objects = True
        fs = [
            'Sample01_pass.fastq.gz',
            'Sample01_fail.fastq.gz',
            'Sample02_pass.fastq.gz',
            'Sample02_fail.fastq.gz',
        ]
        parser.filename_map = {k: 'gs://BUCKET/FAKE/' + k for k in fs}
        parser.skip_checking_gcs_objects = True

        file_contents = '\n'.join(rows)
        resp = await parser.parse_manifest(
            StringIO(file_contents), delimiter=',', dry_run=True
        )

        participants_to_add = resp['participants']['insert']
        participants_to_update = resp['participants']['update']
        samples_to_add = resp['samples']['insert']
        samples_to_update = resp['samples']['update']
        sequencing_to_add = resp['sequences']['insert']
        sequencing_to_update = resp['sequences']['update']

        self.assertEqual(1, len(participants_to_add))
        self.assertEqual(1, len(participants_to_update))
        self.assertEqual(1, len(samples_to_add))
        self.assertEqual(2, len(sequencing_to_add))
        self.assertEqual(1, len(samples_to_update))
        self.assertEqual(0, len(sequencing_to_update))

        meta_dict = {
            'barcoding': 'None',
            'basecalling': '4.0.11+f1071ce',
            'device': 'PromethION',
            'experiment_name': 'PBXP_Awesome',
            'failed_reads': [
                [
                    {
                        'location': 'gs://BUCKET/FAKE/Sample01_fail.fastq.gz',
                        'basename': 'Sample01_fail.fastq.gz',
                        'class': 'File',
                        'checksum': None,
                        'size': None,
                    }
                ]
            ],
            'flow_cell': 'PRO002',
            'flowcell_id': 'XYZ1',
            'mux_total': '7107',
            'protocol': 'LSK1',
            'reads': [
                [
                    {
                        'basename': 'Sample01_pass.fastq.gz',
                        'checksum': None,
                        'class': 'File',
                        'location': 'gs://BUCKET/FAKE/Sample01_pass.fastq.gz',
                        'size': None,
                    }
                ]
            ],
            'reads_type': 'fastq',
            'sequencing_date': '10/12/2034',
        }

        self.assertDictEqual(meta_dict, sequencing_to_add[0].meta)
