import unittest
from io import StringIO
from test.testbase import run_as_sync
from unittest.mock import patch

from scripts.process_ont_products import OntProductParser


class TestOntSampleSheetParser(unittest.TestCase):
    """Test the TestOntSampleSheetParser"""

    @run_as_sync
    @patch('metamist.apis.SampleApi.get_sample_id_map_by_external')
    @patch('metamist.parser.cloudhelper.CloudHelper.file_exists')
    @patch('metamist.parser.cloudhelper.CloudHelper.file_size')
    async def test_single_row_all_files_exist(
        self, mock_filesize, mock_fileexists, mock_get_sample_id
    ):
        """
        Test processing one row with all files existing
        """
        mock_get_sample_id.return_value = {'Sample01': 'CPGaaa'}
        mock_filesize.return_value = 111
        mock_fileexists.return_value = True

        rows = [
            'Experiment name,Sample ID,Alignment_file,Alignment_software,SV_file,SV_software,SNV_file,SNV_software,Indel_file,Indel_software',
            'PBX10,Sample01,Sample01.bam,minimap2/2.22,Sample01.sv.vcf.gz,"Sniffles2, Version 2.0.2",Sample01.snvs.vcf.gz,Clair3 v0.1-r7,Sample01.indels.vcf.gz,Clair3 v0.1-r7',
        ]

        parser = OntProductParser(
            search_paths=[],
            # doesn't matter, we're going to mock the call anyway
            project='dev',
            dry_run=True,
        )

        # parser.skip_checking_gcs_objects = True
        fs = [
            'Sample01.bam',
            'Sample01.sv.vcf.gz',
            'Sample01.snvs.vcf.gz',
            'Sample01.indels.vcf.gz',
        ]
        parser.filename_map = {k: 'gs://BUCKET/FAKE/' + k for k in fs}
        # parser.skip_checking_gcs_objects = True

        file_contents = '\n'.join(rows)
        analyses = await parser.parse_manifest(
            StringIO(file_contents),
            delimiter=',',
        )

        self.assertEqual(4, len(analyses))
