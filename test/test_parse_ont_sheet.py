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
from scripts.parse_ont_sheet import OntParser


class TestOntSampleSheetParser(DbIsolatedTest):
    """Test the TestOntSampleSheetParser"""

    @run_as_sync
    @patch('metamist.parser.generic_parser.query_async')
    async def test_simple_sheet(self, mock_graphql_query):
        """
        Test importing a two rows, forms objects and checks response
        - MOCKS:
            - get_participant_id_map_by_external_ids
            - get_sample_id_map_by_external
            - get_sequence_ids_for_sample_ids_by_type
        """

        player = ParticipantLayer(self.connection)
        await player.upsert_participants(
            [
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'Sample01'},
                    samples=[
                        SampleUpsertInternal(
                            external_ids={PRIMARY_EXTERNAL_ORG: 'Sample01'},
                        )
                    ],
                )
            ]
        )

        mock_graphql_query.side_effect = self.run_graphql_query_async

        rows = [
            'Sequencing_date,Experiment name,Sample ID,Protocol,Flow cell,Barcoding,Device,Flowcell ID,MUX total,Basecalling,Fail FASTQ filename,Pass FASTQ filename',
            '10/12/2034,PBXP_Awesome,Sample01,LSK1,PRO002,None,PromethION,XYZ1,7107,4.0.11+f1071ce,Sample01_fail.fastq.gz,Sample01_pass.fastq.gz',
            '21/10/2021,PBXP_Awesome,Sample02,LSK1,PRO002,None,PromethION,XYZ1,8056,4.0.11+f1071ce,Sample02_fail.fastq.gz,Sample02_pass.fastq.gz',
        ]
        parser = OntParser(
            search_locations=[],
            # doesn't matter, we're going to mock the call anyway
            project=self.project_name,
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
        participants: list[ParsedParticipant]
        summary, participants = await parser.parse_manifest(
            StringIO(file_contents), delimiter=',', dry_run=True
        )

        participants_to_add = summary.participants.insert
        participants_to_update = summary.participants.update
        samples_to_add = summary.samples.insert
        samples_to_update = summary.samples.update
        sequencing_to_add = summary.assays.insert
        sequencing_to_update = summary.assays.update

        self.assertEqual(1, participants_to_add)
        self.assertEqual(1, participants_to_update)
        self.assertEqual(1, samples_to_add)
        self.assertEqual(2, sequencing_to_add)
        self.assertEqual(1, samples_to_update)
        self.assertEqual(0, sequencing_to_update)

        meta_dict = {
            'barcoding': 'None',
            'basecalling': '4.0.11+f1071ce',
            'device': 'PromethION',
            'experiment_name': 'PBXP_Awesome',
            'flow_cell': 'PRO002',
            'flowcell_id': 'XYZ1',
            'mux_total': 7107,
            'protocol': 'LSK1',
            'reads': [
                {
                    'basename': 'Sample01_pass.fastq.gz',
                    'checksum': None,
                    'class': 'File',
                    'location': 'gs://BUCKET/FAKE/Sample01_pass.fastq.gz',
                    'size': None,
                    'datetime_added': None,
                }
            ],
            'reads_type': 'fastq',
            'sequencing_date': '10/12/2034',
            'sequencing_platform': 'oxford-nanopore',
            'sequencing_technology': 'long-read',
            'sequencing_type': 'genome',
        }

        seqgroup_meta = {
            'failed_reads': [
                [
                    {
                        'location': 'gs://BUCKET/FAKE/Sample01_fail.fastq.gz',
                        'basename': 'Sample01_fail.fastq.gz',
                        'class': 'File',
                        'checksum': None,
                        'size': None,
                        'datetime_added': None,
                    }
                ]
            ],
        }
        self.maxDiff = None
        sequencing_group = participants[0].samples[0].sequencing_groups[0]
        self.assertDictEqual(seqgroup_meta, sequencing_group.meta)
        self.assertDictEqual(meta_dict, sequencing_group.assays[0].meta)
