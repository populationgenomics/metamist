from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers import ParticipantLayer
from models.models import (
    SampleUpsertInternal,
    ParticipantUpsertInternal,
    SequencingGroupUpsertInternal,
    AssayUpsertInternal,
)

default_assay_meta = {
    'sequencing_type': 'genome',
    'sequencing_technology': 'short-read',
    'sequencing_platform': 'illumina',
}

SINGLE_PARTICIPANT_UPSERT = ParticipantUpsertInternal(
    external_id='Demeter',
    meta={},
    samples=[
        SampleUpsertInternal(
            external_id='sample_id001',
            meta={},
            type='blood',
            sequencing_groups=[
                SequencingGroupUpsertInternal(
                    type='genome',
                    technology='short-read',
                    platform='illumina',
                    assays=[
                        AssayUpsertInternal(
                            type='sequencing',
                            meta={
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
                                'batch': 'M001',
                                **default_assay_meta,
                            },
                        ),
                    ],
                )
            ],
        )
    ],
)


class TestWeb(DbIsolatedTest):
    """Test web class containing web endpoints"""

    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()
        self.player = ParticipantLayer(self.connection)

    @run_as_sync
    async def test_basic_graphql_query(self):
        """Test getting the summary for a project"""
        p = (await self.player.upsert_participants([SINGLE_PARTICIPANT_UPSERT]))[0]

        query = """
query MyQuery($project: String!) {
  project(name: $project) {
    participants {
      id
      samples {
        id
        sequencingGroups {
          id
          assays {
            id
          }
        }
      }
    }
  }
}"""
        data = await self.run_graphql_query_async(
            query, variables={'project': self.project_name}
        )
        participants = data['project']['participants']
        self.assertEqual(1, len(participants))
        self.assertEqual(p.id, participants[0]['id'])
        samples = participants[0]['samples']
        self.assertEqual(1, len(samples))

        self.assertEqual(p.samples[0].to_external().id, samples[0]['id'])
        sequencing_groups = samples[0]['sequencingGroups']
        self.assertEqual(1, len(sequencing_groups))
        self.assertEqual(
            p.samples[0].sequencing_groups[0].to_external().id,
            sequencing_groups[0]['id'],
        )
        assays = sequencing_groups[0]['assays']
        self.assertEqual(
            1, len(participants[0]['samples'][0]['sequencingGroups'][0]['assays'])
        )
        self.assertEqual(
            p.samples[0].sequencing_groups[0].assays[0].id, assays[0]['id']
        )
